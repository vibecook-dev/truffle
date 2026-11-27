// Live route tables (sidecar protocol 14). The listener and its HTTP server
// outlive every table installed on them.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const maxProxyRoutes = 1024

type proxyTable struct {
	routes  []*boundRoute // longest prefix first
	byKey   map[string]*boundRoute
	headers http.Header // unmatched requests
}

// A bound route owns its transport, cancellation, sockets and directory root.
// A directory request that acquired the old table can finish opening its file.
type boundRoute struct {
	key, prefix    string
	allow, caps    []string
	handler        http.Handler
	target         *url.URL
	rewriteRequest func(*http.Request)
	headers        http.Header

	ctx       context.Context
	cancel    context.CancelFunc
	transport *http.Transport
	root      staticRoot
	mu        sync.Mutex
	retired   bool
	readers   int
	sockets   map[net.Conn]struct{}
}

func (r *boundRoute) acquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return false
	}
	if r.root != nil {
		r.readers++
	}
	return true
}

func (r *boundRoute) releaseFile() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readers--
	if r.retired && r.readers == 0 {
		_ = r.root.Close()
	}
}

func (r *boundRoute) retire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return
	}
	r.retired = true
	if r.cancel != nil {
		r.cancel()
	}
	if r.transport != nil {
		r.transport.CloseIdleConnections()
	}
	for conn := range r.sockets {
		_ = conn.Close()
	}
	if r.root != nil && r.readers == 0 {
		_ = r.root.Close()
	}
}

func (r *boundRoute) trackSockets(client, backend net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return false
	}
	if r.sockets == nil {
		r.sockets = make(map[net.Conn]struct{})
	}
	r.sockets[client] = struct{}{}
	r.sockets[backend] = struct{}{}
	return true
}
func (r *boundRoute) untrackSockets(client, backend net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sockets, client)
	delete(r.sockets, backend)
}

func (t *proxyTable) retireExcept(keep *proxyTable) {
	if t == nil {
		return
	}
	for _, r := range t.routes {
		if keep == nil || keep.byKey[r.key] != r {
			r.retire()
		}
	}
}

func (e *proxyEntry) match(path string) (*boundRoute, http.Header) {
	for {
		table := e.table.Load()
		for _, r := range table.routes {
			if pathMatchesPrefix(path, r.prefix) {
				if r.acquire() {
					return r, nil
				}
				// A swap retired this route after Load. Retry the new table.
				break
			}
		}
		if table == e.table.Load() {
			return nil, table.headers
		}
	}
}

func sortedNames(names []string) []string {
	out := append([]string{}, names...)
	slices.Sort(out)
	return slices.Compact(out)
}

// Validate and bind before publication. On failure or panic only the newly
// acquired resources are retired. Shared routes remain owned by the old table.
func (s *shim) bindProxyRoutes(data proxyAddData, old *proxyTable, legacy bool) (*proxyTable, *proxyAddFailure) {
	if failure := validatePublicServe(data); failure != nil {
		return nil, failure
	}
	if len(data.Routes) > maxProxyRoutes {
		return nil, &proxyAddFailure{"TOO_MANY_ROUTES", fmt.Sprintf("a serve supports at most %d routes", maxProxyRoutes)}
	}
	if failure := validateAcceptAppCaps(data.AcceptAppCaps); failure != nil {
		return nil, failure
	}
	if failure := validateResponseHeaders(data); failure != nil {
		return nil, failure
	}
	routes := data.Routes
	if legacy && len(routes) == 0 {
		target := url.URL{Scheme: data.TargetScheme, Host: net.JoinHostPort(strings.Trim(data.TargetHost, "[]"), strconv.Itoa(int(data.TargetPort)))}
		routes = []proxyRouteData{{Prefix: "/", TargetURL: target.String()}}
	}
	table := &proxyTable{headers: mergeResponseHeaders(data.Headers, nil), byKey: make(map[string]*boundRoute, len(routes))}
	complete := false
	defer func() {
		if !complete {
			table.retireExcept(old)
		}
	}()
	caps := sortedNames(data.AcceptAppCaps)
	for _, rt := range routes {
		headers := mergeResponseHeaders(data.Headers, rt.Headers)
		rt.Allow = sortedNames(effectiveAllow(rt.Allow, data.Allow))
		rt.Headers = nil // the normalized effective header map is part of the key
		encoded, err := json.Marshal(struct {
			Route   proxyRouteData
			Headers http.Header
			Caps    []string
		}{rt, headers, caps})
		if err != nil {
			return nil, &proxyAddFailure{"INVALID_ROUTE", err.Error()}
		}
		key := string(encoded)
		r := table.byKey[key]
		if r == nil && old != nil {
			r = old.byKey[key]
		}
		if r == nil {
			r = &boundRoute{key: key, prefix: rt.Prefix, allow: rt.Allow, caps: caps, headers: headers}
			table.routes = append(table.routes, r) // own even partial binds
			if fail := s.bindRoute(r, data, rt); fail != nil {
				return nil, fail
			}
		} else {
			table.routes = append(table.routes, r)
		}
		table.byKey[key] = r
	}
	sort.SliceStable(table.routes, func(i, j int) bool { return len(table.routes[i].prefix) > len(table.routes[j].prefix) })
	complete = true
	return table, nil
}

func (s *shim) bindRoute(r *boundRoute, data proxyAddData, rt proxyRouteData) *proxyAddFailure {
	if !strings.HasPrefix(rt.Prefix, "/") {
		return &proxyAddFailure{"INVALID_ROUTE", fmt.Sprintf("route prefix %q must start with '/'", rt.Prefix)}
	}
	if (rt.TargetURL == "") == (rt.Dir == "") {
		return &proxyAddFailure{"INVALID_ROUTE", fmt.Sprintf("route %q needs exactly one of targetUrl and dir", rt.Prefix)}
	}
	if rt.Dir != "" {
		if !filepath.IsAbs(rt.Dir) {
			return &proxyAddFailure{"INVALID_ROUTE", fmt.Sprintf("route %q: dir must be an absolute path", rt.Prefix)}
		}
		root, err := openStaticRoot(rt.Dir)
		r.root = root
		if err != nil || root == nil {
			return &proxyAddFailure{"STATIC_ROOT_INVALID", fmt.Sprintf("route %q static root %q cannot be opened: %v", rt.Prefix, rt.Dir, err)}
		}
		r.handler = withStaticResponseHeaders(newSPAFileServer(root, rt.Fallback), r.headers)
		return nil
	}
	u, err := url.Parse(rt.TargetURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return &proxyAddFailure{"INVALID_ROUTE", fmt.Sprintf("route %q: targetUrl must be an absolute http(s) URL", rt.Prefix)}
	}
	if !data.AllowNonLoopback && !isLoopbackHost(u.Hostname()) {
		return &proxyAddFailure{"TARGET_NOT_LOOPBACK", fmt.Sprintf("route %q target %q is not loopback; set allowNonLoopback to opt in (RFC 023 §9.3)", rt.Prefix, u.Hostname())}
	}
	strip := ""
	if rt.StripPrefix {
		strip = rt.Prefix
	}
	proxy := s.buildReverseProxy(data.ID, u, strip)
	withResponseHeaders(proxy, r.headers)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.handler, r.target, r.rewriteRequest = proxy, u, proxy.Director
	if data.Funnel {
		// ReverseProxy removes forwarding and hop-by-hop headers before
		// Rewrite. Restore trusted metadata only after that removal.
		proxy.Director = nil
		proxy.Rewrite = func(req *httputil.ProxyRequest) {
			r.rewriteRequest(req.Out)
			visitor := req.In.Context().Value(funnelAddrKey{}).(netip.Addr)
			setFunnelHeaders(req.Out.Header, visitor)
		}
	}
	r.transport = proxy.Transport.(*http.Transport)
	return nil
}

// Dispatch processes updates in wire order. An update always supplies routes,
// including [] to keep the listener up with every path returning 404.
func (s *shim) handleProxyUpdate(raw json.RawMessage) {
	var data proxyAddData
	fail := func(code, msg string) {
		s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: code, Message: msg, RequestID: data.RequestID})
	}
	if err := json.Unmarshal(raw, &data); err != nil || data.Routes == nil {
		fail("INVALID_ROUTE", "proxy:update requires a routes array")
		return
	}
	s.proxyMu.Lock()
	entry := s.proxies[data.ID]
	s.proxyMu.Unlock()
	if entry == nil {
		fail("NOT_FOUND", "proxy not found")
		return
	}
	data.AllowNonLoopback = entry.allowNonLoopback
	data.Funnel, data.ListenPort, data.Tls = entry.funnel, entry.listenPort, &entry.tlsOn
	old := entry.table.Load()
	next, failure := s.bindProxyRoutes(data, old, false)
	if failure != nil {
		fail(failure.code, failure.message)
		return
	}
	s.proxyMu.Lock()
	if s.proxies[data.ID] != entry {
		s.proxyMu.Unlock()
		next.retireExcept(old)
		fail("NOT_FOUND", "proxy removed during update")
		return
	}
	entry.table.Store(next)
	entry.targetHost, entry.targetPort, entry.targetScheme = "", 0, ""
	s.proxyMu.Unlock()
	old.retireExcept(next)
	s.sendEvent("proxy:updated", proxyRemovedEventData{ID: data.ID, RequestID: data.RequestID})
}
