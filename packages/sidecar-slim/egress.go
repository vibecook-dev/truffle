// RFC 026: egress for the node's own host processes.
//
// Truffle serves IN (RFC 023 proxy:add, tsnet:listen) and, before RFC 026,
// reached OUT only for the Rust core itself (bridge:dial — a verb no other
// process can speak). The three rungs here let an ordinary process on the
// same host (a Node daemon, a browser) reach a tailnet node THROUGH this
// node, as this node:
//
//   - E1 tsnet:loopback — tsnet's own loopback SOCKS5 proxy (and LocalAPI),
//     password-authenticated; the credential crosses the bridge once.
//   - E2 egress:add / egress:remove / egress:list — a loopback listener with
//     a FIXED tailnet target. Each accepted connection is dialled through the
//     node and spliced raw, so a WebSocket upgrade (or any other protocol)
//     passes byte-for-byte: no HTTP is parsed, because the target is fixed.
//   - E3 egress:proxy — a loopback HTTP CONNECT proxy for clients that
//     cannot speak SOCKS5 (browsers), Basic-authenticated, tunnelling only to
//     admitted tailnet nodes.
//
// Every listener here binds 127.0.0.1. E2 and E3 admit a destination only
// when it is a node of this node's netmap whose owner login passes the
// node's RFC 025 gate (and a route's own glob), and they re-check at every
// connection: a peer can leave the tailnet, or change owner, while a route
// stands. E1 is tsnet's own server and applies no admission (RFC 026 §5).
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/net/tsdial"
	"tailscale.com/tsnet"
)

// Egress refusal codes (RFC 026 §4). They ride egress:error, and tsnet:error
// for tsnet:loopback.
const (
	egressInvalidTarget = "INVALID_TARGET"
	egressInvalidLogin  = "INVALID_LOGIN"
	egressNotPeer       = "TARGET_NOT_PEER"
	egressLoginRefused  = "LOGIN_REFUSED"
	egressLoginUnknown  = "LOGIN_UNKNOWN"
	egressStatusFailed  = "STATUS_UNAVAILABLE"
	egressDialFailed    = "DIAL_FAILED"
)

// loopbackData is the payload for tsnet:loopback commands (E1).
type loopbackData struct {
	// LocalAPI asks for the LocalAPI credential as well. tsnet serves a
	// read-write LocalAPI on the same listener (prefs, logout), so that
	// credential crosses the bridge only when a caller asks for it.
	LocalAPI  bool   `json:"localAPI,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

// loopbackResultData is the payload for tsnet:loopbackResult events. The
// SOCKS5 username is always "tsnet" (tsnet's own constant).
type loopbackResultData struct {
	Address            string `json:"address"`
	ProxyCredential    string `json:"proxyCredential"`
	LocalAPICredential string `json:"localAPICredential,omitempty"`
	RequestID          string `json:"requestId,omitempty"`
}

// egressAddData is the payload for egress:add commands (E2).
type egressAddData struct {
	ID string `json:"id"`
	// Target is "<peer-fqdn-or-tailnet-ip>:<port>", fixed for the route.
	Target string `json:"target"`
	// Login optionally narrows the route to targets whose owner login matches
	// this glob (RFC 025 §3.2 grammar), on top of the node's own gate.
	Login string `json:"login,omitempty"`
	// NodeLoginAllow is the node's RFC 025 login_allow. The core owns that
	// gate and the sidecar cannot otherwise know it; empty = ungated.
	NodeLoginAllow []string `json:"nodeLoginAllow,omitempty"`
	RequestID      string   `json:"requestId,omitempty"`
}

// egressAddedData is the payload for egress:added events.
type egressAddedData struct {
	ID        string `json:"id"`
	Listen    string `json:"listen"` // "127.0.0.1:<p>"
	RequestID string `json:"requestId,omitempty"`
}

// egressRemoveData is the payload for egress:remove commands, and doubles as
// the egress:removed event payload.
type egressRemoveData struct {
	ID        string `json:"id"`
	RequestID string `json:"requestId,omitempty"`
}

// egressListData is the optional payload for egress:list commands.
type egressListData struct {
	RequestID string `json:"requestId,omitempty"`
}

// egressRouteInfo is one row of an egress:list event.
type egressRouteInfo struct {
	ID     string `json:"id"`
	Target string `json:"target"`
	Listen string `json:"listen"`
	Login  string `json:"login,omitempty"`
}

// egressListResultData is the payload for egress:list events. Routes is
// never null on the wire: an empty list is [].
type egressListResultData struct {
	Routes    []egressRouteInfo `json:"routes"`
	RequestID string            `json:"requestId,omitempty"`
}

// egressProxyData is the payload for egress:proxy commands (E3).
type egressProxyData struct {
	// NodeLoginAllow is the node's RFC 025 gate, as on egress:add. The proxy
	// keeps the list it was created with: the gate is fixed for a node's
	// lifetime (RFC 025 §3.1), so every later call carries the same one.
	NodeLoginAllow []string `json:"nodeLoginAllow,omitempty"`
	RequestID      string   `json:"requestId,omitempty"`
}

// egressProxyResultData is the payload for egress:proxyResult events. Any
// Basic username is accepted; the password must be the credential.
type egressProxyResultData struct {
	Address    string `json:"address"`
	Credential string `json:"credential"`
	RequestID  string `json:"requestId,omitempty"`
}

// egressErrorData is the payload for egress:error events: the failure
// terminal of egress:add / egress:remove / egress:proxy (with the command's
// requestId), and a runtime event when a route refuses or cannot dial an
// accepted connection (ID set, no requestId).
type egressErrorData struct {
	ID        string `json:"id,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId,omitempty"`
}

// egressRefusal is an admission failure: a stable code for the core and a
// message for a human.
type egressRefusal struct {
	code    string
	message string
}

// egressNode is an admitted destination: the netmap node a target names and
// the tailnet address to dial it at.
type egressNode struct {
	addr  netip.Addr
	name  string // MagicDNS name, lowercased, no trailing dot
	login string // owner login; "" when the netmap names none
}

// parseEgressTarget splits a "<host>:<port>" target. The host is a MagicDNS
// name or a tailnet IP (IPv6 in brackets); whether it names a netmap node is
// admission's question, not this one's.
func parseEgressTarget(target string) (string, uint16, *egressRefusal) {
	host, portText, err := net.SplitHostPort(target)
	if err != nil || host == "" {
		return "", 0, &egressRefusal{egressInvalidTarget, fmt.Sprintf("target %q must be <peer-fqdn-or-tailnet-ip>:<port>", target)}
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", 0, &egressRefusal{egressInvalidTarget, fmt.Sprintf("target %q has no valid port", target)}
	}
	return host, uint16(port), nil
}

// normalizeDNSName lowercases a MagicDNS name and drops its trailing dot.
func normalizeDNSName(name string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(name)), ".")
}

// preferredTailnetAddr picks the address a node named by NAME is dialled at:
// its first IPv4 address, else its first address.
func preferredTailnetAddr(addrs []netip.Addr) netip.Addr {
	for _, a := range addrs {
		if a.Is4() {
			return a
		}
	}
	if len(addrs) > 0 {
		return addrs[0]
	}
	return netip.Addr{}
}

// netmapNodes lists the nodes an egress may reach: this node itself, then its
// peers. Self is included so a local page can reach this node's own served
// origin through E3 the way it reaches any other node's.
func netmapNodes(st *ipnstate.Status) []*ipnstate.PeerStatus {
	nodes := make([]*ipnstate.PeerStatus, 0, len(st.Peer)+1)
	if st.Self != nil {
		nodes = append(nodes, st.Self)
	}
	for _, p := range st.Peer {
		if p != nil {
			nodes = append(nodes, p)
		}
	}
	return nodes
}

// admitEgressHost resolves host against the netmap and applies the gates.
//
// The host must name a node of this netmap — by its MagicDNS name (any case,
// trailing dot optional) or by one of its tailnet IPs. A name that merely
// ends in .ts.net is not enough: tsnet dials a name outside the netmap over
// the host's own network (another tailnet's Funnel host, say), which is the
// egress this engine must never offer. The address dialled is the IP the
// target named, or the node's preferred address when it was named by name,
// so the bytes go to the node that was admitted and nowhere else.
//
// With a gate — the node's RFC 025 login_allow and/or the route's own glob —
// the node's owner login must be present and pass BOTH (fail closed, RFC 025
// §3.2): a gated egress never reaches a node whose owner it cannot state.
func admitEgressHost(st *ipnstate.Status, host string, nodeAllow []string, routeLogin string) (egressNode, *egressRefusal) {
	if st == nil {
		return egressNode{}, &egressRefusal{egressStatusFailed, "no netmap status"}
	}
	var match *ipnstate.PeerStatus
	var addr netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		ip = ip.Unmap()
	search:
		for _, n := range netmapNodes(st) {
			for _, a := range n.TailscaleIPs {
				if a == ip {
					match, addr = n, ip
					break search
				}
			}
		}
	} else if name := normalizeDNSName(host); name != "" {
		for _, n := range netmapNodes(st) {
			if normalizeDNSName(n.DNSName) == name {
				match, addr = n, preferredTailnetAddr(n.TailscaleIPs)
				break
			}
		}
	}
	if match == nil || !addr.IsValid() {
		return egressNode{}, &egressRefusal{egressNotPeer, fmt.Sprintf("%s is not a node of this node's tailnet", host)}
	}

	node := egressNode{addr: addr, name: normalizeDNSName(match.DNSName), login: loginNameForUser(st.User, match.UserID)}
	if len(nodeAllow) == 0 && routeLogin == "" {
		return node, nil
	}
	if node.login == "" {
		return egressNode{}, &egressRefusal{egressLoginUnknown, fmt.Sprintf("the owner of %s is unknown, and a gated egress fails closed", host)}
	}
	if !allowedLogin(nodeAllow, node.login) {
		return egressNode{}, &egressRefusal{egressLoginRefused, fmt.Sprintf("the node's login gate refuses %s (owner %s)", host, node.login)}
	}
	if routeLogin != "" && !allowedLogin([]string{routeLogin}, node.login) {
		return egressNode{}, &egressRefusal{egressLoginRefused, fmt.Sprintf("login %q refuses %s (owner %s)", routeLogin, host, node.login)}
	}
	return node, nil
}

// admitEgress reads the netmap now and admits host against it. A status that
// cannot be read refuses: the gate never guesses.
func (s *shim) admitEgress(ctx context.Context, lc *local.Client, host string, nodeAllow []string, routeLogin string) (egressNode, *egressRefusal) {
	st, err := lc.Status(ctx)
	if err != nil {
		return egressNode{}, &egressRefusal{egressStatusFailed, "netmap status unavailable: " + err.Error()}
	}
	return admitEgressHost(st, host, nodeAllow, routeLogin)
}

// dialEgress keeps E2/E3 inside this node's userspace stack even if the
// admitted peer disappears before dialing. Server.Dial/UserDial makes a
// second routing decision and can fall back to the host for an unknown IP.
// tsnet exposes no stable tailnet-only TCP dial, so isolate its pinned
// subsystem API here and exercise it in the upgrade regression suite.
func dialEgress(ctx context.Context, dialer *tsdial.Dialer, target netip.AddrPort) (net.Conn, error) {
	if dialer == nil || dialer.NetstackDialTCP == nil {
		return nil, errors.New("egress netstack dialer is unavailable")
	}
	return dialer.NetstackDialTCP(ctx, target)
}

// newEgressCredential returns 16 random bytes as hex, the shape of tsnet's
// own loopback credentials.
func newEgressCredential() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// ── E1: tsnet:loopback ────────────────────────────────────────────────────

// handleLoopback answers the node's tsnet loopback: a SOCKS5 proxy onto the
// tailnet (username "tsnet", password the proxy credential) and the LocalAPI
// behind a second credential, on one 127.0.0.1 listener. tsnet starts it on
// the first call and closes it with the server, so it lives exactly as long
// as the node; every call in a lifecycle answers the same address and
// credentials, and a restart mints new ones.
//
// Runs on the dispatch loop, unlike most handlers: tsnet's Loopback creates
// its listener lazily with no lock of its own, and Server.Close reads that
// field unguarded, so this call must be serialized with tsnet:stop — and the
// dispatch loop serializes them.
func (s *shim) handleLoopback(raw json.RawMessage) {
	defer s.recoverPanic("handleLoopback")
	var d loopbackData
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &d); err != nil {
			s.sendErrorFor("INVALID_COMMAND", "invalid tsnet:loopback data: "+err.Error(), requestIDOf(raw))
			return
		}
	}
	srv := s.getServer()
	if srv == nil {
		s.sendErrorFor("NOT_RUNNING", "node not running", d.RequestID)
		return
	}
	addr, proxyCred, localAPICred, err := srv.Loopback()
	if err != nil {
		s.sendErrorFor("LOOPBACK_ERROR", err.Error(), d.RequestID)
		return
	}
	result := loopbackResultData{Address: addr, ProxyCredential: proxyCred, RequestID: d.RequestID}
	if d.LocalAPI {
		result.LocalAPICredential = localAPICred
	}
	s.sendEvent("tsnet:loopbackResult", result)
}

// ── E2: egress routes ─────────────────────────────────────────────────────

// egressRoute is one E2 route: a loopback listener whose every connection is
// admitted afresh and spliced to a FIXED tailnet target. A route with no
// listener is a placeholder reserving its id while egress:add is in flight.
type egressRoute struct {
	id        string
	target    string // as given: "<host>:<port>"
	host      string
	port      uint16
	login     string   // the route's own owner glob; "" = none
	nodeAllow []string // the node's RFC 025 gate, from the core
	listen    string   // "127.0.0.1:<p>"
	listener  net.Listener
	cancel    context.CancelFunc

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
}

func (r *egressRoute) placeholder() bool { return r.listener == nil }

// track registers a live connection so egress:remove and tsnet:stop can
// force it closed. On a route already shut down it closes c instead and
// reports false.
func (r *egressRoute) track(c net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		_ = c.Close()
		return false
	}
	if r.conns == nil {
		r.conns = make(map[net.Conn]struct{})
	}
	r.conns[c] = struct{}{}
	return true
}

func (r *egressRoute) untrack(c net.Conn) {
	r.mu.Lock()
	delete(r.conns, c)
	r.mu.Unlock()
}

// shutdown stops the route: the listener closes and every live connection,
// both halves, is closed with it. Idempotent, and safe on a placeholder.
func (r *egressRoute) shutdown() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	conns := r.conns
	r.conns = nil
	r.mu.Unlock()

	if r.cancel != nil {
		r.cancel()
	}
	if r.listener != nil {
		_ = r.listener.Close()
	}
	for c := range conns {
		_ = c.Close()
	}
}

// handleEgressAdd binds a route. The id is reserved at once (a duplicate is
// refused even while the first add is in flight); admission reads the
// netmap, so it runs off the dispatch loop, like handleGetPeers (G3).
func (s *shim) handleEgressAdd(raw json.RawMessage) {
	var d egressAddData
	if err := json.Unmarshal(raw, &d); err != nil {
		s.sendEvent("egress:error", egressErrorData{Code: "INVALID_COMMAND", Message: "invalid egress:add data: " + err.Error(), RequestID: requestIDOf(raw)})
		return
	}
	fail := func(code, message string) {
		s.sendEvent("egress:error", egressErrorData{ID: d.ID, Code: code, Message: message, RequestID: d.RequestID})
	}

	srv := s.getServer()
	if srv == nil {
		fail("NOT_RUNNING", "node not running")
		return
	}
	if d.ID == "" {
		fail("INVALID_COMMAND", "egress:add needs an id")
		return
	}
	host, port, bad := parseEgressTarget(d.Target)
	if bad != nil {
		fail(bad.code, bad.message)
		return
	}
	if d.Login != "" {
		if _, err := path.Match(strings.ToLower(d.Login), ""); err != nil {
			fail(egressInvalidLogin, fmt.Sprintf("login glob %q is malformed: %v", d.Login, err))
			return
		}
	}
	lc, err := s.localClientFor(srv)
	if err != nil {
		fail("INTERNAL", "local client unavailable: "+err.Error())
		return
	}
	lifecycle := s.lifecycleCtx()
	if lifecycle == nil || lifecycle.Err() != nil {
		fail("NOT_READY", "node lifecycle is not running")
		return
	}

	placeholder := &egressRoute{id: d.ID}
	s.egressMu.Lock()
	if _, exists := s.egressRoutes[d.ID]; exists {
		s.egressMu.Unlock()
		fail("EGRESS_EXISTS", "an egress route with this id already exists")
		return
	}
	if s.egressRoutes == nil {
		s.egressRoutes = make(map[string]*egressRoute)
	}
	s.egressRoutes[d.ID] = placeholder
	s.egressMu.Unlock()

	go func() {
		defer s.recoverPanic("handleEgressAdd")
		installed := false
		defer func() {
			if !installed {
				s.removeEgressPlaceholder(d.ID, placeholder)
			}
		}()

		admitCtx, cancelAdmit := context.WithTimeout(lifecycle, statusTimeout)
		_, refusal := s.admitEgress(admitCtx, lc, host, d.NodeLoginAllow, d.Login)
		cancelAdmit()
		if refusal != nil {
			fail(refusal.code, fmt.Sprintf("egress %q to %s refused: %s", d.ID, d.Target, refusal.message))
			return
		}

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fail("LISTEN_ERROR", err.Error())
			return
		}
		ctx, cancel := context.WithCancel(lifecycle)
		route := &egressRoute{
			id: d.ID, target: d.Target, host: host, port: port,
			login: d.Login, nodeAllow: append([]string(nil), d.NodeLoginAllow...),
			listen: ln.Addr().String(), listener: ln, cancel: cancel,
		}
		if !s.installEgressRoute(d.ID, lifecycle, placeholder, route) {
			route.shutdown()
			fail("NOT_READY", "node lifecycle ended before the route became ready")
			return
		}
		installed = true
		s.sendEvent("egress:added", egressAddedData{ID: d.ID, Listen: route.listen, RequestID: d.RequestID})
		s.serveEgressRoute(ctx, srv, lc, route)
	}()
}

// installEgressRoute is the single point where a route goes live. handleStop
// cancels the lifecycle before taking egressMu to reset the table, so a
// check of the captured lifecycle under the same lock keeps an in-flight add
// from installing into a stopped node.
func (s *shim) installEgressRoute(id string, lifecycle context.Context, placeholder, route *egressRoute) bool {
	s.egressMu.Lock()
	defer s.egressMu.Unlock()
	if lifecycle.Err() != nil || s.egressRoutes[id] != placeholder {
		return false
	}
	s.egressRoutes[id] = route
	return true
}

// removeEgressPlaceholder releases an id reserved by a failed add. It never
// removes a live route or another add's placeholder.
func (s *shim) removeEgressPlaceholder(id string, placeholder *egressRoute) {
	s.egressMu.Lock()
	if s.egressRoutes[id] == placeholder {
		delete(s.egressRoutes, id)
	}
	s.egressMu.Unlock()
}

// serveEgressRoute accepts until the route closes. A transient accept error
// (descriptor exhaustion) backs off and retries, as net/http's server does,
// instead of silently killing the route.
func (s *shim) serveEgressRoute(ctx context.Context, srv *tsnet.Server, lc *local.Client, r *egressRoute) {
	var backoff time.Duration
	for {
		c, err := r.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else if backoff *= 2; backoff > time.Second {
				backoff = time.Second
			}
			log.Printf("egress %s: accept error: %v; retrying in %v", r.id, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		go s.serveEgressConn(ctx, srv, lc, r, c)
	}
}

// serveEgressConn admits one accepted connection afresh — the netmap may
// have changed since egress:add — dials the target through the node, and
// splices both ways. The splice is raw TCP: the target is fixed per route,
// so there is nothing to route on and nothing to rewrite, and an upgrade's
// request line reaches the target exactly as the client wrote it. A refusal
// or a failed dial closes the client and reports on egress:error (no
// requestId: a runtime event, not an answer to a command).
func (s *shim) serveEgressConn(ctx context.Context, srv *tsnet.Server, lc *local.Client, r *egressRoute, client net.Conn) {
	defer s.recoverPanic("serveEgressConn")
	if !r.track(client) {
		return
	}
	defer r.untrack(client)

	admitCtx, cancelAdmit := context.WithTimeout(ctx, whoisTimeout)
	node, refusal := s.admitEgress(admitCtx, lc, r.host, r.nodeAllow, r.login)
	cancelAdmit()
	if refusal != nil {
		_ = client.Close()
		s.sendEvent("egress:error", egressErrorData{ID: r.id, Code: refusal.code, Message: refusal.message})
		return
	}

	dialCtx, cancelDial := context.WithTimeout(ctx, dialTimeout)
	defer cancelDial()
	upstream, err := dialEgress(dialCtx, srv.Sys().Dialer.Get(), netip.AddrPortFrom(node.addr, r.port))
	if err != nil {
		_ = client.Close()
		s.sendEvent("egress:error", egressErrorData{ID: r.id, Code: egressDialFailed, Message: fmt.Sprintf("dial %s: %v", r.target, err)})
		return
	}
	if !r.track(upstream) {
		_ = client.Close()
		return
	}
	defer r.untrack(upstream)
	bridgeCopy(upstream, client, s.idleTimeoutOrDefault())
}

// handleEgressRemove closes a route and every connection it carries.
func (s *shim) handleEgressRemove(raw json.RawMessage) {
	var d egressRemoveData
	if err := json.Unmarshal(raw, &d); err != nil {
		s.sendEvent("egress:error", egressErrorData{Code: "INVALID_COMMAND", Message: "invalid egress:remove data: " + err.Error(), RequestID: requestIDOf(raw)})
		return
	}
	s.egressMu.Lock()
	route, ok := s.egressRoutes[d.ID]
	if !ok || route.placeholder() {
		s.egressMu.Unlock()
		s.sendEvent("egress:error", egressErrorData{ID: d.ID, Code: "NOT_FOUND", Message: "no egress route with this id", RequestID: d.RequestID})
		return
	}
	delete(s.egressRoutes, d.ID)
	s.egressMu.Unlock()

	route.shutdown()
	s.sendEvent("egress:removed", egressRemoveData{ID: d.ID, RequestID: d.RequestID})
}

// handleEgressList answers the live routes, sorted by id. Placeholders for
// adds still in flight are not routes yet and are not listed.
func (s *shim) handleEgressList(raw json.RawMessage) {
	var d egressListData
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &d)
	}
	s.egressMu.Lock()
	routes := make([]egressRouteInfo, 0, len(s.egressRoutes))
	for _, r := range s.egressRoutes {
		if r.placeholder() {
			continue
		}
		routes = append(routes, egressRouteInfo{ID: r.id, Target: r.target, Listen: r.listen, Login: r.login})
	}
	s.egressMu.Unlock()
	sort.Slice(routes, func(i, j int) bool { return routes[i].ID < routes[j].ID })
	s.sendEvent("egress:list", egressListResultData{Routes: routes, RequestID: d.RequestID})
}

// ── E3: the authenticated HTTP CONNECT proxy ──────────────────────────────

// egressHTTPProxy is the E3 engine: one loopback HTTP CONNECT proxy per
// lifecycle, Basic-authenticated, tunnelling only to admitted tailnet nodes.
type egressHTTPProxy struct {
	address    string
	credential string
	nodeAllow  []string
	server     *http.Server

	mu      sync.Mutex
	closed  bool
	tunnels map[net.Conn]struct{}
}

// track registers a tunnel's connection so shutdown can close it:
// http.Server.Close does not touch hijacked connections (the P1 lesson of
// the serve engine). On a closed proxy it closes c and reports false.
func (p *egressHTTPProxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = c.Close()
		return false
	}
	if p.tunnels == nil {
		p.tunnels = make(map[net.Conn]struct{})
	}
	p.tunnels[c] = struct{}{}
	return true
}

func (p *egressHTTPProxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.tunnels, c)
	p.mu.Unlock()
}

// shutdown closes the listener, every in-flight request, and every tunnel.
func (p *egressHTTPProxy) shutdown() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	tunnels := p.tunnels
	p.tunnels = nil
	p.mu.Unlock()

	if p.server != nil {
		_ = p.server.Close()
	}
	for c := range tunnels {
		_ = c.Close()
	}
}

// proxyAuthorized checks a Proxy-Authorization header: the Basic scheme, any
// username, and a password equal to the credential (compared in constant
// time). The credential is the secret; the username carries none.
func proxyAuthorized(header, credential string) bool {
	if credential == "" {
		return false
	}
	scheme, encoded, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Basic") {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return false
	}
	_, password, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(password), []byte(credential)) == 1
}

// egressProxyHandler serves the E3 proxy. Order matters: authentication
// first (407 — an unauthenticated caller learns nothing else), then CONNECT
// only (405 — no absolute-form forwarding, so the proxy never parses or
// rewrites a request it relays), then admission (a bare 403, as RFC 023
// §9.7's gate answers: no detail about the netmap), then the tunnel.
func (s *shim) egressProxyHandler(p *egressHTTPProxy, srv *tsnet.Server, lc *local.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !proxyAuthorized(r.Header.Get("Proxy-Authorization"), p.credential) {
			w.Header().Set("Proxy-Authenticate", `Basic realm="truffle"`)
			http.Error(w, "Proxy Authentication Required", http.StatusProxyAuthRequired)
			return
		}
		if r.Method != http.MethodConnect {
			w.Header().Set("Allow", http.MethodConnect)
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		host, port, bad := parseEgressTarget(r.Host)
		if bad != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		admitCtx, cancelAdmit := context.WithTimeout(r.Context(), whoisTimeout)
		node, refusal := s.admitEgress(admitCtx, lc, host, p.nodeAllow, "")
		cancelAdmit()
		if refusal != nil {
			debugf("egress proxy: CONNECT %s refused: %s", r.Host, refusal.message)
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		dialCtx, cancelDial := context.WithTimeout(r.Context(), dialTimeout)
		defer cancelDial()
		upstream, err := dialEgress(dialCtx, srv.Sys().Dialer.Get(), netip.AddrPortFrom(node.addr, port))
		if err != nil {
			debugf("egress proxy: CONNECT %s dial failed: %v", r.Host, err)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			_ = upstream.Close()
			http.Error(w, "CONNECT hijack not supported", http.StatusInternalServerError)
			return
		}
		client, buffered, err := hijacker.Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		if !p.track(client) {
			_ = upstream.Close()
			return
		}
		defer p.untrack(client)
		if !p.track(upstream) {
			_ = client.Close()
			return
		}
		defer p.untrack(upstream)

		spliceEgressTunnel(upstream, client, buffered.Reader, s.idleTimeoutOrDefault())
	})
}

// spliceEgressTunnel writes the CONNECT response and any already-buffered
// client bytes before handing both streams to the regular idle-bounded copy.
func spliceEgressTunnel(upstream, client net.Conn, buffered *bufio.Reader, idleTimeout time.Duration) {
	_ = client.SetWriteDeadline(time.Now().Add(idleTimeout))
	if _, err := io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	// Bytes the client pipelined behind the CONNECT (a TLS ClientHello)
	// already sit in the server's reader; forward them before splicing.
	if n := buffered.Buffered(); n > 0 {
		pending, _ := buffered.Peek(n)
		_ = upstream.SetWriteDeadline(time.Now().Add(idleTimeout))
		if _, err := upstream.Write(pending); err != nil {
			_ = client.Close()
			_ = upstream.Close()
			return
		}
	}
	bridgeCopy(upstream, client, idleTimeout)
}

// handleEgressProxy starts the node's E3 proxy on first call and answers the
// same address and credential on every later one in the lifecycle. Runs on
// the dispatch loop (nothing here blocks), which also serializes it with
// tsnet:stop.
func (s *shim) handleEgressProxy(raw json.RawMessage) {
	var d egressProxyData
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &d); err != nil {
			s.sendEvent("egress:error", egressErrorData{Code: "INVALID_COMMAND", Message: "invalid egress:proxy data: " + err.Error(), RequestID: requestIDOf(raw)})
			return
		}
	}
	fail := func(code, message string) {
		s.sendEvent("egress:error", egressErrorData{Code: code, Message: message, RequestID: d.RequestID})
	}

	srv := s.getServer()
	if srv == nil {
		fail("NOT_RUNNING", "node not running")
		return
	}
	lc, err := s.localClientFor(srv)
	if err != nil {
		fail("INTERNAL", "local client unavailable: "+err.Error())
		return
	}
	lifecycle := s.lifecycleCtx()
	if lifecycle == nil || lifecycle.Err() != nil {
		fail("NOT_READY", "node lifecycle is not running")
		return
	}

	s.egressMu.Lock()
	if p := s.egressProxy; p != nil {
		s.egressMu.Unlock()
		s.sendEvent("egress:proxyResult", egressProxyResultData{Address: p.address, Credential: p.credential, RequestID: d.RequestID})
		return
	}
	credential, err := newEgressCredential()
	if err != nil {
		s.egressMu.Unlock()
		fail("INTERNAL", "credential generation failed: "+err.Error())
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.egressMu.Unlock()
		fail("LISTEN_ERROR", err.Error())
		return
	}
	p := &egressHTTPProxy{
		address:    ln.Addr().String(),
		credential: credential,
		nodeAllow:  append([]string(nil), d.NodeLoginAllow...),
	}
	// P4's bounds, as the serve engine sets them: header reads are bounded
	// (slow-loris); ReadTimeout stays unset so a tunnel can live long.
	p.server = &http.Server{
		Handler:           s.egressProxyHandler(p, srv, lc),
		BaseContext:       func(net.Listener) context.Context { return lifecycle },
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	s.egressProxy = p
	s.egressMu.Unlock()

	go func() {
		defer s.recoverPanic("egressProxy")
		if err := p.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.sendEvent("egress:error", egressErrorData{Code: "SERVE_ERROR", Message: err.Error()})
		}
	}()
	s.sendEvent("egress:proxyResult", egressProxyResultData{Address: p.address, Credential: p.credential, RequestID: d.RequestID})
}

// stopEgress tears down every E2 route and the E3 proxy. handleStop calls it
// after cancelling the lifecycle; the E1 loopback closes with the server.
func (s *shim) stopEgress() {
	s.egressMu.Lock()
	routes := make([]*egressRoute, 0, len(s.egressRoutes))
	for _, r := range s.egressRoutes {
		routes = append(routes, r)
	}
	s.egressRoutes = make(map[string]*egressRoute)
	proxy := s.egressProxy
	s.egressProxy = nil
	s.egressMu.Unlock()

	for _, r := range routes {
		r.shutdown()
	}
	if proxy != nil {
		proxy.shutdown()
	}
}
