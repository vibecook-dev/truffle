package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Protocol 13 (RFC 030): proxy:add's response headers, on the serve and on
// each route, are set on every response the serve writes.

// responseHeaderVectors is the shared table. truffle-core's validate_config is
// tested against the same file (crates/truffle-core/src/proxy/mod.rs), so the
// two validators cannot drift apart.
type responseHeaderVectors struct {
	Bounds  responseHeaderBounds `json:"bounds"`
	Refused []string             `json:"refused"`
	Cases   []responseHeaderCase `json:"cases"`
}

type responseHeaderBounds struct {
	MaxHeaders    int `json:"maxHeaders"`
	MaxNameBytes  int `json:"maxNameBytes"`
	MaxValueBytes int `json:"maxValueBytes"`
	MaxListBytes  int `json:"maxListBytes"`
}

type responseHeaderCase struct {
	ID        string                 `json:"id"`
	Serve     []responseHeaderVector `json:"serve"`
	Route     []responseHeaderVector `json:"route"`
	ServeFill *responseHeaderFill    `json:"serveFill"`
	RouteFill *responseHeaderFill    `json:"routeFill"`
	OK        bool                   `json:"ok"`
	Error     string                 `json:"error"`
}

type responseHeaderVector struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Repeat *int   `json:"repeat"`
}

type responseHeaderFill struct {
	Prefix string `json:"prefix"`
	Count  int    `json:"count"`
}

func loadResponseHeaderVectors(t *testing.T) responseHeaderVectors {
	t.Helper()
	path := filepath.Join("..", "..", "crates", "truffle-core", "src", "proxy", "response_headers_vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the shared vectors: %v", err)
	}
	var v responseHeaderVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode the shared vectors: %v", err)
	}
	return v
}

// vectorHeaders builds a case's list: each value its unit repeated `repeat`
// times, then fill's generated {prefix}1..{prefix}N, each valued "v".
func vectorHeaders(list []responseHeaderVector, fill *responseHeaderFill) []proxyHeaderData {
	var hs []proxyHeaderData
	for _, h := range list {
		repeat := 1
		if h.Repeat != nil {
			repeat = *h.Repeat
		}
		hs = append(hs, proxyHeaderData{Name: h.Name, Value: strings.Repeat(h.Value, repeat)})
	}
	if fill != nil {
		for i := 1; i <= fill.Count; i++ {
			hs = append(hs, proxyHeaderData{Name: fill.Prefix + strconv.Itoa(i), Value: "v"})
		}
	}
	return hs
}

// proxyAddData is a case as the wire carries it: its serve list on the serve,
// and its route list (when it has one) on a single "/" dir route.
func (c responseHeaderCase) proxyAddData() proxyAddData {
	data := proxyAddData{ID: c.ID, Name: c.ID, Headers: vectorHeaders(c.Serve, c.ServeFill)}
	if c.Route != nil || c.RouteFill != nil {
		data.Routes = []proxyRouteData{{Prefix: "/", Dir: "/srv", Headers: vectorHeaders(c.Route, c.RouteFill)}}
	}
	return data
}

func TestResponseHeaderVectors(t *testing.T) {
	v := loadResponseHeaderVectors(t)
	if len(v.Cases) <= 50 {
		t.Fatalf("the table lost its cases: %d left", len(v.Cases))
	}
	for _, c := range v.Cases {
		failure := validateResponseHeaders(c.proxyAddData())
		switch {
		case c.OK && failure != nil:
			t.Errorf("%s: refused: %s", c.ID, failure.message)
		case !c.OK && failure == nil:
			t.Errorf("%s: accepted", c.ID)
		case !c.OK && (failure.code != "INVALID_HEADER" || c.Error == "" || !strings.Contains(failure.message, c.Error)):
			t.Errorf("%s: %s %q lacks %q", c.ID, failure.code, failure.message, c.Error)
		}
	}
}

func TestResponseHeaderBoundsAndRefusedNamesAreTheVectors(t *testing.T) {
	v := loadResponseHeaderVectors(t)
	ours := responseHeaderBounds{
		MaxHeaders:    maxResponseHeaders,
		MaxNameBytes:  maxResponseHeaderNameBytes,
		MaxValueBytes: maxResponseHeaderValueBytes,
		MaxListBytes:  maxResponseHeadersBytes,
	}
	if v.Bounds != ours {
		t.Errorf("bounds = %+v, the vectors say %+v", ours, v.Bounds)
	}
	refused := append(slices.Clone(refusedResponseHeaders), refusedResponseHeaderPrefix+"*")
	theirs := slices.Clone(v.Refused)
	slices.Sort(refused)
	slices.Sort(theirs)
	if !slices.Equal(refused, theirs) {
		t.Errorf("refused names = %q, the vectors say %q", refused, theirs)
	}
}

// The wire shape the core sends at protocol 13 (protocol.rs's
// serialize_proxy_add_command_v2_routes) reads back into both lists.
func TestProxyAddDataReadsHeaders(t *testing.T) {
	raw := `{"id":"app","listenPort":8443,"tls":false,` +
		`"headers":[{"name":"Content-Security-Policy","value":"default-src 'self'"}],` +
		`"routes":[{"prefix":"/","dir":"/srv","headers":[{"name":"X-Frame-Options","value":"DENY"}]}]}`
	var d proxyAddData
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatal(err)
	}
	if want := []proxyHeaderData{{Name: "Content-Security-Policy", Value: "default-src 'self'"}}; !reflect.DeepEqual(d.Headers, want) {
		t.Errorf("serve headers = %#v, want %#v", d.Headers, want)
	}
	if want := []proxyHeaderData{{Name: "X-Frame-Options", Value: "DENY"}}; len(d.Routes) != 1 || !reflect.DeepEqual(d.Routes[0].Headers, want) {
		t.Errorf("routes = %#v, want one with headers %#v", d.Routes, want)
	}
}

func TestMergeResponseHeaders(t *testing.T) {
	if h := mergeResponseHeaders(nil, nil); h != nil {
		t.Fatalf("no headers merged to %v, want nil", h)
	}
	serve := []proxyHeaderData{
		{Name: "X-Frame-Options", Value: "DENY"},
		{Name: "content-security-policy", Value: "default-src 'self'"},
	}
	route := []proxyHeaderData{{Name: "x-frame-options", Value: "SAMEORIGIN"}}
	want := http.Header{
		"X-Frame-Options":         {"SAMEORIGIN"},
		"Content-Security-Policy": {"default-src 'self'"},
	}
	if h := mergeResponseHeaders(serve, route); !reflect.DeepEqual(h, want) {
		t.Fatalf("merged = %v, want %v (the route's replaces the serve's; the rest is inherited)", h, want)
	}
}

func TestApplyResponseHeadersReplacesAndCopies(t *testing.T) {
	policy := http.Header{"X-Frame-Options": {"DENY"}}
	dst := http.Header{"X-Frame-Options": {"SAMEORIGIN", "ALLOW-FROM x"}, "X-Backend": {"yes"}}
	applyResponseHeaders(dst, policy)
	if got := dst.Values("X-Frame-Options"); !slices.Equal(got, []string{"DENY"}) {
		t.Fatalf("X-Frame-Options = %q, want the policy's alone", got)
	}
	if got := dst.Get("X-Backend"); got != "yes" {
		t.Fatalf("X-Backend = %q, want it untouched", got)
	}
	dst.Add("X-Frame-Options", "later")
	if got := policy.Values("X-Frame-Options"); !slices.Equal(got, []string{"DENY"}) {
		t.Fatalf("a later Add reached the shared policy: %q", got)
	}
}

// TestResponseHeadersOnEveryResponse drives the real handler through a
// loopback listener: the serve's headers ride static files (200, SPA fallback,
// HEAD, 304, 404, 405, 416), proxied responses (replacing the backend's values,
// also after a relayed 103, and on a 101 that ReverseProxy relays itself), the
// engine's 502 on both proxy paths, the gate's 403 and the no-route 404; a
// route's headers ride that route alone.
func TestResponseHeadersOnEveryResponse(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<h1>app</h1>"), 0o644); err != nil {
		t.Fatal(err)
	}
	// The proxy's own listener first, so the dead backend's freed port below
	// can never be handed to it.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/upgrade" {
			// An upgrade that is not a WebSocket: the sidecar hands it to
			// ReverseProxy, which relays this 101 itself.
			conn, brw, err := http.NewResponseController(w).Hijack()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer conn.Close()
			_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n" +
				"Connection: Upgrade\r\nUpgrade: foo\r\n" +
				"Content-Security-Policy: default-src *\r\nX-Backend: yes\r\n\r\n")
			_ = brw.Flush()
			return
		}
		if r.URL.Path == "/api/early" {
			w.Header().Set("Link", "</app/style.css>; rel=preload")
			w.WriteHeader(http.StatusEarlyHints)
		}
		w.Header().Set("Content-Security-Policy", "default-src *")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		w.Header().Set("X-Backend", "yes")
		_, _ = io.WriteString(w, "backend")
	}))
	defer backend.Close()
	// A closed server frees its port, so a dial to it is refused.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	s := newTestShim()
	defer s.cancel()
	defer s.handleStop()
	s.setDNSName("test.tail-example.ts.net")
	tlsOff := false
	serveHeaders := []proxyHeaderData{
		{Name: "Content-Security-Policy", Value: "default-src 'self'"},
		{Name: "Referrer-Policy", Value: "no-referrer"},
	}
	routes := []proxyRouteData{
		{Prefix: "/app", Dir: dir, Fallback: "/index.html"},
		{Prefix: "/api", TargetURL: backend.URL, Headers: []proxyHeaderData{{Name: "X-Frame-Options", Value: "DENY"}}},
		{Prefix: "/dead", TargetURL: deadURL},
		{Prefix: "/admin", Dir: dir, Allow: []string{"root@example.com"}, Headers: []proxyHeaderData{{Name: "X-Admin", Value: "1"}}},
	}
	data := proxyAddData{
		ID: "headers", Name: "headers", Tls: &tlsOff,
		ListenPort: uint16(listener.Addr().(*net.TCPAddr).Port),
		Headers:    serveHeaders,
		Routes:     routes,
	}
	s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) { return listener, nil })
	waitForProxyEntry(t, s, data.ID)

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err == nil {
				// Supply the verified identity at the WhoIs cache seam.
				s.storeCachedIdentity(conn.LocalAddr().String(), peerIdentityData{LoginName: "alice@example.com"}, time.Now())
			}
			return conn, err
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	do := func(t *testing.T, ctx context.Context, method, path string, header http.Header) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, "http://"+listener.Addr().String()+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for name, values := range header {
			req.Header[name] = values
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		return res
	}

	serve := http.Header{
		"Content-Security-Policy": {"default-src 'self'"},
		"Referrer-Policy":         {"no-referrer"},
	}
	with := func(extra http.Header) http.Header {
		h := serve.Clone()
		for name, values := range extra {
			h[name] = values
		}
		return h
	}
	proxied := with(http.Header{"X-Frame-Options": {"DENY"}, "X-Backend": {"yes"}})
	future := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	upgrade := http.Header{"Connection": {"Upgrade"}, "Upgrade": {"websocket"}}
	for _, tc := range []struct {
		name, method, path string
		header             http.Header
		status             int
		want               http.Header
		absent             []string
	}{
		{"static file", http.MethodGet, "/app/", nil, http.StatusOK, serve, []string{"X-Frame-Options", "X-Admin"}},
		{"spa fallback", http.MethodGet, "/app/client/route", nil, http.StatusOK, serve, nil},
		{"head", http.MethodHead, "/app/", nil, http.StatusOK, serve, nil},
		{"not modified", http.MethodGet, "/app/index.html", http.Header{"If-Modified-Since": {future}}, http.StatusNotModified, serve, nil},
		{"dotfile 404", http.MethodGet, "/app/.env", nil, http.StatusNotFound, serve, nil},
		{"static 405", http.MethodPost, "/app/", nil, http.StatusMethodNotAllowed, serve, nil},
		{"static 416", http.MethodGet, "/app/index.html", http.Header{"Range": {"bytes=999999-"}}, http.StatusRequestedRangeNotSatisfiable, serve, nil},
		{"proxied", http.MethodGet, "/api/x", nil, http.StatusOK, proxied, nil},
		{"dead backend 502", http.MethodGet, "/dead/", nil, http.StatusBadGateway, serve, []string{"X-Frame-Options"}},
		{"websocket dial 502", http.MethodGet, "/dead/socket", upgrade, http.StatusBadGateway, serve, nil},
		{"gate 403", http.MethodGet, "/admin/", nil, http.StatusForbidden, with(http.Header{"X-Admin": {"1"}}), nil},
		{"no-route 404", http.MethodGet, "/elsewhere", nil, http.StatusNotFound, serve, []string{"X-Frame-Options", "X-Admin"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := do(t, context.Background(), tc.method, tc.path, tc.header)
			if res.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.status)
			}
			for name, want := range tc.want {
				if got := res.Header.Values(name); !slices.Equal(got, want) {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
			for _, name := range tc.absent {
				if got := res.Header.Values(name); len(got) != 0 {
					t.Errorf("%s = %q, want it absent", name, got)
				}
			}
		})
	}

	// A relayed 103 clears the writer's headers; the policy still rides the
	// final response because it is set on the backend's response, not ahead.
	t.Run("proxied after a 103", func(t *testing.T) {
		codes := make(chan int, 8)
		trace := &httptrace.ClientTrace{
			Got1xxResponse: func(code int, _ textproto.MIMEHeader) error {
				codes <- code
				return nil
			},
		}
		res := do(t, httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, "/api/early", nil)
		close(codes)
		var informational []int
		for code := range codes {
			informational = append(informational, code)
		}
		if !slices.Equal(informational, []int{http.StatusEarlyHints}) {
			t.Fatalf("informational responses = %v, want one 103", informational)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", res.StatusCode)
		}
		for name, want := range proxied {
			if got := res.Header.Values(name); !slices.Equal(got, want) {
				t.Errorf("%s = %q, want %q", name, got, want)
			}
		}
	})

	// An upgrade that is not a WebSocket is not hijacked by the sidecar:
	// ReverseProxy relays the backend's 101 itself, after ModifyResponse, so
	// the policy rides it. (A WebSocket upgrade's 101 is the backend's own.)
	t.Run("proxied 101", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String()+"/api/upgrade", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "foo")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		// The body is the switched connection: close it unread.
		_ = res.Body.Close()
		if res.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d, want 101", res.StatusCode)
		}
		for name, want := range proxied {
			if got := res.Header.Values(name); !slices.Equal(got, want) {
				t.Errorf("%s = %q, want %q", name, got, want)
			}
		}
	})
}

// TestResponseHeadersOnASingleTargetServe: a v1 serve (no routes) is its own
// branch of bindProxyRoutes, with its own merge and wrap. Its list replaces the
// backend's value on a proxied 200, leaves the names it does not set to the
// backend, and rides the engine's 502 for a dead target.
func TestResponseHeadersOnASingleTargetServe(t *testing.T) {
	// The proxies' own listeners first, so the dead backend's freed port below
	// can never be handed to either.
	liveListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer liveListener.Close()
	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer deadListener.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src *")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		_, _ = io.WriteString(w, "backend")
	}))
	defer backend.Close()
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	s := newTestShim()
	defer s.cancel()
	defer s.handleStop()
	s.setDNSName("test.tail-example.ts.net")
	tlsOff := false
	singleTarget := func(id, rawURL string, listener net.Listener) proxyAddData {
		t.Helper()
		target, err := url.Parse(rawURL)
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.ParseUint(target.Port(), 10, 16)
		if err != nil {
			t.Fatal(err)
		}
		return proxyAddData{
			ID: id, Name: id, Tls: &tlsOff,
			ListenPort:   uint16(listener.Addr().(*net.TCPAddr).Port),
			TargetHost:   target.Hostname(),
			TargetPort:   uint16(port),
			TargetScheme: target.Scheme,
			Headers: []proxyHeaderData{
				{Name: "Content-Security-Policy", Value: "default-src 'self'"},
				{Name: "Referrer-Policy", Value: "no-referrer"},
			},
		}
	}
	live := singleTarget("live", backend.URL, liveListener)
	s.startProxyAdd(nil, nil, live, func(bool, string) (net.Listener, error) { return liveListener, nil })
	waitForProxyEntry(t, s, live.ID)
	gone := singleTarget("gone", deadURL, deadListener)
	s.startProxyAdd(nil, nil, gone, func(bool, string) (net.Listener, error) { return deadListener, nil })
	waitForProxyEntry(t, s, gone.ID)

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err == nil {
				// Supply the verified identity at the WhoIs cache seam.
				s.storeCachedIdentity(conn.LocalAddr().String(), peerIdentityData{LoginName: "alice@example.com"}, time.Now())
			}
			return conn, err
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	for _, tc := range []struct {
		name     string
		listener net.Listener
		status   int
		want     http.Header
	}{
		{"proxied 200", liveListener, http.StatusOK, http.Header{
			"Content-Security-Policy": {"default-src 'self'"},
			"Referrer-Policy":         {"no-referrer"},
			"X-Frame-Options":         {"SAMEORIGIN"},
		}},
		{"dead target 502", deadListener, http.StatusBadGateway, http.Header{
			"Content-Security-Policy": {"default-src 'self'"},
			"Referrer-Policy":         {"no-referrer"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := client.Get("http://" + tc.listener.Addr().String() + "/")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			if res.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.status)
			}
			for name, want := range tc.want {
				if got := res.Header.Values(name); !slices.Equal(got, want) {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
		})
	}
}

// TestResponseHeadersRefusedAtAdd: the sidecar re-checks the headers off the
// wire before anything is bound (pattern of TestStaticRootInvalidIsStableAddError).
func TestResponseHeadersRefusedAtAdd(t *testing.T) {
	over := make([]proxyHeaderData, 0, maxResponseHeaders+1)
	for i := 0; i <= maxResponseHeaders; i++ {
		over = append(over, proxyHeaderData{Name: "X-Route-" + strconv.Itoa(i), Value: "v"})
	}
	for _, tc := range []struct {
		name  string
		serve []proxyHeaderData
		route []proxyHeaderData
	}{
		{"serve Set-Cookie", []proxyHeaderData{{Name: "Set-Cookie", Value: "s=1"}}, nil},
		{"route Connection", nil, []proxyHeaderData{{Name: "Connection", Value: "close"}}},
		{"serve CRLF in a value", []proxyHeaderData{{Name: "X-V", Value: "a\r\nSet-Cookie: s=1"}}, nil},
		{"route over the count", nil, over},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestShim()
			t.Cleanup(s.cancel)
			var output bytes.Buffer
			s.writer = json.NewEncoder(&output)
			setStaticRootOpener(t, func(string) (staticRoot, error) {
				t.Error("refused headers must fail the add before its static root is opened")
				return nil, fs.ErrPermission
			})
			data := staticProxyAddData("refused-headers", 8770, t.TempDir())
			data.Headers = tc.serve
			data.Routes[0].Headers = tc.route

			s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) {
				t.Fatal("listener creation must not run for refused headers")
				return nil, nil
			})

			var got struct {
				Event string              `json:"event"`
				Data  proxyErrorEventData `json:"data"`
			}
			if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &got); err != nil {
				t.Fatalf("decode event: %v; output = %q", err, output.String())
			}
			if got.Event != "proxy:error" || got.Data.Code != "INVALID_HEADER" {
				t.Fatalf("event = %#v, want proxy:error/INVALID_HEADER", got)
			}
			s.proxyMu.Lock()
			_, exists := s.proxies[data.ID]
			s.proxyMu.Unlock()
			if exists {
				t.Fatal("refused headers installed a placeholder or live proxy")
			}
		})
	}
}
