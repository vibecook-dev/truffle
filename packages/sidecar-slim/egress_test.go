package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/net/tsdial"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

// RFC 026 unit rows: the pieces of the egress engine that need no tailnet.
// The two-node rows (a real SOCKS5 dial, a WebSocket through a route, a
// CONNECT tunnel) live in egress_tailnet_test.go.

// tapEvent is one JSON event the shim wrote, with its data kept raw.
type tapEvent struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func (ev tapEvent) requestID() string {
	var probe struct {
		RequestID string `json:"requestId"`
	}
	_ = json.Unmarshal(ev.Data, &probe)
	return probe.RequestID
}

func (ev tapEvent) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(ev.Data, v); err != nil {
		t.Fatalf("decode %s data %s: %v", ev.Event, ev.Data, err)
	}
}

// eventTap records every event a shim writes. json.Encoder hands each event
// to Write in one call, so one Write is one event. Events are kept, never
// consumed, so a runtime event cannot be lost to a waiter looking for a
// reply.
type eventTap struct {
	mu     sync.Mutex
	events []tapEvent
	notify chan struct{}
}

func newEventTap() *eventTap { return &eventTap{notify: make(chan struct{}, 1)} }

func (e *eventTap) Write(p []byte) (int, error) {
	var ev tapEvent
	if err := json.Unmarshal(p, &ev); err == nil {
		e.mu.Lock()
		e.events = append(e.events, ev)
		e.mu.Unlock()
		select {
		case e.notify <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

// find waits for the first recorded event matching match.
func (e *eventTap) find(t *testing.T, what string, match func(tapEvent) bool) tapEvent {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		e.mu.Lock()
		for _, ev := range e.events {
			if match(ev) {
				e.mu.Unlock()
				return ev
			}
		}
		e.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for %s", what)
		}
		select {
		case <-e.notify:
		case <-time.After(remaining):
		}
	}
}

// reply waits for the event that answers requestID — the routing the core's
// reply broker does.
func (e *eventTap) reply(t *testing.T, requestID string) tapEvent {
	t.Helper()
	return e.find(t, "the reply to "+requestID, func(ev tapEvent) bool { return ev.requestID() == requestID })
}

func newTappedShim() (*shim, *eventTap) {
	tap := newEventTap()
	s := newTestShim()
	s.writer = json.NewEncoder(tap)
	return s, tap
}

func mustAddr(s string) netip.Addr { return netip.MustParseAddr(s) }

// egressTestStatus is a netmap as the LocalAPI reports it: self (alpha) and
// three peers — bob's beta (IPv6 listed FIRST, so a name must still dial the
// IPv4), a tagged node, and a node whose owner has no profile.
func egressTestStatus() *ipnstate.Status {
	alpha := &ipnstate.PeerStatus{
		DNSName:      "alpha.tail-scale.ts.net.",
		TailscaleIPs: []netip.Addr{mustAddr("100.64.0.1"), mustAddr("fd7a:115c:a1e0::1")},
		UserID:       1,
	}
	beta := &ipnstate.PeerStatus{
		DNSName:      "beta.tail-scale.ts.net.",
		TailscaleIPs: []netip.Addr{mustAddr("fd7a:115c:a1e0::2"), mustAddr("100.64.0.2")},
		UserID:       2,
	}
	tagged := &ipnstate.PeerStatus{
		DNSName:      "ci.tail-scale.ts.net.",
		TailscaleIPs: []netip.Addr{mustAddr("100.64.0.3")},
		UserID:       3,
	}
	ownerless := &ipnstate.PeerStatus{
		DNSName:      "ghost.tail-scale.ts.net.",
		TailscaleIPs: []netip.Addr{mustAddr("100.64.0.4")},
		UserID:       99,
	}
	return &ipnstate.Status{
		Self: alpha,
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): beta,
			key.NewNode().Public(): tagged,
			key.NewNode().Public(): ownerless,
		},
		User: map[tailcfg.UserID]tailcfg.UserProfile{
			1: {LoginName: "alice@example.com"},
			2: {LoginName: "bob@example.com"},
			3: {LoginName: "tagged-devices"},
		},
	}
}

func TestParseEgressTarget(t *testing.T) {
	for _, tc := range []struct {
		target   string
		wantHost string
		wantPort uint16
		ok       bool
	}{
		{"beta.tail-scale.ts.net:443", "beta.tail-scale.ts.net", 443, true},
		{"100.64.0.2:9417", "100.64.0.2", 9417, true},
		{"[fd7a:115c:a1e0::2]:80", "fd7a:115c:a1e0::2", 80, true},
		{"100.64.0.2", "", 0, false},       // no port
		{"100.64.0.2:0", "", 0, false},     // port 0 is not a destination
		{"100.64.0.2:65536", "", 0, false}, // out of range
		{"beta:http", "", 0, false},        // named ports are not resolved
		{":443", "", 0, false},             // no host
		{"", "", 0, false},
	} {
		host, port, refusal := parseEgressTarget(tc.target)
		if tc.ok {
			if refusal != nil || host != tc.wantHost || port != tc.wantPort {
				t.Errorf("parseEgressTarget(%q) = %q, %d, %v; want %q, %d", tc.target, host, port, refusal, tc.wantHost, tc.wantPort)
			}
			continue
		}
		if refusal == nil || refusal.code != egressInvalidTarget {
			t.Errorf("parseEgressTarget(%q) = %q, %d, %v; want %s", tc.target, host, port, refusal, egressInvalidTarget)
		}
	}
}

// TestAdmitEgressHost is the admission table: a destination is a node of
// this netmap, named by MagicDNS name or tailnet IP, whose owner passes the
// node's gate AND the route's glob — and nothing else.
func TestAdmitEgressHost(t *testing.T) {
	st := egressTestStatus()
	for _, tc := range []struct {
		name       string
		host       string
		nodeAllow  []string
		routeLogin string
		wantCode   string // "" = admitted
		wantAddr   string
	}{
		{name: "peer by MagicDNS name dials its IPv4", host: "beta.tail-scale.ts.net", wantAddr: "100.64.0.2"},
		{name: "name in any case with a trailing dot", host: "BETA.Tail-Scale.TS.net.", wantAddr: "100.64.0.2"},
		{name: "peer by tailnet IPv4", host: "100.64.0.2", wantAddr: "100.64.0.2"},
		{name: "peer by tailnet IPv6 dials that IPv6", host: "fd7a:115c:a1e0::2", wantAddr: "fd7a:115c:a1e0::2"},
		{name: "IPv4-mapped IPv6 is the IPv4", host: "::ffff:100.64.0.2", wantAddr: "100.64.0.2"},
		{name: "this node itself", host: "alpha.tail-scale.ts.net", wantAddr: "100.64.0.1"},

		{name: "tailnet-range IP not in the netmap", host: "100.64.0.9", wantCode: egressNotPeer},
		{name: "the quad-100 service address", host: "100.100.100.100", wantCode: egressNotPeer},
		{name: "a public name", host: "example.com", wantCode: egressNotPeer},
		{name: "a LAN address", host: "192.168.1.10", wantCode: egressNotPeer},
		{name: "a .ts.net name of ANOTHER tailnet", host: "beta.tail-other.ts.net", wantCode: egressNotPeer},
		{name: "a short MagicDNS name", host: "beta", wantCode: egressNotPeer},

		{name: "node gate admits the owner", host: "beta.tail-scale.ts.net", nodeAllow: []string{"bob@example.com"}, wantAddr: "100.64.0.2"},
		{name: "node gate glob is case-insensitive", host: "100.64.0.2", nodeAllow: []string{"*@EXAMPLE.com"}, wantAddr: "100.64.0.2"},
		{name: "node gate refuses a foreign owner", host: "beta.tail-scale.ts.net", nodeAllow: []string{"alice@example.com"}, wantCode: egressLoginRefused},
		{name: "node gate refuses a tagged node", host: "100.64.0.3", nodeAllow: []string{"*@example.com"}, wantCode: egressLoginRefused},
		{name: "gated, owner unknown: fail closed", host: "100.64.0.4", nodeAllow: []string{"*@example.com"}, wantCode: egressLoginUnknown},
		{name: "ungated, owner unknown: admitted", host: "100.64.0.4", wantAddr: "100.64.0.4"},

		{name: "route glob admits", host: "beta.tail-scale.ts.net", routeLogin: "bob@*", wantAddr: "100.64.0.2"},
		{name: "route glob refuses", host: "alpha.tail-scale.ts.net", routeLogin: "bob@*", wantCode: egressLoginRefused},
		{name: "route glob, owner unknown", host: "100.64.0.4", routeLogin: "*", wantCode: egressLoginUnknown},
		{name: "gate admits, route refuses", host: "100.64.0.2", nodeAllow: []string{"*@example.com"}, routeLogin: "alice@*", wantCode: egressLoginRefused},
		{name: "route admits, gate refuses", host: "100.64.0.2", nodeAllow: []string{"alice@*"}, routeLogin: "bob@*", wantCode: egressLoginRefused},
		{name: "a malformed glob never matches", host: "100.64.0.2", routeLogin: "[", wantCode: egressLoginRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, refusal := admitEgressHost(st, tc.host, tc.nodeAllow, tc.routeLogin)
			if tc.wantCode != "" {
				if refusal == nil || refusal.code != tc.wantCode {
					t.Fatalf("admitEgressHost(%q) = %+v, %+v; want refusal %s", tc.host, node, refusal, tc.wantCode)
				}
				if refusal.message == "" {
					t.Error("a refusal must say why")
				}
				return
			}
			if refusal != nil {
				t.Fatalf("admitEgressHost(%q) refused: %s %s", tc.host, refusal.code, refusal.message)
			}
			if node.addr != mustAddr(tc.wantAddr) {
				t.Errorf("admitEgressHost(%q) dials %v, want %s", tc.host, node.addr, tc.wantAddr)
			}
		})
	}

	if _, refusal := admitEgressHost(nil, "beta.tail-scale.ts.net", nil, ""); refusal == nil || refusal.code != egressStatusFailed {
		t.Errorf("no status must refuse with %s, got %+v", egressStatusFailed, refusal)
	}
}

func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func TestProxyAuthorized(t *testing.T) {
	const cred = "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name   string
		header string
		want   bool
	}{
		{"the credential", basic("truffle", cred), true},
		{"any username", basic("", cred), true},
		{"scheme in any case", "basic " + base64.StdEncoding.EncodeToString([]byte("x:"+cred)), true},
		{"a wrong password", basic("truffle", "nope"), false},
		{"a prefix of the credential", basic("truffle", cred[:16]), false},
		{"the credential as the username", basic(cred, ""), false},
		{"no header", "", false},
		{"bearer scheme", "Bearer " + cred, false},
		{"not base64", "Basic !!!", false},
		{"no colon", "Basic " + base64.StdEncoding.EncodeToString([]byte(cred)), false},
	} {
		if got := proxyAuthorized(tc.header, cred); got != tc.want {
			t.Errorf("%s: proxyAuthorized = %v, want %v", tc.name, got, tc.want)
		}
	}
	if proxyAuthorized(basic("x", ""), "") {
		t.Error("an empty credential must never authorize")
	}
}

// TestEgressWireShapes pins what the core parses: camelCase keys, the
// LocalAPI credential absent unless asked for, and an empty route list as
// [] rather than null.
func TestEgressWireShapes(t *testing.T) {
	var add egressAddData
	if err := json.Unmarshal([]byte(`{"id":"a","target":"beta.tail-scale.ts.net:9417","login":"bob@*","nodeLoginAllow":["*@example.com"],"requestId":"r1"}`), &add); err != nil {
		t.Fatal(err)
	}
	if add.ID != "a" || add.Target != "beta.tail-scale.ts.net:9417" || add.Login != "bob@*" ||
		len(add.NodeLoginAllow) != 1 || add.RequestID != "r1" {
		t.Errorf("egress:add parsed as %+v", add)
	}

	for _, tc := range []struct {
		name string
		v    any
		want string
	}{
		{"loopback without LocalAPI", loopbackResultData{Address: "127.0.0.1:1", ProxyCredential: "p", RequestID: "r"},
			`{"address":"127.0.0.1:1","proxyCredential":"p","requestId":"r"}`},
		{"loopback with LocalAPI", loopbackResultData{Address: "127.0.0.1:1", ProxyCredential: "p", LocalAPICredential: "l"},
			`{"address":"127.0.0.1:1","proxyCredential":"p","localAPICredential":"l"}`},
		{"added", egressAddedData{ID: "a", Listen: "127.0.0.1:2", RequestID: "r"},
			`{"id":"a","listen":"127.0.0.1:2","requestId":"r"}`},
		{"empty list", egressListResultData{Routes: []egressRouteInfo{}},
			`{"routes":[]}`},
		{"list row", egressListResultData{Routes: []egressRouteInfo{{ID: "a", Target: "t:1", Listen: "127.0.0.1:2"}}},
			`{"routes":[{"id":"a","target":"t:1","listen":"127.0.0.1:2"}]}`},
		{"proxy", egressProxyResultData{Address: "127.0.0.1:3", Credential: "c", RequestID: "r"},
			`{"address":"127.0.0.1:3","credential":"c","requestId":"r"}`},
		{"runtime error", egressErrorData{ID: "a", Code: egressLoginRefused, Message: "m"},
			`{"id":"a","code":"LOGIN_REFUSED","message":"m"}`},
	} {
		got, err := json.Marshal(tc.v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}

// TestEgressCommandsAnswerWithoutANode: every egress verb terminates with an
// event carrying the command's requestId even when no node is running, so
// the core fails the waiter at once instead of timing out.
func TestEgressCommandsAnswerWithoutANode(t *testing.T) {
	s, tap := newTappedShim()
	defer s.cancel()

	s.handleLoopback(json.RawMessage(`{"requestId":"lb"}`))
	s.handleEgressAdd(json.RawMessage(`{"id":"a","target":"beta.tail-scale.ts.net:1","requestId":"add"}`))
	s.handleEgressProxy(json.RawMessage(`{"requestId":"px"}`))
	s.handleEgressRemove(json.RawMessage(`{"id":"a","requestId":"rm"}`))
	s.handleEgressList(json.RawMessage(`{"requestId":"ls"}`))
	s.handleEgressAdd(json.RawMessage(`{"id":5,"requestId":"bad"}`))

	for _, tc := range []struct{ requestID, event, code string }{
		{"lb", "tsnet:error", "NOT_RUNNING"},
		{"add", "egress:error", "NOT_RUNNING"},
		{"px", "egress:error", "NOT_RUNNING"},
		{"rm", "egress:error", "NOT_FOUND"},
		{"bad", "egress:error", "INVALID_COMMAND"},
	} {
		ev := tap.reply(t, tc.requestID)
		var data struct{ Code string }
		ev.decode(t, &data)
		if ev.Event != tc.event || data.Code != tc.code {
			t.Errorf("%s: got %s %s, want %s %s", tc.requestID, ev.Event, ev.Data, tc.event, tc.code)
		}
	}
	ev := tap.reply(t, "ls")
	if ev.Event != "egress:list" || !strings.Contains(string(ev.Data), `"routes":[]`) {
		t.Errorf("an empty egress:list = %s %s", ev.Event, ev.Data)
	}
}

// TestEgressRouteShutdownClosesEverything: a route's shutdown closes its
// listener and every tracked connection, and a connection that races in
// after it is closed rather than kept.
func TestEgressRouteShutdownClosesEverything(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &egressRoute{id: "r", listener: ln}
	clientEnd, routeEnd := net.Pipe()
	if !r.track(routeEnd) {
		t.Fatal("a live route must track a connection")
	}

	r.shutdown()
	r.shutdown() // idempotent

	_ = clientEnd.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientEnd.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("a tracked connection must be closed by shutdown, read err = %v", err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("the listener must be closed by shutdown, accept err = %v", err)
	}
	late, lateRoute := net.Pipe()
	if r.track(lateRoute) {
		t.Error("a shut-down route must refuse to track")
	}
	_ = late.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := late.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("a connection refused by a shut-down route must be closed, read err = %v", err)
	}

	(&egressRoute{id: "placeholder"}).shutdown() // nothing bound yet: must not panic
}

// TestHandleStopTearsDownEgress: tsnet:stop closes every route and the
// CONNECT proxy, and leaves an empty table for the next lifecycle.
func TestHandleStopTearsDownEgress(t *testing.T) {
	s, _ := newTappedShim()

	routeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &egressHTTPProxy{address: proxyLn.Addr().String(), credential: "c"}
	proxy.server = &http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	served := make(chan error, 1)
	go func() { served <- proxy.server.Serve(proxyLn) }()

	s.egressRoutes = map[string]*egressRoute{
		"live":    {id: "live", listener: routeLn, listen: routeLn.Addr().String()},
		"pending": {id: "pending"},
	}
	s.egressProxy = proxy

	s.handleStop()

	if len(s.egressRoutes) != 0 || s.egressProxy != nil {
		t.Fatalf("stop left egress state: %d routes, proxy %v", len(s.egressRoutes), s.egressProxy)
	}
	if _, err := routeLn.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("stop must close a route's listener, accept err = %v", err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("proxy Serve returned %v, want ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop must close the CONNECT proxy")
	}
}

// Admission and dialing are separate operations. A peer disappearing in
// between must cause a tailnet failure, never a retry through the host.
func TestEgressDialUsesNetstackAfterPeerLeaves(t *testing.T) {
	for _, host := range []string{"beta.tail-scale.ts.net", "fd7a:115c:a1e0::2"} {
		t.Run(host, func(t *testing.T) {
			node, refusal := admitEgressHost(egressTestStatus(), host, nil, "")
			if refusal != nil {
				t.Fatal(refusal)
			}
			target := netip.AddrPortFrom(node.addr, 443)
			hostDials, netstackDials := 0, 0
			gone := errors.New("admitted peer is no longer reachable")
			dialer := tsdial.NewFromFuncForDebug(t.Logf, func(context.Context, string, string) (net.Conn, error) {
				hostDials++
				return nil, errors.New("escaped to the host network")
			})
			// The later routing snapshot has lost the admitted peer. A
			// system-dial hook keeps this test entirely off the real network.
			dialer.UseNetstackForIP = func(netip.Addr) bool { return false }
			dialer.SetRoutes(nil, []netip.Prefix{netip.MustParsePrefix("0.0.0.0/0"), netip.MustParsePrefix("::/0")})
			dialer.NetstackDialTCP = func(_ context.Context, dst netip.AddrPort) (net.Conn, error) {
				netstackDials++
				if dst != target {
					t.Errorf("dialed %v, admitted %v", dst, target)
				}
				return nil, gone
			}
			conn, err := dialEgress(context.Background(), dialer, target)
			if conn != nil {
				conn.Close()
				t.Error("unreachable peer returned a connection")
			}
			if hostDials != 0 || netstackDials != 1 || !errors.Is(err, gone) {
				t.Fatalf("host dials = %d, netstack dials = %d, error = %v; must fail inside netstack", hostDials, netstackDials, err)
			}
			dialer.NetstackDialTCP = nil
			if conn, err := dialEgress(context.Background(), dialer, target); conn != nil || err == nil || hostDials != 0 {
				if conn != nil {
					conn.Close()
				}
				t.Fatalf("an unavailable netstack must fail closed: conn=%v error=%v host dials=%d", conn, err, hostDials)
			}
		})
	}
}

// The idle bound applies before bridgeCopy too: either the CONNECT response
// or a pipelined ClientHello can encounter backpressure during tunnel setup.
func TestEgressConnectSetupHonorsIdleTimeout(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		name := "blocked response"
		if pipelined {
			name = "blocked pipelined payload"
		}
		t.Run(name, func(t *testing.T) {
			client, clientPeer := net.Pipe()
			upstream, upstreamPeer := net.Pipe()
			defer clientPeer.Close()
			defer upstreamPeer.Close()
			buffered := bufio.NewReader(strings.NewReader(""))
			if pipelined {
				buffered = bufio.NewReader(strings.NewReader("pipelined hello"))
				if _, err := buffered.Peek(1); err != nil {
					t.Fatal(err)
				}
				// Drain the successful CONNECT response. Only the upstream
				// write remains blocked in this case.
				go io.Copy(io.Discard, clientPeer)
			}
			done := make(chan struct{})
			go func() {
				spliceEgressTunnel(upstream, client, buffered, 30*time.Millisecond)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				client.Close()
				upstream.Close()
				<-done
				t.Fatal("CONNECT setup ignored the stream idle timeout")
			}
			if _, err := client.Write([]byte{1}); err == nil {
				t.Error("client connection remains open")
			}
			if _, err := upstream.Write([]byte{1}); err == nil {
				t.Error("upstream connection remains open")
			}
		})
	}
}
