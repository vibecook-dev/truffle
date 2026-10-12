package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"tailscale.com/derp/derpserver"
	"tailscale.com/ipn/store/mem"
	"tailscale.com/net/netns"
	"tailscale.com/net/stun/stuntest"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
	"tailscale.com/tstest/integration/testcontrol"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
	"tailscale.com/types/nettype"
)

// RFC 026 two-node rows: every egress rung against real tsnet nodes on an
// in-process tailnet. No auth key and no network beyond loopback.

// testTailnet is an in-process tailnet: tailscale's testcontrol with a local
// DERP and STUN server — the harness tsnet's own suite stands its nodes up
// on (tsnet_test.go startControl), rebuilt from derpserver and stuntest so
// the sidecar module does not take on tstest/integration's dependency tree.
// Every node is a real tsnet.Server, and each registers as its own user,
// user-N@fake-control.example.net, in registration order.
type testTailnet struct {
	control *testcontrol.Server
	url     string
}

func newTestTailnet(t *testing.T) *testTailnet {
	t.Helper()
	// As tsnet's own tests (Corp#4520): no netns in tests.
	netns.SetEnabled(false)
	t.Cleanup(func() { netns.SetEnabled(true) })

	derp := derpserver.New(key.NewNode(), logger.Discard)
	derpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	derpHTTP := httptest.NewUnstartedServer(derpserver.Handler(derp))
	derpHTTP.Listener.Close()
	derpHTTP.Listener = derpLn
	derpHTTP.Config.ErrorLog = logger.StdLogger(logger.Discard)
	derpHTTP.Config.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler))
	derpHTTP.StartTLS()
	stunAddr, stunCleanup := stuntest.ServeWithPacketListener(t, nettype.Std{})
	t.Cleanup(func() {
		derpHTTP.CloseClientConnections()
		derpHTTP.Close()
		derp.Close()
		stunCleanup()
	})

	control := &testcontrol.Server{
		DERPMap: &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{
			1: {RegionID: 1, RegionCode: "test", Nodes: []*tailcfg.DERPNode{{
				Name: "t1", RegionID: 1, HostName: "127.0.0.1", IPv4: "127.0.0.1", IPv6: "none",
				STUNPort: stunAddr.Port, DERPPort: derpLn.Addr().(*net.TCPAddr).Port,
				InsecureForTests: true, STUNTestIP: "127.0.0.1",
			}}},
		}},
		DNSConfig:      &tailcfg.DNSConfig{Proxied: true},
		MagicDNSDomain: "tail-scale.ts.net",
		Logf:           logger.Discard,
	}
	control.HTTPTestServer = httptest.NewUnstartedServer(control)
	control.HTTPTestServer.Start()
	t.Cleanup(control.HTTPTestServer.Close)
	return &testTailnet{control: control, url: control.HTTPTestServer.URL}
}

// node starts a real tsnet node and waits until it is up with a home DERP
// connection, so a fast test cannot race the DERP handshake into a dropped
// first packet (tsnet's own waitForHomeDERPConnected).
func (tn *testTailnet) node(t *testing.T, ctx context.Context, name string) *tsnet.Server {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &tsnet.Server{
		Dir: dir, ControlURL: tn.url, Hostname: name, Store: new(mem.Store), Ephemeral: true,
		Logf: logger.Discard, UserLogf: logger.Discard,
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.Up(ctx); err != nil {
		t.Fatalf("%s up: %v", name, err)
	}
	health := s.Sys().HealthTracker.Get()
	magic := s.Sys().MagicSock.Get()
	for {
		if r := magic.GetLastNetcheckReport(ctx); r != nil && r.PreferredDERP != 0 &&
			!health.GetDERPRegionReceivedTime(r.PreferredDERP).IsZero() {
			return s
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: no home DERP: %v", name, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// nodeFacts is what a test needs to know about a node, read from its own
// LocalAPI status.
type nodeFacts struct {
	ip      netip.Addr
	dnsName string // no trailing dot
	login   string
	userID  tailcfg.UserID
	nodeKey key.NodePublic
}

func factsOf(t *testing.T, ctx context.Context, s *tsnet.Server) nodeFacts {
	t.Helper()
	lc, err := s.LocalClient()
	if err != nil {
		t.Fatal(err)
	}
	st, err := lc.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f := nodeFacts{
		ip:      preferredTailnetAddr(st.Self.TailscaleIPs),
		dnsName: normalizeDNSName(st.Self.DNSName),
		login:   loginNameForUser(st.User, st.Self.UserID),
		userID:  st.Self.UserID,
		nodeKey: st.Self.PublicKey,
	}
	if !f.ip.IsValid() || f.dnsName == "" || f.login == "" {
		t.Fatalf("incomplete self status: %+v", f)
	}
	return f
}

// peerLoginAt reads the login alpha's netmap currently states for the peer
// holding ip ("" when absent).
func peerLoginAt(ctx context.Context, s *tsnet.Server, ip netip.Addr) string {
	lc, err := s.LocalClient()
	if err != nil {
		return ""
	}
	st, err := lc.Status(ctx)
	if err != nil {
		return ""
	}
	for _, p := range st.Peer {
		for _, a := range p.TailscaleIPs {
			if a == ip {
				return loginNameForUser(st.User, p.UserID)
			}
		}
	}
	return ""
}

// newEgressShim is a sidecar shim driving an already-running tsnet node,
// with every event it writes recorded.
func newEgressShim(t *testing.T, srv *tsnet.Server) (*shim, *eventTap) {
	t.Helper()
	s, tap := newTappedShim()
	s.setServer(srv)
	t.Cleanup(func() {
		s.stopEgress()
		s.cancel()
	})
	return s, tap
}

// serveEcho echoes every connection a listener accepts until it closes.
func serveEcho(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer c.Close()
			_, _ = io.Copy(c, c)
		}()
	}
}

// echoRecording echoes like serveEcho and reports each accepted
// connection's remote address — the address the far side's WhoIs resolves.
func echoRecording(ln net.Listener) <-chan string {
	seen := make(chan string, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case seen <- c.RemoteAddr().String():
			default:
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return seen
}

// expectFrom asserts the far side's tailnet attributes the next accepted
// connection to want: its WhoIs names the egressing node, not the host's
// other identities. That attribution is what a far door trusts.
func expectFrom(t *testing.T, ctx context.Context, far *tsnet.Server, remoteAddr string, want nodeFacts, what string) {
	t.Helper()
	lc, err := far.LocalClient()
	if err != nil {
		t.Fatal(err)
	}
	who, err := lc.WhoIs(ctx, remoteAddr)
	if err != nil {
		t.Fatalf("%s: the far side's WhoIs %s: %v", what, remoteAddr, err)
	}
	if got := normalizeDNSName(who.Node.Name); got != want.dnsName {
		t.Errorf("%s: the far side's WhoIs names %q, want the egressing node %q", what, got, want.dnsName)
	}
}

func nextRemote(t *testing.T, seen <-chan string, what string) string {
	t.Helper()
	select {
	case addr := <-seen:
		return addr
	case <-time.After(15 * time.Second):
		t.Fatalf("%s: no connection reached the far side", what)
		return ""
	}
}

func echoRoundTrip(t *testing.T, r io.Reader, conn net.Conn, msg string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	defer conn.SetDeadline(time.Time{})
	if _, err := io.WriteString(conn, msg); err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read the echo of %q: %v", msg, err)
	}
	if string(got) != msg {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
}

// expectClosed asserts the far end closed c without sending a byte.
func expectClosed(t *testing.T, c net.Conn, what string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
	n, err := c.Read(make([]byte, 1))
	var netErr net.Error
	switch {
	case n > 0:
		t.Fatalf("%s: read a byte from a connection that should be closed", what)
	case errors.As(err, &netErr) && netErr.Timeout():
		t.Fatalf("%s: the connection stayed open", what)
	case err == nil:
		t.Fatalf("%s: read returned no error", what)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ── E1 ────────────────────────────────────────────────────────────────────

var (
	errSOCKSMethodRefused = errors.New("socks5: the proxy accepted none of the offered auth methods")
	errSOCKSAuthRefused   = errors.New("socks5: the proxy refused the credential")
)

// socks5Connect speaks just enough of RFC 1928 and RFC 1929 to CONNECT
// through tsnet's loopback proxy. It offers exactly one auth method:
// username/password when user is non-empty, else "no authentication".
func socks5Connect(proxyAddr, user, pass, target string) (net.Conn, error) {
	c, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (net.Conn, error) {
		c.Close()
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))

	method := byte(0x00)
	if user != "" {
		method = 0x02
	}
	if _, err := c.Write([]byte{5, 1, method}); err != nil {
		return fail(err)
	}
	var choice [2]byte
	if _, err := io.ReadFull(c, choice[:]); err != nil {
		return fail(err)
	}
	if choice[1] != method {
		return fail(fmt.Errorf("%w (method reply %#x)", errSOCKSMethodRefused, choice[1]))
	}
	if method == 0x02 {
		msg := append([]byte{1, byte(len(user))}, user...)
		msg = append(msg, byte(len(pass)))
		msg = append(msg, pass...)
		if _, err := c.Write(msg); err != nil {
			return fail(err)
		}
		var status [2]byte
		if _, err := io.ReadFull(c, status[:]); err != nil {
			return fail(err)
		}
		if status[1] != 0 {
			return fail(errSOCKSAuthRefused)
		}
	}

	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return fail(err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return fail(err)
	}
	req := []byte{5, 1, 0}
	if ip, err := netip.ParseAddr(host); err == nil && ip.Is4() {
		req = append(append(req, 1), ip.AsSlice()...)
	} else if err == nil {
		req = append(append(req, 4), ip.AsSlice()...)
	} else {
		req = append(append(req, 3, byte(len(host))), host...)
	}
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := c.Write(req); err != nil {
		return fail(err)
	}
	var head [4]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return fail(err)
	}
	if head[1] != 0 {
		return fail(fmt.Errorf("socks5: CONNECT refused, reply %d", head[1]))
	}
	var bound int
	switch head[3] {
	case 1:
		bound = 4
	case 4:
		bound = 16
	case 3:
		var n [1]byte
		if _, err := io.ReadFull(c, n[:]); err != nil {
			return fail(err)
		}
		bound = int(n[0])
	default:
		return fail(fmt.Errorf("socks5: bound address type %d", head[3]))
	}
	if _, err := io.ReadFull(c, make([]byte, bound+2)); err != nil {
		return fail(err)
	}
	_ = c.SetDeadline(time.Time{})
	return c, nil
}

// TestEgressE1LoopbackSOCKS5: tsnet:loopback answers the node's loopback
// SOCKS5 proxy, and a dial through it reaches a peer's tsnet listener with
// the credential — and is refused without it.
func TestEgressE1LoopbackSOCKS5(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	alpha := tn.node(t, ctx, "alpha")
	beta := tn.node(t, ctx, "beta")
	alphaFacts := factsOf(t, ctx, alpha)
	betaFacts := factsOf(t, ctx, beta)

	// The peer's listener, as tsnet:listen opens one (srv.Listen).
	ln, err := beta.Listen("tcp", ":8081")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	seen := echoRecording(ln)

	s, tap := newEgressShim(t, alpha)
	s.handleLoopback(json.RawMessage(`{"requestId":"lb-1"}`))
	ev := tap.reply(t, "lb-1")
	if ev.Event != "tsnet:loopbackResult" {
		t.Fatalf("tsnet:loopback answered %s %s", ev.Event, ev.Data)
	}
	var lb loopbackResultData
	ev.decode(t, &lb)
	if !strings.HasPrefix(lb.Address, "127.0.0.1:") || len(lb.ProxyCredential) != 32 {
		t.Fatalf("loopback = %+v; want a 127.0.0.1 address and a 32-hex credential", lb)
	}
	if lb.LocalAPICredential != "" {
		t.Error("the LocalAPI credential crossed the bridge without being asked for")
	}

	byIP := net.JoinHostPort(betaFacts.ip.String(), "8081")
	c, err := socks5Connect(lb.Address, "tsnet", lb.ProxyCredential, byIP)
	if err != nil {
		t.Fatalf("SOCKS5 CONNECT %s with the credential: %v", byIP, err)
	}
	defer c.Close()
	echoRoundTrip(t, c, c, "through alpha, as alpha")
	expectFrom(t, ctx, beta, nextRemote(t, seen, "E1"), alphaFacts, "E1 SOCKS5")

	byName := net.JoinHostPort(betaFacts.dnsName, "8081")
	c2, err := socks5Connect(lb.Address, "tsnet", lb.ProxyCredential, byName)
	if err != nil {
		t.Fatalf("SOCKS5 CONNECT %s with the credential: %v", byName, err)
	}
	defer c2.Close()
	echoRoundTrip(t, c2, c2, "by MagicDNS name")

	if _, err := socks5Connect(lb.Address, "tsnet", "0123456789abcdef0123456789abcdef", byIP); !errors.Is(err, errSOCKSAuthRefused) {
		t.Errorf("a wrong credential: err = %v, want %v", err, errSOCKSAuthRefused)
	}
	if _, err := socks5Connect(lb.Address, "", "", byIP); !errors.Is(err, errSOCKSMethodRefused) {
		t.Errorf("no credential at all: err = %v, want %v", err, errSOCKSMethodRefused)
	}

	// Idempotent within the lifecycle; the LocalAPI credential only on request.
	s.handleLoopback(json.RawMessage(`{"requestId":"lb-2","localAPI":true}`))
	var lb2 loopbackResultData
	tap.reply(t, "lb-2").decode(t, &lb2)
	if lb2.Address != lb.Address || lb2.ProxyCredential != lb.ProxyCredential {
		t.Errorf("a second tsnet:loopback = %+v, want the first's address and credential", lb2)
	}
	if len(lb2.LocalAPICredential) != 32 {
		t.Fatalf("asked-for LocalAPI credential = %q", lb2.LocalAPICredential)
	}

	// It is the LocalAPI's own credential, and the proxy's is not.
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 15 * time.Second}
	for _, tc := range []struct {
		cred string
		want int
	}{{lb2.LocalAPICredential, http.StatusOK}, {lb2.ProxyCredential, http.StatusForbidden}} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+lb2.Address+"/localapi/v0/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Sec-Tailscale", "localapi")
		req.SetBasicAuth("", tc.cred)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != tc.want {
			t.Errorf("LocalAPI status with a credential = %d, want %d", res.StatusCode, tc.want)
		}
	}
}

// ── E2 ────────────────────────────────────────────────────────────────────

func addRoute(t *testing.T, s *shim, tap *eventTap, d egressAddData) egressAddedData {
	t.Helper()
	s.handleEgressAdd(mustJSON(t, d))
	ev := tap.reply(t, d.RequestID)
	if ev.Event != "egress:added" {
		t.Fatalf("egress:add %s answered %s %s", d.ID, ev.Event, ev.Data)
	}
	var added egressAddedData
	ev.decode(t, &added)
	if added.ID != d.ID || !strings.HasPrefix(added.Listen, "127.0.0.1:") {
		t.Fatalf("egress:added = %+v", added)
	}
	return added
}

// TestEgressE2RouteCarriesAWebSocketIntact is T6's regression on the way OUT:
// an upgrade through a route reaches the peer's listener with its escaped
// path and its query byte-for-byte, and the upgraded stream flows both ways.
func TestEgressE2RouteCarriesAWebSocketIntact(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	alpha := tn.node(t, ctx, "alpha")
	beta := tn.node(t, ctx, "beta")
	alphaFacts := factsOf(t, ctx, alpha)
	betaFacts := factsOf(t, ctx, beta)

	// The far door: records the request line it was sent, answers 101, then
	// echoes the upgraded stream.
	type seenRequest struct{ uri, host, remote string }
	seen := make(chan seenRequest, 4)
	ln, err := beta.Listen("tcp", ":8082")
	if err != nil {
		t.Fatal(err)
	}
	door := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- seenRequest{r.RequestURI, r.Host, r.RemoteAddr}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = rw.Flush()
		_, _ = io.Copy(conn, rw)
	}), ReadHeaderTimeout: 10 * time.Second}
	go door.Serve(ln)
	defer door.Close()

	s, tap := newEgressShim(t, alpha)
	added := addRoute(t, s, tap, egressAddData{
		ID: "cell", Target: net.JoinHostPort(betaFacts.dnsName, "8082"), RequestID: "add-cell",
	})

	c, err := net.DialTimeout("tcp", added.Listen, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))

	const requestURI = "/cell/b00t/control%2Fpane?client=2&next=%2Fhome%3Fq"
	upgrade := "GET " + requestURI + " HTTP/1.1\r\n" +
		"Host: " + added.Listen + "\r\n" +
		"Connection: Upgrade\r\nUpgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
	if _, err := io.WriteString(c, upgrade); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	res, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read the upgrade answer: %v", err)
	}
	if res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", res.StatusCode)
	}
	select {
	case got := <-seen:
		if got.uri != requestURI {
			t.Errorf("the far door saw %q, want %q", got.uri, requestURI)
		}
		// A raw splice rewrites nothing: the far door sees the Host the
		// client wrote (RFC 026 §4.2).
		if got.host != added.Listen {
			t.Errorf("the far door saw Host %q, want the client's own %q", got.host, added.Listen)
		}
		expectFrom(t, ctx, beta, got.remote, alphaFacts, "E2 route")
	case <-time.After(10 * time.Second):
		t.Fatal("the far door never saw the upgrade")
	}
	echoRoundTrip(t, br, c, "a frame after the upgrade")
}

// TestEgressE2AdmissionRefusesNonPeers: egress:add binds only a target that
// names a node of this netmap whose owner the gates admit; every refusal
// says why and binds nothing.
func TestEgressE2AdmissionRefusesNonPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	alpha := tn.node(t, ctx, "alpha")
	beta := tn.node(t, ctx, "beta")
	alphaFacts := factsOf(t, ctx, alpha)
	betaFacts := factsOf(t, ctx, beta)
	betaDoor := net.JoinHostPort(betaFacts.dnsName, "8082")

	s, tap := newEgressShim(t, alpha)
	for i, tc := range []struct {
		name      string
		target    string
		login     string
		nodeAllow []string
		wantCode  string
	}{
		{"a documentation-range address", "192.0.2.1:80", "", nil, egressNotPeer},
		{"a public name", "example.com:443", "", nil, egressNotPeer},
		{"the quad-100 service address", "100.100.100.100:53", "", nil, egressNotPeer},
		{"a tailnet-range address no node holds", "100.64.99.99:80", "", nil, egressNotPeer},
		{"a .ts.net name outside this netmap", "beta.tail-other.ts.net:443", "", nil, egressNotPeer},
		{"the node's gate refuses beta's owner", betaDoor, "", []string{alphaFacts.login}, egressLoginRefused},
		{"the route's glob refuses beta's owner", betaDoor, "nobody@*", nil, egressLoginRefused},
		{"a malformed route glob", betaDoor, "[", nil, egressInvalidLogin},
		{"no port", betaFacts.dnsName, "", nil, egressInvalidTarget},
	} {
		requestID := fmt.Sprintf("refused-%d", i)
		s.handleEgressAdd(mustJSON(t, egressAddData{
			ID: fmt.Sprintf("r%d", i), Target: tc.target, Login: tc.login, NodeLoginAllow: tc.nodeAllow, RequestID: requestID,
		}))
		ev := tap.reply(t, requestID)
		var refusal egressErrorData
		ev.decode(t, &refusal)
		if ev.Event != "egress:error" || refusal.Code != tc.wantCode || refusal.Message == "" {
			t.Errorf("%s: egress:add answered %s %s, want egress:error %s with a reason", tc.name, ev.Event, ev.Data, tc.wantCode)
		}
	}

	s.handleEgressList(json.RawMessage(`{"requestId":"ls-empty"}`))
	var list egressListResultData
	tap.reply(t, "ls-empty").decode(t, &list)
	if len(list.Routes) != 0 {
		t.Fatalf("refused adds left routes behind: %+v", list.Routes)
	}

	// The gates admit what they should: beta's owner, by the node's gate and
	// by the route's own glob together.
	added := addRoute(t, s, tap, egressAddData{
		ID: "bob", Target: betaDoor, Login: betaFacts.login, NodeLoginAllow: []string{betaFacts.login}, RequestID: "add-bob",
	})
	s.handleEgressAdd(mustJSON(t, egressAddData{ID: "bob", Target: betaDoor, RequestID: "add-bob-again"}))
	var dup egressErrorData
	tap.reply(t, "add-bob-again").decode(t, &dup)
	if dup.Code != "EGRESS_EXISTS" {
		t.Errorf("a duplicate id: %+v, want EGRESS_EXISTS", dup)
	}

	s.handleEgressList(json.RawMessage(`{"requestId":"ls-bob"}`))
	tap.reply(t, "ls-bob").decode(t, &list)
	want := egressRouteInfo{ID: "bob", Target: betaDoor, Listen: added.Listen, Login: betaFacts.login}
	if len(list.Routes) != 1 || list.Routes[0] != want {
		t.Errorf("egress:list = %+v, want [%+v]", list.Routes, want)
	}
}

// TestEgressE2RemoveClosesLiveConnections: egress:remove cuts a live splice
// and the listener with it.
func TestEgressE2RemoveClosesLiveConnections(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	alpha := tn.node(t, ctx, "alpha")
	beta := tn.node(t, ctx, "beta")
	betaFacts := factsOf(t, ctx, beta)

	ln, err := beta.Listen("tcp", ":8083")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go serveEcho(ln)

	s, tap := newEgressShim(t, alpha)
	added := addRoute(t, s, tap, egressAddData{
		ID: "echo", Target: net.JoinHostPort(betaFacts.ip.String(), "8083"), RequestID: "add-echo",
	})
	c, err := net.DialTimeout("tcp", added.Listen, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	echoRoundTrip(t, c, c, "before the remove")

	s.handleEgressRemove(json.RawMessage(`{"id":"echo","requestId":"rm-echo"}`))
	if ev := tap.reply(t, "rm-echo"); ev.Event != "egress:removed" {
		t.Fatalf("egress:remove answered %s %s", ev.Event, ev.Data)
	}
	expectClosed(t, c, "the live splice after egress:remove")

	if late, err := net.DialTimeout("tcp", added.Listen, 2*time.Second); err == nil {
		expectClosed(t, late, "a connection to a removed route")
		late.Close()
	}
	s.handleEgressRemove(json.RawMessage(`{"id":"echo","requestId":"rm-again"}`))
	var gone egressErrorData
	tap.reply(t, "rm-again").decode(t, &gone)
	if gone.Code != "NOT_FOUND" {
		t.Errorf("removing a removed route: %+v, want NOT_FOUND", gone)
	}
}

// TestEgressE2RechecksAdmissionPerConnection: a route admits every accepted
// connection afresh. When the target's owner changes under a route that
// names the old owner, the next connection is refused (and reported); when
// it changes back, the next one is admitted again.
func TestEgressE2RechecksAdmissionPerConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	alpha := tn.node(t, ctx, "alpha")
	beta := tn.node(t, ctx, "beta")
	alphaFacts := factsOf(t, ctx, alpha)
	betaFacts := factsOf(t, ctx, beta)

	ln, err := beta.Listen("tcp", ":8084")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go serveEcho(ln)

	s, tap := newEgressShim(t, alpha)
	added := addRoute(t, s, tap, egressAddData{
		ID: "beta-owner", Target: net.JoinHostPort(betaFacts.dnsName, "8084"), Login: betaFacts.login, RequestID: "add-owner",
	})
	dialRoute := func() net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", added.Listen, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	first := dialRoute()
	echoRoundTrip(t, first, first, "while beta is its first owner's")

	// The control plane hands beta to alpha's owner.
	setOwner := func(user tailcfg.UserID, wantLogin string) {
		t.Helper()
		n := tn.control.Node(betaFacts.nodeKey)
		if n == nil {
			t.Fatal("control does not know beta")
		}
		n.User = user
		tn.control.UpdateNode(n)
		for peerLoginAt(ctx, alpha, betaFacts.ip) != wantLogin {
			select {
			case <-ctx.Done():
				t.Fatalf("alpha's netmap never showed beta owned by %s", wantLogin)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	setOwner(alphaFacts.userID, alphaFacts.login)

	refused := dialRoute()
	expectClosed(t, refused, "a connection after beta's owner changed")
	ev := tap.find(t, "the runtime refusal", func(ev tapEvent) bool {
		var e egressErrorData
		return ev.Event == "egress:error" && json.Unmarshal(ev.Data, &e) == nil &&
			e.ID == "beta-owner" && e.Code == egressLoginRefused && e.RequestID == ""
	})
	t.Logf("runtime refusal: %s", ev.Data)

	// The route stands; admission is per connection, not latched.
	setOwner(betaFacts.userID, betaFacts.login)
	again := dialRoute()
	echoRoundTrip(t, again, again, "after beta's owner changed back")
}

// ── E3 ────────────────────────────────────────────────────────────────────

// connectThrough writes a CONNECT (and any pipelined bytes) to the proxy and
// reads the answer, returning the reader to continue the tunnel on.
func connectThrough(t *testing.T, proxyAddr, authority, proxyAuth, pipelined string) (*http.Response, net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	req := "CONNECT " + authority + " HTTP/1.1\r\nHost: " + authority + "\r\n"
	if proxyAuth != "" {
		req += "Proxy-Authorization: " + proxyAuth + "\r\n"
	}
	req += "\r\n" + pipelined
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	res, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("CONNECT %s: read the answer: %v", authority, err)
	}
	return res, c, br
}

// TestEgressE3ConnectProxy: egress:proxy answers a loopback CONNECT proxy
// that tunnels to an admitted peer with the credential, challenges without
// it, refuses a public or foreign host, and forwards nothing but CONNECT.
func TestEgressE3ConnectProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	alpha := tn.node(t, ctx, "alpha")
	beta := tn.node(t, ctx, "beta")
	alphaFacts := factsOf(t, ctx, alpha)
	betaFacts := factsOf(t, ctx, beta)

	// beta's :443 — a plain echo stands in for a TLS server: a tunnel carries
	// opaque bytes, and TLS is the client's business end to end.
	ln, err := beta.Listen("tcp", ":443")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	seen := echoRecording(ln)

	s, tap := newEgressShim(t, alpha)
	s.handleEgressProxy(json.RawMessage(`{"requestId":"px-1"}`))
	ev := tap.reply(t, "px-1")
	if ev.Event != "egress:proxyResult" {
		t.Fatalf("egress:proxy answered %s %s", ev.Event, ev.Data)
	}
	var px egressProxyResultData
	ev.decode(t, &px)
	if !strings.HasPrefix(px.Address, "127.0.0.1:") || len(px.Credential) != 32 {
		t.Fatalf("egress:proxy = %+v", px)
	}
	s.handleEgressProxy(json.RawMessage(`{"requestId":"px-2"}`))
	var px2 egressProxyResultData
	tap.reply(t, "px-2").decode(t, &px2)
	if px2.Address != px.Address || px2.Credential != px.Credential {
		t.Errorf("a second egress:proxy = %+v, want the first", px2)
	}
	auth := basic("truffle", px.Credential)
	peer443 := net.JoinHostPort(betaFacts.dnsName, "443")

	// With the credential: a tunnel to <peer>.ts.net:443. Bytes pipelined
	// behind the CONNECT (as a TLS ClientHello would be) arrive first.
	res, c, br := connectThrough(t, px.Address, peer443, auth, "pipelined-hello")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT %s with the credential = %d", peer443, res.StatusCode)
	}
	got := make([]byte, len("pipelined-hello"))
	if _, err := io.ReadFull(br, got); err != nil || string(got) != "pipelined-hello" {
		t.Fatalf("pipelined bytes came back as %q, %v", got, err)
	}
	echoRoundTrip(t, br, c, "through the tunnel")
	expectFrom(t, ctx, beta, nextRemote(t, seen, "E3"), alphaFacts, "E3 CONNECT")

	byIP := net.JoinHostPort(betaFacts.ip.String(), "443")
	res, c, br = connectThrough(t, px.Address, byIP, auth, "")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT %s with the credential = %d", byIP, res.StatusCode)
	}
	echoRoundTrip(t, br, c, "by tailnet IP")

	// Without the credential, or with a wrong one: 407 and a Basic challenge
	// (the one Chromium answers through Electron's login event).
	for _, header := range []string{"", basic("truffle", "0123456789abcdef0123456789abcdef")} {
		res, _, _ := connectThrough(t, px.Address, peer443, header, "")
		if res.StatusCode != http.StatusProxyAuthRequired || !strings.HasPrefix(res.Header.Get("Proxy-Authenticate"), "Basic") {
			t.Errorf("CONNECT with Proxy-Authorization %q = %d %q, want 407 with a Basic challenge",
				header, res.StatusCode, res.Header.Get("Proxy-Authenticate"))
		}
	}

	// Authenticated, but not a node of this tailnet: 403.
	for _, authority := range []string{"example.com:443", "192.0.2.1:443", "100.64.99.99:443", "beta.tail-other.ts.net:443", "100.100.100.100:80"} {
		if res, _, _ := connectThrough(t, px.Address, authority, auth, ""); res.StatusCode != http.StatusForbidden {
			t.Errorf("CONNECT %s = %d, want 403", authority, res.StatusCode)
		}
	}

	// CONNECT only: an authenticated absolute-form GET is not forwarded.
	plain, err := net.DialTimeout("tcp", px.Address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	_ = plain.SetDeadline(time.Now().Add(15 * time.Second))
	fmt.Fprintf(plain, "GET http://%s/ HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n", peer443, peer443, auth)
	res, err = http.ReadResponse(bufio.NewReader(plain), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("an absolute-form GET = %d, want 405", res.StatusCode)
	}

	// A gated node's proxy (the gate is alpha's own login) refuses beta,
	// whose owner is someone else.
	gated, gatedTap := newEgressShim(t, alpha)
	gated.handleEgressProxy(mustJSON(t, egressProxyData{NodeLoginAllow: []string{alphaFacts.login}, RequestID: "px-gated"}))
	var gpx egressProxyResultData
	gatedTap.reply(t, "px-gated").decode(t, &gpx)
	if res, _, _ := connectThrough(t, gpx.Address, peer443, basic("x", gpx.Credential), ""); res.StatusCode != http.StatusForbidden {
		t.Errorf("a gated proxy's CONNECT to a foreign-owned peer = %d, want 403", res.StatusCode)
	}
}

// A node's own address is admitted too: both E2 and E3 must stay within the
// same userspace stack when the destination is self, by IP or MagicDNS name.
func TestEgressSelfDial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tn := newTestTailnet(t)
	self := tn.node(t, ctx, "self")
	facts := factsOf(t, ctx, self)
	ln, err := self.Listen("tcp", ":8085")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go serveEcho(ln)
	s, tap := newEgressShim(t, self)
	s.handleEgressProxy(mustJSON(t, egressProxyData{NodeLoginAllow: []string{facts.login}, RequestID: "self-proxy"}))
	var proxy egressProxyResultData
	tap.reply(t, "self-proxy").decode(t, &proxy)
	for i, host := range []string{facts.ip.String(), facts.dnsName} {
		target := net.JoinHostPort(host, "8085")
		route := addRoute(t, s, tap, egressAddData{
			ID: fmt.Sprintf("self-%d", i), Target: target, NodeLoginAllow: []string{facts.login},
			RequestID: fmt.Sprintf("self-add-%d", i),
		})
		client, err := net.DialTimeout("tcp", route.Listen, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		echoRoundTrip(t, client, client, "E2 to self")
		client.Close()
		response, tunnel, reader := connectThrough(t, proxy.Address, target, basic("truffle", proxy.Credential), "")
		if response.StatusCode != http.StatusOK {
			t.Fatalf("CONNECT to self returned %d", response.StatusCode)
		}
		echoRoundTrip(t, reader, tunnel, "E3 to self")
		tunnel.Close()
	}
}
