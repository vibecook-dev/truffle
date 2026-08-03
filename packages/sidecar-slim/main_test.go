package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// testToken returns a deterministic 32-byte token matching Rust's test_token()
// in bridge/header.rs (bytes 0x00..0x1F).
func testToken() []byte {
	token := make([]byte, 32)
	for i := range token {
		token[i] = byte(i)
	}
	return token
}

// TestGoldenIncomingHeader verifies the binary header format matches
// the Rust truffle-core golden test vectors exactly.
func TestGoldenIncomingHeader(t *testing.T) {
	var buf bytes.Buffer
	token := testToken()

	err := writeHeader(&buf, token, dirIncoming, 443, "", "100.64.0.2:12345", "peer-host.tailnet.ts.net")
	if err != nil {
		t.Fatalf("writeHeader failed: %v", err)
	}

	b := buf.Bytes()

	// Magic
	if got := b[0:4]; !bytes.Equal(got, []byte{0x54, 0x52, 0x46, 0x46}) {
		t.Errorf("magic: got %s, want 54524646", hex.EncodeToString(got))
	}

	// Version
	if b[4] != 0x01 {
		t.Errorf("version: got %02x, want 01", b[4])
	}

	// Session token
	if !bytes.Equal(b[5:37], token) {
		t.Errorf("session token mismatch")
	}

	// Direction = incoming
	if b[37] != 0x01 {
		t.Errorf("direction: got %02x, want 01", b[37])
	}

	// Port = 443 (0x01BB)
	if !bytes.Equal(b[38:40], []byte{0x01, 0xBB}) {
		t.Errorf("port: got %s, want 01bb", hex.EncodeToString(b[38:40]))
	}

	// RequestIdLen = 0
	if !bytes.Equal(b[40:42], []byte{0x00, 0x00}) {
		t.Errorf("request_id_len: got %s, want 0000", hex.EncodeToString(b[40:42]))
	}

	// RemoteAddrLen = 16 ("100.64.0.2:12345")
	if !bytes.Equal(b[42:44], []byte{0x00, 0x10}) {
		t.Errorf("remote_addr_len: got %s, want 0010", hex.EncodeToString(b[42:44]))
	}
	if got := string(b[44:60]); got != "100.64.0.2:12345" {
		t.Errorf("remote_addr: got %q, want %q", got, "100.64.0.2:12345")
	}

	// RemoteDNSNameLen = 24 ("peer-host.tailnet.ts.net")
	if !bytes.Equal(b[60:62], []byte{0x00, 0x18}) {
		t.Errorf("remote_dns_name_len: got %s, want 0018", hex.EncodeToString(b[60:62]))
	}
	if got := string(b[62:86]); got != "peer-host.tailnet.ts.net" {
		t.Errorf("remote_dns_name: got %q, want %q", got, "peer-host.tailnet.ts.net")
	}

	// Total length
	if len(b) != 86 {
		t.Errorf("total length: got %d, want 86", len(b))
	}
}

// TestGoldenOutgoingHeader verifies outgoing header with requestId.
func TestGoldenOutgoingHeader(t *testing.T) {
	var buf bytes.Buffer
	token := testToken()
	requestID := "550e8400-e29b-41d4-a716-446655440000"

	err := writeHeader(&buf, token, dirOutgoing, 9417, requestID, "100.64.0.3:9417", "other-host.tailnet.ts.net")
	if err != nil {
		t.Fatalf("writeHeader failed: %v", err)
	}

	b := buf.Bytes()

	// Direction = outgoing
	if b[37] != 0x02 {
		t.Errorf("direction: got %02x, want 02", b[37])
	}

	// Port = 9417 (0x24C9)
	if !bytes.Equal(b[38:40], []byte{0x24, 0xC9}) {
		t.Errorf("port: got %s, want 24c9", hex.EncodeToString(b[38:40]))
	}

	// RequestIdLen = 36
	if !bytes.Equal(b[40:42], []byte{0x00, 0x24}) {
		t.Errorf("request_id_len: got %s, want 0024", hex.EncodeToString(b[40:42]))
	}

	// RequestId
	reqEnd := 42 + 36
	if got := string(b[42:reqEnd]); got != requestID {
		t.Errorf("request_id: got %q, want %q", got, requestID)
	}

	// Read it back with Rust-compatible offsets
	// After requestId, we have RemoteAddrLen(2) + RemoteAddr + RemoteDNSNameLen(2) + RemoteDNSName
	addrLenOff := reqEnd
	addrLen := int(b[addrLenOff])<<8 | int(b[addrLenOff+1])
	if addrLen != 15 { // "100.64.0.3:9417"
		t.Errorf("remote_addr_len: got %d, want 15", addrLen)
	}
}

// TestGoldenMinimalHeader verifies a header with all empty variable fields.
func TestGoldenMinimalHeader(t *testing.T) {
	var buf bytes.Buffer
	token := make([]byte, 32)
	for i := range token {
		token[i] = 0xAA
	}

	err := writeHeader(&buf, token, dirIncoming, 443, "", "", "")
	if err != nil {
		t.Fatalf("writeHeader failed: %v", err)
	}

	b := buf.Bytes()

	// Minimum header size: 4+1+32+1+2+2+2+2 = 46
	if len(b) != 46 {
		t.Errorf("minimal header length: got %d, want 46", len(b))
	}

	// All length fields should be 0
	if !bytes.Equal(b[40:42], []byte{0, 0}) {
		t.Errorf("request_id_len not 0")
	}
	if !bytes.Equal(b[42:44], []byte{0, 0}) {
		t.Errorf("remote_addr_len not 0")
	}
	if !bytes.Equal(b[44:46], []byte{0, 0}) {
		t.Errorf("remote_dns_name_len not 0")
	}
}

// newTestShim mirrors main()'s shim construction for unit tests: events are
// discarded, the per-subsystem maps are initialized, and the lifecycle context
// is armed from a fresh background context.
func newTestShim() *shim {
	ctx, cancel := context.WithCancel(context.Background())
	return &shim{
		writer:           json.NewEncoder(io.Discard),
		dynamicListeners: make(map[uint16]net.Listener),
		udpRelays:        make(map[uint16]*udpRelay),
		proxies:          make(map[string]*proxyEntry),
		pendingProxyAdds: make(map[string]*pendingProxyAdd),
		ctx:              ctx,
		cancel:           cancel,
	}
}

type trackingStaticRoot struct {
	closeCalls atomic.Int32
	closeOnce  sync.Once
	closed     chan struct{}
	onClose    func()
}

func newTrackingStaticRoot() *trackingStaticRoot {
	return &trackingStaticRoot{closed: make(chan struct{})}
}

func (r *trackingStaticRoot) Open(string) (*os.File, error) {
	return nil, fs.ErrNotExist
}

func (r *trackingStaticRoot) Close() error {
	r.closeCalls.Add(1)
	r.closeOnce.Do(func() {
		if r.onClose != nil {
			r.onClose()
		}
		close(r.closed)
	})
	return nil
}

type testListenerAddr string

func (a testListenerAddr) Network() string { return "test" }
func (a testListenerAddr) String() string  { return string(a) }

type trackingListener struct {
	closeCalls atomic.Int32
	closeOnce  sync.Once
	closed     chan struct{}
}

func newTrackingListener() *trackingListener {
	return &trackingListener{closed: make(chan struct{})}
}

func (l *trackingListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *trackingListener) Close() error {
	l.closeCalls.Add(1)
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *trackingListener) Addr() net.Addr { return testListenerAddr("listener") }

func waitForSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitForProxyEntry(t *testing.T, s *shim, id string) *proxyEntry {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.proxyMu.Lock()
		entry := s.proxies[id]
		s.proxyMu.Unlock()
		if entry != nil {
			return entry
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for proxy %q", id)
	return nil
}

func staticProxyAddData(id string, port uint16, dir string) proxyAddData {
	tlsOff := false
	return proxyAddData{
		ID: id, Name: id, ListenPort: port,
		TargetHost: "localhost", TargetScheme: "http", Tls: &tlsOff,
		Routes: []proxyRouteData{{Prefix: "/", Dir: dir}},
	}
}

func setStaticRootOpener(t *testing.T, opener func(string) (staticRoot, error)) {
	t.Helper()
	previous := openStaticRoot
	openStaticRoot = opener
	t.Cleanup(func() { openStaticRoot = previous })
}

// TestMonitorStateExitsAcrossRestart is the goroutine-leak regression: a
// long-lived goroutine spawned in one lifecycle must terminate on stop and stay
// terminated across a restart, rather than being kept alive by the restart's
// re-armed context. It captures the lifecycle ctx pre-stop (as monitorState
// does at spawn), stops (cancelling that ctx), then restarts (re-arming a fresh
// ctx). monitorState run with the captured ctx must observe cancellation and
// exit. Before the fix — where monitorState re-read s.ctx — the equivalent run
// would observe the re-armed live context and block on its 60s ticker, timing
// out here.
func TestMonitorStateExitsAcrossRestart(t *testing.T) {
	s := newTestShim()

	// The lifecycle ctx a monitorState goroutine captures at spawn.
	ctx := s.lifecycleCtx()

	// Stop cancels that lifecycle context.
	s.handleStop()

	// A restart re-arms a fresh lifecycle context.
	s.armLifecycle(testToken(), 9999)

	// lc is nil-safe here: monitorState returns via ctx.Done before its first
	// 60s tick would touch lc.
	done := make(chan struct{})
	go func() {
		s.monitorState(ctx, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("monitorState from previous lifecycle leaked across restart")
	}
}

// TestRestartLifecycleFieldsSynchronized is the data-race regression: the
// ctx/cancel/sessionToken/bridgePort fields are read by spawned goroutines
// (lifecycleCtx/bridgeParams) while the dispatch loop rewrites them on
// stop/start (handleStop/armLifecycle). All access must go through serverMu.
// Under `go test -race`, the pre-fix raw field reads/writes report a data race;
// after the fix this is clean.
func TestRestartLifecycleFieldsSynchronized(t *testing.T) {
	s := newTestShim()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = s.lifecycleCtx().Err()
			tok, port := s.bridgeParams()
			_ = tok
			_ = port
		}
	}()

	for i := 0; i < 1000; i++ {
		s.handleStop()
		_ = s.armLifecycle(testToken(), 9000)
	}
	wg.Wait()
}

// TestWriteHeaderRejectsOversizeDNS is the RemoteDNSName-cap regression: a DNS
// field one byte over maxRemoteDNSNameLen must be refused, so the sidecar never
// emits a header the paired Rust core is guaranteed to reject. Before the cap
// guard, only the 0xFFFF framing guard applied, so a 4097-byte field wrote
// successfully.
func TestWriteHeaderRejectsOversizeDNS(t *testing.T) {
	var buf bytes.Buffer
	err := writeHeader(&buf, testToken(), dirIncoming, 443, "", "100.64.0.2:1", strings.Repeat("a", maxRemoteDNSNameLen+1))
	if err == nil {
		t.Fatal("writeHeader accepted an oversize RemoteDNSName; want error")
	}
}

// TestWriteHeaderAcceptsDNSAtCap verifies the inclusive boundary: a DNS field of
// exactly maxRemoteDNSNameLen bytes is accepted (Rust caps inclusively too).
func TestWriteHeaderAcceptsDNSAtCap(t *testing.T) {
	var buf bytes.Buffer
	err := writeHeader(&buf, testToken(), dirIncoming, 443, "", "100.64.0.2:1", strings.Repeat("a", maxRemoteDNSNameLen))
	if err != nil {
		t.Fatalf("writeHeader rejected a DNS field at the cap: %v", err)
	}
}

// TestMarshalPeerIdentityDropsOversizeOptionalFields verifies graceful
// degradation: an identity whose profilePicUrl blows past the cap is still
// emitted, with only the offending optional field dropped — dnsName, nodeId, and
// loginName survive so identity verification/display still work.
func TestMarshalPeerIdentityDropsOversizeOptionalFields(t *testing.T) {
	identity := peerIdentityData{
		DNSName:       "peer.tailnet.ts.net",
		NodeID:        "nABC123",
		LoginName:     "alice@example.com",
		DisplayName:   "Alice",
		ProfilePicURL: strings.Repeat("x", 8192),
	}
	out := marshalPeerIdentity(identity)
	if len(out) > maxRemoteDNSNameLen {
		t.Fatalf("marshalPeerIdentity output %d exceeds cap %d", len(out), maxRemoteDNSNameLen)
	}

	var got peerIdentityData
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("marshalPeerIdentity produced invalid JSON: %v", err)
	}
	if got.ProfilePicURL != "" {
		t.Errorf("profilePicUrl: got %q, want empty (should have been dropped)", got.ProfilePicURL)
	}
	if got.NodeID != "nABC123" {
		t.Errorf("nodeId: got %q, want nABC123", got.NodeID)
	}
	if got.DNSName != "peer.tailnet.ts.net" {
		t.Errorf("dnsName: got %q, want peer.tailnet.ts.net", got.DNSName)
	}
	if got.LoginName != "alice@example.com" {
		t.Errorf("loginName: got %q, want alice@example.com", got.LoginName)
	}
}

// TestMarshalPeerIdentitySmallIdentityUnchanged verifies the common case: an
// identity that fits within the cap is emitted verbatim with every field intact.
func TestMarshalPeerIdentitySmallIdentityUnchanged(t *testing.T) {
	identity := peerIdentityData{
		DNSName:       "peer.tailnet.ts.net",
		NodeID:        "nABC123",
		LoginName:     "alice@example.com",
		DisplayName:   "Alice",
		ProfilePicURL: "https://example.com/pic.png",
	}
	out := marshalPeerIdentity(identity)

	var got peerIdentityData
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("marshalPeerIdentity produced invalid JSON: %v", err)
	}
	if got != identity {
		t.Errorf("identity round-trip mismatch: got %+v, want %+v", got, identity)
	}
}

// TestShouldWrapTLS covers the dial TLS-wrap decision: an explicit tls flag
// wins in both directions, and when absent (nil) the dial is never wrapped
// (RFC 023 D4 retired the legacy wrap-iff-port==443 rule).
func TestShouldWrapTLS(t *testing.T) {
	tru, fals := true, false
	cases := []struct {
		name string
		port uint16
		tls  *bool
		want bool
	}{
		{"nil never wraps on 443", 443, nil, false},
		{"nil never wraps off 443", 8080, nil, false},
		{"explicit true wraps non-443", 8080, &tru, true},
		{"explicit false skips 443", 443, &fals, false},
		{"explicit true on 443", 443, &tru, true},
		{"explicit false on non-443", 9000, &fals, false},
	}
	for _, tc := range cases {
		if got := shouldWrapTLS(tc.port, tc.tls); got != tc.want {
			t.Errorf("%s: shouldWrapTLS(%d, %v) = %v, want %v", tc.name, tc.port, tc.tls, got, tc.want)
		}
	}
}

// TestStatusEventAdvertisesProtocolVersion verifies the status payload carries
// protocolVersion so the core gates capabilities by sidecar version instead of
// treating this sidecar as v1. The core reads it off the "running" status/started
// event (protocol.rs StatusEventData.protocol_version, camelCase, integer).
// The pin is deliberate: v4 = requestId echoed on every RPC-style terminal
// event; bumping the constant means updating this test AND the version gates
// in provider.rs.
func TestStatusEventAdvertisesProtocolVersion(t *testing.T) {
	if sidecarProtocolVersion != 4 {
		t.Fatalf("sidecarProtocolVersion = %d, want 4 (requestId echo on RPC events)", sidecarProtocolVersion)
	}

	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}
	s.sendStatus("running", "host", "host.tail.ts.net", "100.64.0.1", "")

	// The core matches the camelCase key exactly, as a JSON integer.
	if !bytes.Contains(buf.Bytes(), []byte(`"protocolVersion":4`)) {
		t.Errorf("status payload missing protocolVersion on the wire: %s", strings.TrimSpace(buf.String()))
	}

	var ev struct {
		Event string     `json:"event"`
		Data  statusData `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatalf("status event JSON parse: %v", err)
	}
	if ev.Data.ProtocolVersion != sidecarProtocolVersion {
		t.Errorf("status protocolVersion = %d, want %d", ev.Data.ProtocolVersion, sidecarProtocolVersion)
	}
}

// TestWhoisResultWireShape pins the tsnet:whoisResult payload: identity rides
// as a nested camelCase object (protocol.rs WhoisResultEventData deserializes
// it straight into TailscalePeerIdentity), and the anonymous case omits the
// identity key entirely — absent, not an empty object.
func TestWhoisResultWireShape(t *testing.T) {
	id := peerIdentityData{
		DNSName:   "kitchen.tail1234.ts.net",
		LoginName: "alice@corp.com",
		NodeID:    "nQRJl4CNTRL",
	}
	b, err := json.Marshal(whoisResultData{Addr: "100.64.0.7", Identity: &id, RequestID: "r1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"addr":"100.64.0.7"`,
		`"identity":{`,
		`"dnsName":"kitchen.tail1234.ts.net"`,
		`"loginName":"alice@corp.com"`,
		`"nodeId":"nQRJl4CNTRL"`,
		`"requestId":"r1"`,
	} {
		if !bytes.Contains(b, []byte(want)) {
			t.Errorf("whoisResult payload missing %s: %s", want, b)
		}
	}

	anon, err := json.Marshal(whoisResultData{Addr: "100.64.0.8"})
	if err != nil {
		t.Fatalf("marshal anonymous: %v", err)
	}
	if bytes.Contains(anon, []byte(`"identity"`)) {
		t.Errorf("anonymous whoisResult must omit identity: %s", anon)
	}
	if bytes.Contains(anon, []byte(`"error"`)) {
		t.Errorf("anonymous whoisResult must omit error: %s", anon)
	}
}

// TestRPCEventsEchoRequestID pins the v4 correlation contract on every RPC-style
// terminal event: a set id marshals as the camelCase "requestId" key the core's
// reply broker routes on, and an unset one drops the key entirely so a pre-v4
// core sees the exact bytes it saw before.
func TestRPCEventsEchoRequestID(t *testing.T) {
	cases := []struct {
		name   string
		withID any
		noID   any
	}{
		{"listening", listeningData{Port: 8080, RequestID: "r1"}, listeningData{Port: 8080}},
		{"unlistened", unlistenedData{Port: 8080, RequestID: "r1"}, unlistenedData{Port: 8080}},
		{"listeningPacket", listeningPacketData{Port: 9, LocalPort: 41234, RequestID: "r1"}, listeningPacketData{Port: 9, LocalPort: 41234}},
		// listenPacketData doubles as the unlistenedPacket event payload.
		{"unlistenedPacket", listenPacketData{Port: 9, RequestID: "r1"}, listenPacketData{Port: 9}},
		{"proxyAdded", proxyAddedEventData{ID: "p1", ListenPort: 443, URL: "https://h/", RequestID: "r1"}, proxyAddedEventData{ID: "p1", ListenPort: 443, URL: "https://h/"}},
		{"proxyRemoved", proxyRemovedEventData{ID: "p1", RequestID: "r1"}, proxyRemovedEventData{ID: "p1"}},
		{"proxyError", proxyErrorEventData{ID: "p1", Code: "NOT_FOUND", Message: "nope", RequestID: "r1"}, proxyErrorEventData{ID: "p1", Code: "NOT_FOUND", Message: "nope"}},
		{"proxyList", proxyListEventData{RequestID: "r1"}, proxyListEventData{}},
		{"error", errorData{Code: "NOT_RUNNING", Message: "node not running", RequestID: "r1"}, errorData{Code: "NOT_RUNNING", Message: "node not running"}},
	}
	for _, tc := range cases {
		b, err := json.Marshal(tc.withID)
		if err != nil {
			t.Fatalf("%s: marshal: %v", tc.name, err)
		}
		if !bytes.Contains(b, []byte(`"requestId":"r1"`)) {
			t.Errorf("%s: payload missing requestId echo: %s", tc.name, b)
		}

		bare, err := json.Marshal(tc.noID)
		if err != nil {
			t.Fatalf("%s: marshal without id: %v", tc.name, err)
		}
		if bytes.Contains(bare, []byte(`"requestId"`)) {
			t.Errorf("%s: unset requestId must not appear on the wire: %s", tc.name, bare)
		}
	}
}

// TestSendErrorForRoutesTheFailure covers the error path the broker depends on:
// a failure that knows its command's id rides tsnet:error tagged with it, while
// sendError (parse failures, unknown commands) stays untagged.
func TestSendErrorForRoutesTheFailure(t *testing.T) {
	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}

	s.sendErrorFor("NOT_RUNNING", "node not running", "r1")
	if !bytes.Contains(buf.Bytes(), []byte(`"event":"tsnet:error"`)) {
		t.Errorf("sendErrorFor must emit tsnet:error: %s", strings.TrimSpace(buf.String()))
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"requestId":"r1"`)) {
		t.Errorf("sendErrorFor must tag the failure with its requestId: %s", strings.TrimSpace(buf.String()))
	}

	buf.Reset()
	s.sendError("PARSE_ERROR", "failed to parse command")
	if bytes.Contains(buf.Bytes(), []byte(`"requestId"`)) {
		t.Errorf("sendError must stay untagged: %s", strings.TrimSpace(buf.String()))
	}
}

// TestRequestIDOfTolerantParse covers the id extraction handleProxyAdd uses
// before unmarshalling its payload: a malformed or absent payload must yield ""
// rather than pre-empting the handler's own parse-failure report.
func TestRequestIDOfTolerantParse(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"id present", `{"id":"p1","requestId":"r1"}`, "r1"},
		{"id absent", `{"id":"p1"}`, ""},
		{"empty payload", ``, ""},
		{"malformed payload", `{"id":`, ""},
		{"wrong type", `{"requestId":42}`, ""},
	}
	for _, tc := range cases {
		if got := requestIDOf(json.RawMessage(tc.raw)); got != tc.want {
			t.Errorf("%s: requestIDOf(%q) = %q, want %q", tc.name, tc.raw, got, tc.want)
		}
	}
}

// TestProxyListEchoesOptionalRequestID exercises the one command whose payload
// is optional: pre-v4 cores send proxy:list with no data at all, and that must
// still answer with an untagged proxy:list event.
func TestProxyListEchoesOptionalRequestID(t *testing.T) {
	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}

	s.handleProxyList(nil)
	if !bytes.Contains(buf.Bytes(), []byte(`"event":"proxy:list"`)) {
		t.Fatalf("proxy:list without payload must still answer: %s", strings.TrimSpace(buf.String()))
	}
	if bytes.Contains(buf.Bytes(), []byte(`"requestId"`)) {
		t.Errorf("proxy:list without payload must not invent a requestId: %s", strings.TrimSpace(buf.String()))
	}

	buf.Reset()
	s.handleProxyList(json.RawMessage(`{"requestId":"r1"}`))
	if !bytes.Contains(buf.Bytes(), []byte(`"requestId":"r1"`)) {
		t.Errorf("proxy:list must echo the requestId it was given: %s", strings.TrimSpace(buf.String()))
	}
}

// TestResolveIdleTimeout covers the idle-reap knob resolution: nil or a
// non-positive value falls back to idleDeadline, a positive value is seconds.
func TestResolveIdleTimeout(t *testing.T) {
	secs := func(n int) *int { return &n }
	cases := []struct {
		name string
		in   *int
		want time.Duration
	}{
		{"nil uses default", nil, idleDeadline},
		{"zero uses default", secs(0), idleDeadline},
		{"negative uses default", secs(-5), idleDeadline},
		{"positive is seconds", secs(30), 30 * time.Second},
	}
	for _, tc := range cases {
		if got := resolveIdleTimeout(tc.in); got != tc.want {
			t.Errorf("%s: resolveIdleTimeout(%v) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
	}
}

// TestModulePath guards the go.mod module path against the stale
// claude-code-on-the-go name reappearing.
func TestModulePath(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build info embedded in test binary")
	}
	const want = "github.com/vibecook-dev/truffle/packages/sidecar-slim"
	if bi.Main.Path != want {
		t.Errorf("module path = %q, want %q", bi.Main.Path, want)
	}
}

// ── RFC 023 engine v2 seams ───────────────────────────────────────────────
//
// The tests below cover the unit-testable helpers of proxy engine v2 (commit
// 242ae22). The full handler assembly needs a live tsnet server (WhoIs
// identity, TLS listeners) and is exercised by the Rust integration suite;
// here we pin the pure functions and the two http.Handler builders that need
// nothing but httptest.

// equalStringSlices reports element-wise slice equality (nil == empty).
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAllowedLogin covers the loginName glob gate (RFC 023 §9.7 / D14): empty
// glob list means no gate, a caller without a login identity fails closed
// against any non-empty gate, matching is case-insensitive and shell-style,
// and a malformed glob is skipped rather than fatal.
func TestAllowedLogin(t *testing.T) {
	cases := []struct {
		name  string
		globs []string
		login string
		want  bool
	}{
		{"nil globs allow all", nil, "anyone@example.com", true},
		{"empty globs allow all", []string{}, "anyone@example.com", true},
		{"empty globs allow even empty login", nil, "", true},
		{"non-empty gate, empty login fails closed", []string{"*@corp.com"}, "", false},
		{"exact match", []string{"alice@corp.com"}, "alice@corp.com", true},
		{"exact non-match", []string{"alice@corp.com"}, "bob@corp.com", false},
		{"domain glob matches", []string{"*@corp.com"}, "alice@corp.com", true},
		{"domain glob rejects other domain", []string{"*@corp.com"}, "alice@evil.com", false},
		{"case-insensitive glob vs login", []string{"*@CORP.com"}, "Alice@corp.COM", true},
		{"case-insensitive exact", []string{"Alice@Corp.Com"}, "alice@corp.com", true},
		{"second glob in list matches", []string{"*@other.com", "*@corp.com"}, "bob@corp.com", true},
		{"no glob in list matches", []string{"*@other.com", "*@more.com"}, "bob@corp.com", false},
		// path.Match's '*' does not cross '/', so a login containing '/' is not
		// swallowed by a bare domain glob — documents the matcher's boundary.
		{"star does not cross slash", []string{"*@corp.com"}, "a/b@corp.com", false},
		// A malformed pattern (unterminated class) must not panic and must not
		// match; path.Match returns ErrBadPattern, which the gate treats as skip.
		{"invalid glob does not match", []string{"[unterminated"}, "alice@corp.com", false},
		{"invalid glob skipped, valid one still matches", []string{"[bad", "*@corp.com"}, "alice@corp.com", true},
	}
	for _, tc := range cases {
		if got := allowedLogin(tc.globs, tc.login); got != tc.want {
			t.Errorf("%s: allowedLogin(%q, %q) = %v, want %v", tc.name, tc.globs, tc.login, got, tc.want)
		}
	}
}

// TestEffectiveAllow verifies a matched route's per-route allow list overrides
// the config-level list, and an empty route list inherits the config list
// (RFC 023 §7 / D14).
func TestEffectiveAllow(t *testing.T) {
	route := []string{"ops@corp.com"}
	config := []string{"*@corp.com"}

	if got := effectiveAllow(route, config); !equalStringSlices(got, route) {
		t.Errorf("route list should override config: got %q, want %q", got, route)
	}
	if got := effectiveAllow(nil, config); !equalStringSlices(got, config) {
		t.Errorf("nil route list should inherit config: got %q, want %q", got, config)
	}
	if got := effectiveAllow([]string{}, config); !equalStringSlices(got, config) {
		t.Errorf("empty route list should inherit config: got %q, want %q", got, config)
	}
	// A config-less, route-less proxy has no gate at all.
	if got := effectiveAllow(nil, nil); len(got) != 0 {
		t.Errorf("no lists should yield empty gate: got %q", got)
	}
}

// TestPathMatchesPrefix covers the longest-prefix route mux boundary rules
// (RFC 023 §7 / D11): "/" matches everything, otherwise the match is exact or
// on a segment boundary so "/api" never captures "/apix".
func TestPathMatchesPrefix(t *testing.T) {
	cases := []struct {
		p, prefix string
		want      bool
	}{
		{"/anything/at/all", "/", true},
		{"/", "/", true},
		{"", "/", true},
		{"/api", "/api", true},
		{"/api/", "/api", true},
		{"/api/users", "/api", true},
		{"/apix", "/api", false},
		{"/ap", "/api", false},
		{"/", "/api", false},
		{"/api/v1/x", "/api/v1", true},
		{"/api/v2", "/api/v1", false},
		{"/api/v1", "/api/v1", true},
	}
	for _, tc := range cases {
		if got := pathMatchesPrefix(tc.p, tc.prefix); got != tc.want {
			t.Errorf("pathMatchesPrefix(%q, %q) = %v, want %v", tc.p, tc.prefix, got, tc.want)
		}
	}
}

// TestStripPathPrefix verifies mount-prefix stripping always leaves an
// absolute path (RFC 023 §7): "/api/x" → "/x", and the bare mount "/api" → "/"
// rather than the empty string.
func TestStripPathPrefix(t *testing.T) {
	cases := []struct {
		p, prefix, want string
	}{
		{"/api/x", "/api", "/x"},
		{"/api/users/1", "/api", "/users/1"},
		{"/api", "/api", "/"},
		{"/app/index.html", "/app", "/index.html"},
		{"/api/", "/api", "/"},
	}
	for _, tc := range cases {
		got := stripPathPrefix(tc.p, tc.prefix)
		if got != tc.want {
			t.Errorf("stripPathPrefix(%q, %q) = %q, want %q", tc.p, tc.prefix, got, tc.want)
		}
		if !strings.HasPrefix(got, "/") {
			t.Errorf("stripPathPrefix(%q, %q) = %q is not absolute", tc.p, tc.prefix, got)
		}
	}
}

// TestPublicURL verifies the tailnet URL renderer omits the scheme's default
// port (https 443, http 80) and keeps every other port explicit.
func TestPublicURL(t *testing.T) {
	cases := []struct {
		name    string
		tlsOn   bool
		dnsName string
		port    uint16
		want    string
	}{
		{"https default port omitted", true, "app.tail1234.ts.net", 443, "https://app.tail1234.ts.net"},
		{"http default port omitted", false, "app.tail1234.ts.net", 80, "http://app.tail1234.ts.net"},
		{"https non-default port shown", true, "app.tail1234.ts.net", 8443, "https://app.tail1234.ts.net:8443"},
		{"http non-default port shown", false, "app.tail1234.ts.net", 8080, "http://app.tail1234.ts.net:8080"},
		{"https on 80 keeps port", true, "app.tail1234.ts.net", 80, "https://app.tail1234.ts.net:80"},
		{"http on 443 keeps port", false, "app.tail1234.ts.net", 443, "http://app.tail1234.ts.net:443"},
	}
	for _, tc := range cases {
		if got := publicURL(tc.tlsOn, tc.dnsName, tc.port); got != tc.want {
			t.Errorf("%s: publicURL(%v, %q, %d) = %q, want %q", tc.name, tc.tlsOn, tc.dnsName, tc.port, got, tc.want)
		}
	}
}

// TestURLPort resolves a target URL's port, honoring an explicit port and
// otherwise defaulting by scheme (used to route WebSocket hijack dials).
func TestURLPort(t *testing.T) {
	cases := []struct {
		raw  string
		want uint16
	}{
		{"http://localhost:8000", 8000},
		{"https://localhost:8443", 8443},
		{"http://localhost", 80},
		{"https://localhost", 443},
		{"http://127.0.0.1:3000/api", 3000},
	}
	for _, tc := range cases {
		u, err := url.Parse(tc.raw)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", tc.raw, err)
		}
		if got := urlPort(u); got != tc.want {
			t.Errorf("urlPort(%q) = %d, want %d", tc.raw, got, tc.want)
		}
	}
}

// TestSanitizeTailscaleHeaders verifies the spoof defense (RFC 023 §9.2): any
// inbound header in the Tailscale-* namespace is stripped, while unrelated
// headers survive, so a client cannot smuggle a forged identity to the backend.
func TestSanitizeTailscaleHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Tailscale-User-Login", "attacker@evil.com")
	h.Set("Tailscale-User-Name", "Mallory")
	h.Set("Tailscale-User-Profile-Pic", "http://evil.example/pic.png")
	h.Set("Tailscale-Something-Else", "x") // whole namespace is stripped
	h.Set("X-Forwarded-For", "100.64.0.9")
	h.Set("Authorization", "Bearer keepme")
	h.Set("Content-Type", "application/json")

	sanitizeTailscaleHeaders(h)

	for _, k := range []string{
		"Tailscale-User-Login", "Tailscale-User-Name",
		"Tailscale-User-Profile-Pic", "Tailscale-Something-Else",
	} {
		if got := h.Get(k); got != "" {
			t.Errorf("%s not stripped: got %q", k, got)
		}
	}
	if got := h.Get("X-Forwarded-For"); got != "100.64.0.9" {
		t.Errorf("X-Forwarded-For dropped: got %q", got)
	}
	if got := h.Get("Authorization"); got != "Bearer keepme" {
		t.Errorf("Authorization dropped: got %q", got)
	}
	if got := h.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type dropped: got %q", got)
	}
}

// TestSanitizeTailscaleHeadersAnyCasing documents that however a client cases
// the header on the wire, net/http canonicalizes it before the sanitizer sees
// it, so every spelling lands in the same canonical slot and is stripped
// (RFC 023 §9.2 — "any casing").
func TestSanitizeTailscaleHeadersAnyCasing(t *testing.T) {
	for _, spelling := range []string{
		"tailscale-user-login",
		"TAILSCALE-USER-LOGIN",
		"TaIlScAlE-uSeR-lOgIn",
		"Tailscale-User-Login",
	} {
		h := http.Header{}
		h.Set(spelling, "attacker@evil.com")
		sanitizeTailscaleHeaders(h)
		if got := h.Get(hdrUserLogin); got != "" {
			t.Errorf("spelling %q not stripped: Tailscale-User-Login = %q", spelling, got)
		}
	}
}

// TestInjectIdentityHeaders verifies the verified WhoIs identity is written to
// the Tailscale-User-* headers the backend trusts (RFC 023 §7).
func TestInjectIdentityHeaders(t *testing.T) {
	h := http.Header{}
	injectIdentityHeaders(h, peerIdentityData{
		LoginName:     "alice@corp.com",
		DisplayName:   "Alice Example",
		ProfilePicURL: "https://corp.example/alice.png",
		NodeID:        "nQRJl4CNTRL",
		DNSName:       "alice-mbp.tail1234.ts.net",
	})
	if got := h.Get(hdrUserLogin); got != "alice@corp.com" {
		t.Errorf("%s = %q, want alice@corp.com", hdrUserLogin, got)
	}
	if got := h.Get(hdrUserName); got != "Alice Example" {
		t.Errorf("%s = %q, want Alice Example", hdrUserName, got)
	}
	if got := h.Get(hdrProfilePic); got != "https://corp.example/alice.png" {
		t.Errorf("%s = %q, want the profile pic URL", hdrProfilePic, got)
	}
	if got := h.Get(hdrNodeID); got != "nQRJl4CNTRL" {
		t.Errorf("%s = %q, want nQRJl4CNTRL", hdrNodeID, got)
	}
	if got := h.Get(hdrNodeName); got != "alice-mbp.tail1234.ts.net" {
		t.Errorf("%s = %q, want the MagicDNS name", hdrNodeName, got)
	}
}

// TestInjectIdentityHeadersTaggedNode is the caller-without-a-user-profile case:
// a tagged node has Node identity but no UserProfile, so only the node pair is
// injected. This is exactly what the node headers exist for — the login gate
// cannot see these callers at all.
func TestInjectIdentityHeadersTaggedNode(t *testing.T) {
	h := http.Header{}
	injectIdentityHeaders(h, peerIdentityData{
		NodeID:  "nTAGGEDNODE",
		DNSName: "ci-runner.tail1234.ts.net",
	})
	if got := h.Get(hdrNodeID); got != "nTAGGEDNODE" {
		t.Errorf("%s = %q, want nTAGGEDNODE", hdrNodeID, got)
	}
	if got := h.Get(hdrNodeName); got != "ci-runner.tail1234.ts.net" {
		t.Errorf("%s = %q, want the MagicDNS name", hdrNodeName, got)
	}
	for _, k := range []string{hdrUserLogin, hdrUserName, hdrProfilePic} {
		if _, ok := h[k]; ok {
			t.Errorf("%s present for tagged node: %q", k, h[k])
		}
	}
}

// TestSanitizeThenInjectOverridesSpoof exercises the real request-path order
// (RFC 023 §9.2): strip inbound copies, then inject verified values — a client
// that presents its own Tailscale-User-Login sees it replaced by the
// connection's true identity, never merged.
func TestSanitizeThenInjectOverridesSpoof(t *testing.T) {
	h := http.Header{}
	h.Set(hdrUserLogin, "attacker@evil.com")
	h.Set(hdrNodeID, "nFORGEDNODE")

	sanitizeTailscaleHeaders(h)
	injectIdentityHeaders(h, peerIdentityData{LoginName: "alice@corp.com", NodeID: "nREALNODE"})

	if got := h.Get(hdrUserLogin); got != "alice@corp.com" {
		t.Errorf("%s = %q, want alice@corp.com (spoof must be overridden)", hdrUserLogin, got)
	}
	if vals := h.Values(hdrUserLogin); len(vals) != 1 {
		t.Errorf("%s has %d values %q, want exactly 1 (no merge with spoof)", hdrUserLogin, len(vals), vals)
	}
	if got := h.Get(hdrNodeID); got != "nREALNODE" {
		t.Errorf("%s = %q, want nREALNODE (forged node id must be overridden)", hdrNodeID, got)
	}
	if vals := h.Values(hdrNodeID); len(vals) != 1 {
		t.Errorf("%s has %d values %q, want exactly 1 (no merge with forgery)", hdrNodeID, len(vals), vals)
	}
}

// TestInjectIdentityHeadersEmptyInjectsNothing is the anonymous-caller case
// (RFC 023 §9.6): when WhoIs yields no identity (tagged node, failed lookup,
// future Funnel), nothing is injected — and because the inbound copies were
// already stripped, the backend sees no Tailscale-User-* header to trust.
func TestInjectIdentityHeadersEmptyInjectsNothing(t *testing.T) {
	h := http.Header{}
	h.Set(hdrUserLogin, "attacker@evil.com") // client-supplied spoof

	sanitizeTailscaleHeaders(h)
	injectIdentityHeaders(h, peerIdentityData{}) // anonymous

	for _, k := range []string{hdrUserLogin, hdrUserName, hdrProfilePic, hdrNodeID, hdrNodeName} {
		if _, ok := h[k]; ok {
			t.Errorf("%s present for anonymous caller: %q", k, h[k])
		}
	}
}

// writeServeFile writes content to base/rel (slash-separated), creating parent
// dirs, for spaFileServer tests.
func writeServeFile(t *testing.T, base, rel, content string) {
	t.Helper()
	full := filepath.Join(base, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// serveRequest drives an http.Handler with httptest and returns the recorder.
func serveRequest(h http.Handler, method, target string) *httptest.ResponseRecorder {
	return serveRequestWithHeaders(h, method, target, nil)
}

func serveRequestWithHeaders(h http.Handler, method, target string, headers http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func openTestSPAFileServer(t *testing.T, dir, fallback string) http.Handler {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot(%q): %v", dir, err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return newSPAFileServer(root, fallback)
}

func makeServeSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		// Local Windows environments may not grant symlink privilege. CI is a
		// security gate and must never silently skip this coverage.
		if runtime.GOOS == "windows" && os.Getenv("CI") == "" {
			t.Skipf("symlinks unavailable on this Windows host: %v", err)
		}
		t.Fatalf("symlink %q -> %q: %v", link, target, err)
	}
}

// TestSPAFileServer covers the static handler's RFC 023 §6.2 hardening: files
// with correct body/mime, index.html at directory roots, NO directory listing,
// dotfiles denied even when present on disk, SPA fallback behavior, and
// method restriction.
func TestSPAFileServer(t *testing.T) {
	dir := t.TempDir()
	writeServeFile(t, dir, "style.css", "body{color:red}")
	writeServeFile(t, dir, "index.html", "ROOT_INDEX")
	writeServeFile(t, dir, "app/index.html", "APP_INDEX")
	writeServeFile(t, dir, "noindex/data.txt", "DIRDATA")
	writeServeFile(t, dir, ".env", "SECRET=topsecret")
	writeServeFile(t, dir, ".git/config", "[core]\n\trepositoryformatversion = 0")

	withFallback := openTestSPAFileServer(t, dir, "/index.html")
	noFallback := openTestSPAFileServer(t, dir, "")

	t.Run("serves file with body and mime", func(t *testing.T) {
		rec := serveRequest(noFallback, http.MethodGet, "/style.css")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); body != "body{color:red}" {
			t.Errorf("body = %q, want the css content", body)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/css") {
			t.Errorf("Content-Type = %q, want text/css", ct)
		}
	})

	t.Run("serves index.html at root", func(t *testing.T) {
		rec := serveRequest(noFallback, http.MethodGet, "/")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); body != "ROOT_INDEX" {
			t.Errorf("body = %q, want ROOT_INDEX", body)
		}
	})

	t.Run("serves index.html at subdirectory root", func(t *testing.T) {
		rec := serveRequest(noFallback, http.MethodGet, "/app")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); body != "APP_INDEX" {
			t.Errorf("body = %q, want APP_INDEX", body)
		}
	})

	t.Run("directory without index.html 404s with no listing", func(t *testing.T) {
		rec := serveRequest(noFallback, http.MethodGet, "/noindex")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		// A directory listing would leak the child file name into the body.
		if body := rec.Body.String(); strings.Contains(body, "data.txt") || strings.Contains(body, "DIRDATA") {
			t.Errorf("directory listing leaked: %q", body)
		}
	})

	t.Run("dotfiles denied even when present", func(t *testing.T) {
		for _, p := range []string{"/.env", "/.git/config"} {
			// Use the fallback handler to prove dotfiles 404 even with a SPA
			// fallback configured (the dotfile guard precedes the fallback).
			rec := serveRequest(withFallback, http.MethodGet, p)
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s: status = %d, want 404", p, rec.Code)
			}
			if body := rec.Body.String(); strings.Contains(body, "SECRET") || strings.Contains(body, "repositoryformatversion") {
				t.Errorf("%s: dotfile contents leaked: %q", p, body)
			}
		}
	})

	t.Run("SPA fallback serves fallback for a miss", func(t *testing.T) {
		rec := serveRequest(withFallback, http.MethodGet, "/does/not/exist")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); body != "ROOT_INDEX" {
			t.Errorf("body = %q, want ROOT_INDEX (fallback)", body)
		}
	})

	t.Run("no fallback 404s a miss", func(t *testing.T) {
		rec := serveRequest(noFallback, http.MethodGet, "/does/not/exist")
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})

	t.Run("HEAD is allowed", func(t *testing.T) {
		rec := serveRequest(noFallback, http.MethodHead, "/style.css")
		if rec.Code != http.StatusOK {
			t.Errorf("HEAD status = %d, want 200", rec.Code)
		}
	})

	t.Run("non-GET/HEAD is 405", func(t *testing.T) {
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			rec := serveRequest(noFallback, m, "/style.css")
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s: status = %d, want 405", m, rec.Code)
			}
		}
	})

	t.Run("range and conditional-date behavior remains compatible", func(t *testing.T) {
		rangeRec := serveRequestWithHeaders(noFallback, http.MethodGet, "/style.css", http.Header{
			"Range": {"bytes=5-9"},
		})
		if rangeRec.Code != http.StatusPartialContent {
			t.Fatalf("range status = %d, want 206", rangeRec.Code)
		}
		if got := rangeRec.Header().Get("Content-Range"); got != "bytes 5-9/15" {
			t.Errorf("Content-Range = %q, want %q", got, "bytes 5-9/15")
		}
		if body := rangeRec.Body.String(); body != "color" {
			t.Errorf("range body = %q, want %q", body, "color")
		}

		first := serveRequest(noFallback, http.MethodGet, "/style.css")
		lastModified := first.Header().Get("Last-Modified")
		if lastModified == "" {
			t.Fatal("Last-Modified is empty")
		}
		if etag := first.Header().Get("ETag"); etag != "" {
			t.Errorf("generated ETag = %q, want none", etag)
		}
		conditional := serveRequestWithHeaders(noFallback, http.MethodGet, "/style.css", http.Header{
			"If-Modified-Since": {lastModified},
		})
		if conditional.Code != http.StatusNotModified {
			t.Errorf("conditional status = %d, want 304", conditional.Code)
		}
	})
}

func TestClassifyStaticLookupError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want staticServeResult
	}{
		{name: "not exist", err: &os.PathError{Op: "openat", Path: "missing", Err: fs.ErrNotExist}, want: staticServeMiss},
		{name: "not a directory", err: &os.PathError{Op: "openat", Path: "file/child", Err: syscall.ENOTDIR}, want: staticServeMiss},
		{name: "permission", err: &os.PathError{Op: "openat", Path: "secret", Err: fs.ErrPermission}, want: staticServeDenied},
		{name: "root escape", err: errors.New("path escapes from parent"), want: staticServeDenied},
		{name: "arbitrary failure", err: errors.New("lookup failed"), want: staticServeDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyStaticLookupError(tt.err); got != tt.want {
				t.Errorf("classifyStaticLookupError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestSPAFileServerRootConfinement(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	writeServeFile(t, dir, "index.html", "ROOT_INDEX")
	writeServeFile(t, dir, "inside.txt", "INSIDE_FILE")
	writeServeFile(t, dir, "inside-dir/index.html", "INSIDE_INDEX")
	writeServeFile(t, dir, ".env", "IN_ROOT_DOTFILE")
	writeServeFile(t, outside, "outside.txt", "OUTSIDE_SECRET")
	writeServeFile(t, outside, "outside-dir/index.html", "OUTSIDE_INDEX")
	writeServeFile(t, outside, "outside-dir/child.txt", "OUTSIDE_CHILD")

	makeServeSymlink(t, filepath.Join(outside, "outside.txt"), filepath.Join(dir, "escape-file"))
	makeServeSymlink(t, filepath.Join(outside, "outside-dir"), filepath.Join(dir, "escape-dir"))
	makeServeSymlink(t, "inside.txt", filepath.Join(dir, "inside-file-link"))
	makeServeSymlink(t, "inside-dir", filepath.Join(dir, "inside-dir-link"))
	makeServeSymlink(t, ".env", filepath.Join(dir, "env-alias"))

	noFallback := openTestSPAFileServer(t, dir, "")
	withFallback := openTestSPAFileServer(t, dir, "/index.html")
	externalFallback := openTestSPAFileServer(t, dir, "/escape-file")

	denied := []struct {
		name   string
		target string
		h      http.Handler
	}{
		{name: "external file", target: "/escape-file", h: noFallback},
		{name: "external file trailing slash", target: "/escape-file/", h: noFallback},
		{name: "external directory child", target: "/escape-dir/child.txt", h: noFallback},
		{name: "external directory trailing slash", target: "/escape-dir/", h: noFallback},
		{name: "external file bypasses SPA fallback", target: "/escape-file", h: withFallback},
		{name: "external directory child bypasses SPA fallback", target: "/escape-dir/child.txt", h: withFallback},
		{name: "external fallback is denied", target: "/ordinary-miss", h: externalFallback},
	}
	for _, tt := range denied {
		t.Run(tt.name, func(t *testing.T) {
			rec := serveRequest(tt.h, http.MethodGet, tt.target)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body = %q", rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			for _, secret := range []string{"OUTSIDE_SECRET", "OUTSIDE_INDEX", "OUTSIDE_CHILD", outside} {
				if strings.Contains(body, secret) {
					t.Errorf("response leaked external target detail %q: %q", secret, body)
				}
			}
		})
	}

	allowed := []struct {
		name     string
		target   string
		wantBody string
	}{
		{name: "in-root file symlink", target: "/inside-file-link", wantBody: "INSIDE_FILE"},
		{name: "in-root directory symlink index", target: "/inside-dir-link", wantBody: "INSIDE_INDEX"},
		{name: "non-dot alias to in-root dotfile", target: "/env-alias", wantBody: "IN_ROOT_DOTFILE"},
	}
	for _, tt := range allowed {
		t.Run(tt.name, func(t *testing.T) {
			rec := serveRequest(noFallback, http.MethodGet, tt.target)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body = %q", rec.Code, rec.Body.String())
			}
			if body := rec.Body.String(); body != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
		})
	}

	if rec := serveRequest(noFallback, http.MethodGet, "/.env"); rec.Code != http.StatusNotFound {
		t.Errorf("direct dotfile status = %d, want 404", rec.Code)
	}
	if rec := serveRequest(withFallback, http.MethodGet, "/ordinary-miss"); rec.Code != http.StatusOK || rec.Body.String() != "ROOT_INDEX" {
		t.Errorf("ordinary fallback = (%d, %q), want (200, ROOT_INDEX)", rec.Code, rec.Body.String())
	}
}

func TestSPAFileServerTraversalShapedRequests(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	writeServeFile(t, dir, "index.html", "ROOT_INDEX")
	writeServeFile(t, outside, "outside.txt", "OUTSIDE_SECRET")
	h := openTestSPAFileServer(t, dir, "")

	for _, target := range []string{
		"/../outside.txt",
		"/%2e%2e/outside.txt",
		"/safe/../../outside.txt",
		"/safe/%2e%2e/%2e%2e/outside.txt",
	} {
		t.Run(target, func(t *testing.T) {
			rec := serveRequest(h, http.MethodGet, target)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body = %q", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "OUTSIDE_SECRET") || strings.Contains(rec.Body.String(), outside) {
				t.Errorf("traversal response leaked external data: %q", rec.Body.String())
			}
		})
	}
}

func TestBindProxyRoutesValidatesRootsAndCleansResources(t *testing.T) {
	s := newTestShim()
	t.Cleanup(s.cancel)

	t.Run("missing root", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing")
		_, _, failure := s.bindProxyRoutes(staticProxyAddData("missing", 8443, missing))
		if failure == nil || failure.code != "STATIC_ROOT_INVALID" {
			t.Fatalf("failure = %#v, want STATIC_ROOT_INVALID", failure)
		}
	})

	t.Run("non-directory root", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file.txt")
		if err := os.WriteFile(file, []byte("not a directory"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, _, failure := s.bindProxyRoutes(staticProxyAddData("file", 8444, file))
		if failure == nil || failure.code != "STATIC_ROOT_INVALID" {
			t.Fatalf("failure = %#v, want STATIC_ROOT_INVALID", failure)
		}
	})

	t.Run("injected unopenable root", func(t *testing.T) {
		setStaticRootOpener(t, func(string) (staticRoot, error) {
			return nil, fs.ErrPermission
		})
		_, _, failure := s.bindProxyRoutes(staticProxyAddData("denied", 8445, t.TempDir()))
		if failure == nil || failure.code != "STATIC_ROOT_INVALID" {
			t.Fatalf("failure = %#v, want STATIC_ROOT_INVALID", failure)
		}
	})

	t.Run("later invalid route closes earlier root", func(t *testing.T) {
		root := newTrackingStaticRoot()
		setStaticRootOpener(t, func(string) (staticRoot, error) { return root, nil })
		data := staticProxyAddData("later-invalid", 8446, t.TempDir())
		data.Routes = append(data.Routes, proxyRouteData{Prefix: "not-absolute", TargetURL: "http://localhost:3000"})
		_, _, failure := s.bindProxyRoutes(data)
		if failure == nil || failure.code != "INVALID_ROUTE" {
			t.Fatalf("failure = %#v, want INVALID_ROUTE", failure)
		}
		waitForSignal(t, root.closed, "earlier root cleanup")
		if got := root.closeCalls.Load(); got != 1 {
			t.Errorf("root Close calls = %d, want 1", got)
		}
	})

	t.Run("later root-open failure closes earlier root", func(t *testing.T) {
		first := newTrackingStaticRoot()
		calls := 0
		setStaticRootOpener(t, func(string) (staticRoot, error) {
			calls++
			if calls == 1 {
				return first, nil
			}
			return nil, fs.ErrPermission
		})
		data := staticProxyAddData("later-root", 8447, t.TempDir())
		data.Routes = append(data.Routes, proxyRouteData{Prefix: "/second", Dir: t.TempDir()})
		_, _, failure := s.bindProxyRoutes(data)
		if failure == nil || failure.code != "STATIC_ROOT_INVALID" {
			t.Fatalf("failure = %#v, want STATIC_ROOT_INVALID", failure)
		}
		waitForSignal(t, first.closed, "earlier root cleanup")
	})

	t.Run("successful binding transfers an idempotent owner", func(t *testing.T) {
		root := newTrackingStaticRoot()
		setStaticRootOpener(t, func(string) (staticRoot, error) { return root, nil })
		_, resources, failure := s.bindProxyRoutes(staticProxyAddData("success", 8448, t.TempDir()))
		if failure != nil {
			t.Fatalf("bind failure: %#v", failure)
		}
		select {
		case <-root.closed:
			t.Fatal("root closed before owner teardown")
		default:
		}
		resources.close()
		resources.close()
		waitForSignal(t, root.closed, "resource owner close")
		if got := root.closeCalls.Load(); got != 1 {
			t.Errorf("root Close calls = %d, want 1", got)
		}
	})
}

func TestStaticRootInvalidIsStableAddError(t *testing.T) {
	s := newTestShim()
	t.Cleanup(s.cancel)
	var output bytes.Buffer
	s.writer = json.NewEncoder(&output)
	missing := filepath.Join(t.TempDir(), "missing")
	data := staticProxyAddData("invalid-root", 8550, missing)

	s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) {
		t.Fatal("listener creation must not run for an invalid static root")
		return nil, nil
	})

	var got struct {
		Event string              `json:"event"`
		Data  proxyErrorEventData `json:"data"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &got); err != nil {
		t.Fatalf("decode event: %v; output = %q", err, output.String())
	}
	if got.Event != "proxy:error" || got.Data.Code != "STATIC_ROOT_INVALID" {
		t.Fatalf("event = %#v, want proxy:error/STATIC_ROOT_INVALID", got)
	}
	s.proxyMu.Lock()
	_, exists := s.proxies[data.ID]
	s.proxyMu.Unlock()
	if exists {
		t.Fatal("invalid root installed a placeholder or live proxy")
	}
}

func TestProxyAddClosesRootsOnEveryPreTransferBranch(t *testing.T) {
	run := func(t *testing.T, setup func(*shim, proxyAddData), listen proxyListenFunc) {
		t.Helper()
		s := newTestShim()
		t.Cleanup(s.cancel)
		root := newTrackingStaticRoot()
		setStaticRootOpener(t, func(string) (staticRoot, error) { return root, nil })
		data := staticProxyAddData("pending", 8660, t.TempDir())
		if setup != nil {
			setup(s, data)
		}
		s.startProxyAdd(nil, nil, data, listen)
		waitForSignal(t, root.closed, "pending root cleanup")
		if got := root.closeCalls.Load(); got != 1 {
			t.Errorf("root Close calls = %d, want 1", got)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			s.proxyMu.Lock()
			entry, exists := s.proxies[data.ID]
			pending := s.pendingProxyAdds[data.ID]
			s.proxyMu.Unlock()
			if pending == nil && (!exists || entry != nil) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("failed add left a placeholder or pending resource owner")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	t.Run("duplicate proxy id", func(t *testing.T) {
		run(t, func(s *shim, data proxyAddData) {
			s.proxies[data.ID] = &proxyEntry{id: data.ID}
		}, func(bool, string) (net.Listener, error) {
			t.Fatal("listener called after duplicate ID")
			return nil, nil
		})
	})

	t.Run("proxy port conflict", func(t *testing.T) {
		run(t, func(s *shim, data proxyAddData) {
			s.proxies["other"] = &proxyEntry{id: "other", listenPort: data.ListenPort}
		}, func(bool, string) (net.Listener, error) {
			t.Fatal("listener called after proxy port conflict")
			return nil, nil
		})
	})

	t.Run("dynamic listener conflict", func(t *testing.T) {
		var dynamic *trackingListener
		run(t, func(s *shim, data proxyAddData) {
			dynamic = newTrackingListener()
			s.dynamicListeners[data.ListenPort] = dynamic
		}, func(bool, string) (net.Listener, error) {
			t.Fatal("listener called after dynamic port conflict")
			return nil, nil
		})
		_ = dynamic.Close()
	})

	t.Run("listener creation failure", func(t *testing.T) {
		run(t, nil, func(bool, string) (net.Listener, error) {
			return nil, errors.New("listen failed")
		})
	})

	t.Run("listener creation returns handle and error", func(t *testing.T) {
		ln := newTrackingListener()
		run(t, nil, func(bool, string) (net.Listener, error) {
			return ln, errors.New("partial listen failure")
		})
		waitForSignal(t, ln.closed, "partial listener cleanup")
	})

	t.Run("post-listen not ready", func(t *testing.T) {
		ln := newTrackingListener()
		run(t, nil, func(bool, string) (net.Listener, error) { return ln, nil })
		waitForSignal(t, ln.closed, "listener cleanup")
	})

	t.Run("panic before transfer", func(t *testing.T) {
		run(t, nil, func(bool, string) (net.Listener, error) {
			panic("listener panic")
		})
	})
}

func TestPendingProxyAddCannotInstallAfterShutdown(t *testing.T) {
	s := newTestShim()
	root := newTrackingStaticRoot()
	setStaticRootOpener(t, func(string) (staticRoot, error) { return root, nil })
	data := staticProxyAddData("shutdown-race", 8770, t.TempDir())
	listenEntered := make(chan struct{})
	releaseListen := make(chan struct{})
	ln := newTrackingListener()

	s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) {
		close(listenEntered)
		<-releaseListen
		return ln, nil
	})
	waitForSignal(t, listenEntered, "listener attempt")

	s.proxyMu.Lock()
	pending, exists := s.proxies[data.ID]
	s.proxyMu.Unlock()
	if !exists || pending != nil {
		t.Fatalf("pending proxy state = (%v, %v), want existing nil placeholder", exists, pending)
	}

	s.handleStop()
	waitForSignal(t, root.closed, "root cleanup after shutdown race")
	// Listener creation is still blocked, so this proves shutdown directly
	// drained the registered pending owner rather than waiting for the add
	// goroutine to return on its own.
	close(releaseListen)
	waitForSignal(t, ln.closed, "listener cleanup after shutdown race")

	s.proxyMu.Lock()
	_, exists = s.proxies[data.ID]
	s.proxyMu.Unlock()
	if exists {
		t.Fatal("pending add installed a proxy after shutdown snapshot")
	}
	if got := root.closeCalls.Load(); got != 1 {
		t.Errorf("root Close calls = %d, want 1", got)
	}
}

func TestOldPendingCleanupCannotDeleteNewPlaceholder(t *testing.T) {
	s := newTestShim()
	t.Cleanup(s.cancel)
	oldPending := newPendingProxyAdd(&proxyResources{})
	newPending := newPendingProxyAdd(&proxyResources{})
	s.proxies["same-id"] = nil
	s.pendingProxyAdds["same-id"] = newPending

	s.removeProxyPlaceholder("same-id", oldPending)

	s.proxyMu.Lock()
	entry, exists := s.proxies["same-id"]
	gotPending := s.pendingProxyAdds["same-id"]
	s.proxyMu.Unlock()
	if !exists || entry != nil || gotPending != newPending {
		t.Fatalf("new attempt was disturbed: exists=%v entry=%v pending=%p, want nil placeholder/%p", exists, entry, gotPending, newPending)
	}
	newPending.abort()
}

func TestProxyRootOwnershipTransferRemoveAndShutdown(t *testing.T) {
	for _, mode := range []string{"remove", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s := newTestShim()
			t.Cleanup(s.cancel)
			s.setDNSName("node.example.ts.net")
			root := newTrackingStaticRoot()
			setStaticRootOpener(t, func(string) (staticRoot, error) { return root, nil })
			data := staticProxyAddData("live-"+mode, 8880, t.TempDir())
			ln := newTrackingListener()
			s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) { return ln, nil })
			entry := waitForProxyEntry(t, s, data.ID)
			if entry.resources == nil {
				t.Fatal("live proxy did not own rooted resources")
			}
			select {
			case <-root.closed:
				t.Fatal("root closed before proxy teardown")
			default:
			}

			if mode == "remove" {
				raw, err := json.Marshal(proxyRemoveData{ID: data.ID})
				if err != nil {
					t.Fatal(err)
				}
				s.handleProxyRemove(raw)
			} else {
				s.handleStop()
			}
			waitForSignal(t, root.closed, "live root teardown")
			waitForSignal(t, ln.closed, "live listener teardown")
			if got := root.closeCalls.Load(); got != 1 {
				t.Errorf("root Close calls = %d, want 1", got)
			}
		})
	}
}

func TestProxyShutdownDrainsHTTPBeforeClosingRoots(t *testing.T) {
	root := newTrackingStaticRoot()
	resources := &proxyResources{}
	resources.addRoot(root)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseRequest) }) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseRequest
		_, _ = io.WriteString(w, "DONE")
	})}
	entry := &proxyEntry{listener: ln, server: httpSrv, resources: resources}
	serveDone := make(chan struct{})
	go func() {
		_ = httpSrv.Serve(ln)
		close(serveDone)
	}()

	clientDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			clientDone <- err
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err == nil && string(body) != "DONE" {
			err = errors.New("unexpected response body: " + string(body))
		}
		clientDone <- err
	}()
	waitForSignal(t, requestStarted, "in-flight HTTP request")

	shutdownDone := make(chan struct{})
	go func() {
		entry.shutdown(2 * time.Second)
		close(shutdownDone)
	}()
	select {
	case <-root.closed:
		t.Fatal("root closed while HTTP request was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseRequest) })
	select {
	case err := <-clientDone:
		if err != nil {
			t.Fatalf("HTTP request failed during graceful shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for HTTP request")
	}
	waitForSignal(t, shutdownDone, "proxy shutdown")
	waitForSignal(t, root.closed, "root close after HTTP drain")
	waitForSignal(t, serveDone, "HTTP server exit")
}

func TestRootedHandleCountReturnsToBaseline(t *testing.T) {
	s := newTestShim()
	t.Cleanup(s.cancel)
	s.setDNSName("node.example.ts.net")
	var liveRoots atomic.Int32
	var openedRoots chan *trackingStaticRoot
	setStaticRootOpener(t, func(string) (staticRoot, error) {
		root := newTrackingStaticRoot()
		liveRoots.Add(1)
		root.onClose = func() { liveRoots.Add(-1) }
		openedRoots <- root
		return root, nil
	})

	for i := 0; i < 20; i++ {
		openedRoots = make(chan *trackingStaticRoot, 1)
		data := staticProxyAddData("repeat-"+strconv.Itoa(i), uint16(9000+i), t.TempDir())
		ln := newTrackingListener()
		s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) { return ln, nil })
		root := <-openedRoots
		_ = waitForProxyEntry(t, s, data.ID)
		raw, err := json.Marshal(proxyRemoveData{ID: data.ID})
		if err != nil {
			t.Fatal(err)
		}
		s.handleProxyRemove(raw)
		waitForSignal(t, root.closed, "repeated remove root cleanup")
	}
	if got := liveRoots.Load(); got != 0 {
		t.Fatalf("live rooted handles = %d after repeated add/remove, want 0", got)
	}

	for i := 0; i < 20; i++ {
		first := newTrackingStaticRoot()
		liveRoots.Add(1)
		first.onClose = func() { liveRoots.Add(-1) }
		calls := 0
		openStaticRoot = func(string) (staticRoot, error) {
			calls++
			if calls == 1 {
				return first, nil
			}
			return nil, fs.ErrPermission
		}
		data := staticProxyAddData("rejected-"+strconv.Itoa(i), uint16(9100+i), t.TempDir())
		data.Routes = append(data.Routes, proxyRouteData{Prefix: "/second", Dir: t.TempDir()})
		_, _, failure := s.bindProxyRoutes(data)
		if failure == nil || failure.code != "STATIC_ROOT_INVALID" {
			t.Fatalf("failure = %#v, want STATIC_ROOT_INVALID", failure)
		}
		waitForSignal(t, first.closed, "rejected add root cleanup")
	}
	if got := liveRoots.Load(); got != 0 {
		t.Fatalf("live rooted handles = %d after rejected adds, want 0", got)
	}
}

// TestBuildReverseProxy exercises the engine's shared reverse-proxy policy
// (RFC 023 §7, P6/P7): a proxied request reaches the backend with the Host
// rewritten, the mount prefix stripped from the backend-visible path, and an
// X-Forwarded-For set; a dead backend yields a detail-free 502.
func TestBuildReverseProxy(t *testing.T) {
	t.Run("proxies request, rewrites host and path, sets XFF", func(t *testing.T) {
		var gotHost, gotPath, gotXFF string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHost = r.Host
			gotPath = r.URL.Path
			gotXFF = r.Header.Get("X-Forwarded-For")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "backend-body")
		}))
		defer backend.Close()

		target, err := url.Parse(backend.URL)
		if err != nil {
			t.Fatalf("parse backend URL: %v", err)
		}

		s := newTestShim()
		rp := s.buildReverseProxy("web", target, "/api")

		req := httptest.NewRequest(http.MethodGet, "http://myapp.ts.net/api/users/1", nil)
		rec := httptest.NewRecorder()
		rp.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); body != "backend-body" {
			t.Errorf("body = %q, want backend-body", body)
		}
		if gotHost != target.Host {
			t.Errorf("backend Host = %q, want %q (rewritten to target)", gotHost, target.Host)
		}
		if gotPath != "/users/1" {
			t.Errorf("backend path = %q, want /users/1 (prefix /api stripped)", gotPath)
		}
		if gotXFF == "" {
			t.Errorf("backend saw no X-Forwarded-For")
		}
	})

	t.Run("dead backend yields detail-free 502", func(t *testing.T) {
		// A closed server frees its port, so the proxy dial is refused.
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		target, err := url.Parse(backend.URL)
		if err != nil {
			t.Fatalf("parse backend URL: %v", err)
		}
		backend.Close()

		s := newTestShim()
		rp := s.buildReverseProxy("web", target, "")

		req := httptest.NewRequest(http.MethodGet, "http://myapp.ts.net/", nil)
		rec := httptest.NewRecorder()
		rp.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", rec.Code)
		}
		body := rec.Body.String()
		if strings.TrimSpace(body) != "Bad Gateway" {
			t.Errorf("body = %q, want bare \"Bad Gateway\"", body)
		}
		// P7: the target host/port and dial state must not leak to the caller.
		if strings.Contains(body, target.Host) || strings.Contains(body, "connection refused") {
			t.Errorf("502 body leaked internal detail: %q", body)
		}
	})
}
