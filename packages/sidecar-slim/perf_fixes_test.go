package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
)

// fakeLocalAPI serves /localapi/v0/status and /localapi/v0/whois from an
// httptest server and returns a LocalAPI client dialing it the way tsnet's
// in-memory client dials its listener.
func fakeLocalAPI(t *testing.T, status func() ipnstate.Status) (*local.Client, *atomic.Int32) {
	t.Helper()
	var authHeaders atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			authHeaders.Add(1)
		}
		switch r.URL.Path {
		case "/localapi/v0/status":
			if err := json.NewEncoder(w).Encode(status()); err != nil {
				t.Errorf("write status: %v", err)
			}
		case "/localapi/v0/whois":
			io.WriteString(w, `{"Node":{"Name":"peer.tailnet.ts.net.","StableID":"nPEER"},"UserProfile":{"LoginName":"alice@example.com"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	base := &local.Client{Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, api.Listener.Addr().String())
	}}
	return newLocalAPIClient(base), &authHeaders
}

// TestLocalAPIClientSkipsTheMacOSTokenLookup pins the fix for the ~40 ms
// fixed cost on every accepted tailnet connection. local.Client looks up the
// macOS GUI's LocalAPI token on every request unless OmitAuth is set — on
// darwin that execs `lsof` under a process-wide mutex — so the per-accept
// WhoIs cost ~40 ms and serialised all accepts. The client the sidecar uses
// must keep tsnet's dialer, set OmitAuth, and pool enough idle connections
// for concurrent accepts.
func TestLocalAPIClientSkipsTheMacOSTokenLookup(t *testing.T) {
	if newLocalAPIClient(nil) != nil {
		t.Fatal("a nil base client must pass through")
	}
	noDial := &local.Client{}
	if got := newLocalAPIClient(noDial); got != noDial {
		t.Fatal("a base client without a Dial hook must be returned unchanged")
	}

	lc, authHeaders := fakeLocalAPI(t, func() ipnstate.Status { return ipnstate.Status{BackendState: "Running"} })
	if !lc.OmitAuth {
		t.Fatal("the sidecar's LocalAPI client must set OmitAuth")
	}
	if lc.Dial == nil {
		t.Fatal("the sidecar's LocalAPI client must keep tsnet's in-memory dialer")
	}
	tr, ok := lc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", lc.Transport)
	}
	if tr.MaxIdleConnsPerHost < 16 || tr.MaxIdleConns < tr.MaxIdleConnsPerHost {
		t.Fatalf("idle pool = %d per host / %d total; concurrent accepts would re-dial", tr.MaxIdleConnsPerHost, tr.MaxIdleConns)
	}

	// 50 sequential WhoIs calls: ~2 s with the per-request token lookup on
	// darwin, a few ms without it.
	const calls = 50
	start := time.Now()
	for i := 0; i < calls; i++ {
		if _, err := lc.WhoIs(context.Background(), "100.64.0.2:1234"); err != nil {
			t.Fatalf("WhoIs: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("%d WhoIs calls took %v; each LocalAPI call should cost well under a millisecond", calls, elapsed)
	}
	if n := authHeaders.Load(); n != 0 {
		t.Fatalf("%d requests carried an Authorization header; the in-memory LocalAPI needs none", n)
	}
}

// TestSetTimeWaitAppliesToGVisor checks the TIME_WAIT option against a real
// gVisor stack: the option type and protocol must be ones gVisor accepts, and
// the stack must read back the configured duration.
func TestSetTimeWaitAppliesToGVisor(t *testing.T) {
	st := stack.New(stack.Options{TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol}})
	defer st.Close()

	var before tcpip.TCPTimeWaitTimeoutOption
	if err := st.TransportProtocolOption(tcp.ProtocolNumber, &before); err != nil {
		t.Fatalf("read default TIME_WAIT: %v", err)
	}
	if time.Duration(before) != tcp.DefaultTCPTimeWaitTimeout {
		t.Fatalf("default TIME_WAIT = %v, want gVisor's %v", time.Duration(before), tcp.DefaultTCPTimeWaitTimeout)
	}

	if err := setTimeWait(st, tailnetTimeWait); err != nil {
		t.Fatalf("setTimeWait: %v", err)
	}
	var after tcpip.TCPTimeWaitTimeoutOption
	if err := st.TransportProtocolOption(tcp.ProtocolNumber, &after); err != nil {
		t.Fatalf("read TIME_WAIT: %v", err)
	}
	if time.Duration(after) != tailnetTimeWait {
		t.Fatalf("TIME_WAIT = %v, want %v", time.Duration(after), tailnetTimeWait)
	}
	if tailnetTimeWait > 5*time.Second || tailnetTimeWait < 500*time.Millisecond {
		t.Fatalf("tailnetTimeWait = %v; keep it a little above one SYN RTO (1 s)", tailnetTimeWait)
	}
}

// TestReverseProxyReusesUpstreamConnections runs keep-alive load through a
// proxy route at a concurrency above Go's default idle cap of 2 per host and
// counts the upstream connections the backend accepts. With the default the
// proxy closed and re-dialed upstream connections after most requests.
func TestReverseProxyReusesUpstreamConnections(t *testing.T) {
	var upstreamConns atomic.Int32
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}))
	backend.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			upstreamConns.Add(1)
		}
	}
	backend.Start()
	defer backend.Close()

	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend URL: %v", err)
	}
	front := httptest.NewServer(newTestShim().buildReverseProxy("web", target, ""))
	defer front.Close()

	const workers, perWorker = 16, 40
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: workers, MaxIdleConns: workers}}
	defer client.CloseIdleConnections()

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				resp, err := client.Get(front.URL + "/")
				if err != nil {
					errs <- err
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					errs <- fmt.Errorf("status %d", resp.StatusCode)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("request through the proxy failed: %v", err)
	}
	// Every worker may need its own upstream connection; nothing beyond that
	// should be dialed (a little slack for scheduling).
	if got := upstreamConns.Load(); got > workers+4 {
		t.Fatalf("backend accepted %d connections for %d keep-alive requests from %d workers; want <= %d",
			got, workers*perWorker, workers, workers+4)
	}
}

// TestRunningWithoutLoginReportsItLater drives the real status loop against a
// LocalAPI whose first answers are Running without our own user profile (a
// fast restart from saved state, before the netmap arrives) and checks that a
// follow-up "running" status carries the login once the profile appears.
func TestRunningWithoutLoginReportsItLater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var polls atomic.Int32
	lc, _ := fakeLocalAPI(t, func() ipnstate.Status {
		st := ipnstate.Status{
			BackendState: "Running",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.7")},
			Self:         &ipnstate.PeerStatus{ID: "nodeSELF", UserID: 42, DNSName: "myhost.tailnet.ts.net."},
		}
		// The first three answers name no user; later ones do.
		if polls.Add(1) > 3 {
			st.User = map[tailcfg.UserID]tailcfg.UserProfile{42: {ID: 42, LoginName: "alice@example.com"}}
		}
		return st
	})

	events := make(peerWatchEvents, 64)
	s := newTestShim()
	defer s.cancel()
	s.ctx = ctx
	s.writer = json.NewEncoder(events)

	go s.pollUntilRunning(ctx, lc, "myhost")

	type statusEvent struct {
		Event string     `json:"event"`
		Data  statusData `json:"data"`
	}
	var sequence []statusEvent
	timeout := time.After(10 * time.Second)
	for {
		select {
		case raw := <-events:
			var ev statusEvent
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatalf("decode event: %v", err)
			}
			if ev.Event != "tsnet:status" && ev.Event != "tsnet:started" {
				continue
			}
			sequence = append(sequence, ev)
			if ev.Event == "tsnet:status" && ev.Data.LoginName != "" {
				if len(sequence) < 3 {
					t.Fatalf("login arrived before the login-less running pair: %+v", sequence)
				}
				for _, early := range sequence[:2] {
					if early.Data.LoginName != "" {
						t.Fatalf("the first running events must not invent a login: %+v", early)
					}
				}
				if ev.Data.State != "running" || ev.Data.LoginName != "alice@example.com" ||
					ev.Data.DNSName != "myhost.tailnet.ts.net" || ev.Data.TailscaleIP != "100.64.0.7" ||
					ev.Data.ProtocolVersion != sidecarProtocolVersion {
					t.Fatalf("follow-up status = %+v", ev.Data)
				}
				return
			}
		case <-timeout:
			t.Fatalf("no follow-up status with the login; saw %+v", sequence)
		}
	}
}

// TestAwaitSelfLoginStopsWithTheLifecycle: a stop or restart cancels the
// lifecycle context, and the poll must end without emitting anything.
func TestAwaitSelfLoginStopsWithTheLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	lc, _ := fakeLocalAPI(t, func() ipnstate.Status {
		return ipnstate.Status{BackendState: "Running", Self: &ipnstate.PeerStatus{UserID: 42}}
	})
	events := make(peerWatchEvents, 8)
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(events)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.awaitSelfLogin(ctx, lc, "myhost", 10*time.Millisecond, time.Minute)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("awaitSelfLogin kept running after its lifecycle ended")
	}
	select {
	case raw := <-events:
		t.Fatalf("unexpected event without a login: %s", raw)
	default:
	}

	// A spent budget also ends the poll.
	done = make(chan struct{})
	go func() {
		defer close(done)
		s.awaitSelfLogin(context.Background(), lc, "myhost", 10*time.Millisecond, 30*time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("awaitSelfLogin outlived its budget")
	}
}

// TestUnlistenUntracksTheListener: tsnet:unlisten must drop the closed
// listener from the stop-time cleanup list, or a node that opens a data port
// per file transfer accumulates closed listeners for its whole life.
func TestUnlistenUntracksTheListener(t *testing.T) {
	s := newTestShim()
	defer s.cancel()
	keep, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer keep.Close()
	gone, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.trackListener(keep)
	s.trackListener(gone)
	s.dynamicListeners[4242] = gone

	s.handleUnlisten(json.RawMessage(`{"port":4242}`))

	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	if len(s.listeners) != 1 || s.listeners[0] != keep {
		t.Fatalf("tracked listeners after unlisten = %v, want only the other listener", s.listeners)
	}
	if _, err := gone.Accept(); err == nil {
		t.Fatal("the unlistened listener is still open")
	}
}
