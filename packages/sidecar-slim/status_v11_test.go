package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tailscaleroot "tailscale.com"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
)

// Protocol 11: logout, the client-log opt-out, Tailnet Lock's signing state,
// the embedded Tailscale version, and backend state changes seen through the
// IPN bus.

// fakeLocalAPIMux serves mux from an httptest server and returns the LocalAPI
// client the sidecar would use against it.
func fakeLocalAPIMux(t *testing.T, mux *http.ServeMux) *local.Client {
	t.Helper()
	api := httptest.NewServer(mux)
	t.Cleanup(api.Close)
	return newLocalAPIClient(&local.Client{Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, api.Listener.Addr().String())
	}})
}

func serveStatus(t *testing.T, mux *http.ServeMux, status func() ipnstate.Status) {
	mux.HandleFunc("/localapi/v0/status", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(status()); err != nil {
			t.Errorf("write status: %v", err)
		}
	})
}

func serveLock(mux *http.ServeMux, lock func() lockStatusData) {
	mux.HandleFunc("/localapi/v0/tka/status", func(w http.ResponseWriter, r *http.Request) {
		l := lock()
		json.NewEncoder(w).Encode(ipnstate.TailnetLockStatus{Enabled: l.Enabled, NodeKeySigned: l.Signed})
	})
}

func runningSelf() ipnstate.Status {
	return ipnstate.Status{
		BackendState: "Running",
		TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.7")},
		Self:         &ipnstate.PeerStatus{ID: "nSELF", UserID: 42, DNSName: "myhost.tailnet.ts.net."},
		User:         map[tailcfg.UserID]tailcfg.UserProfile{42: {ID: 42, LoginName: "alice@example.com"}},
	}
}

type wireEvent struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func nextEvent(t *testing.T, events peerWatchEvents, within time.Duration) wireEvent {
	t.Helper()
	select {
	case raw := <-events:
		var ev wireEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		return ev
	case <-time.After(within):
		t.Fatalf("no event within %v", within)
		return wireEvent{}
	}
}

// awaitEvent skips events until one named name arrives.
func awaitEvent(t *testing.T, events peerWatchEvents, name string, within time.Duration) wireEvent {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		ev := nextEvent(t, events, time.Until(deadline))
		if ev.Event == name {
			return ev
		}
	}
}

// optedOut sets the process-wide opt-out flag for one test, with the real
// switches replaced, and puts both back after it.
func optedOut(t *testing.T) *atomic.Int32 {
	t.Helper()
	var calls atomic.Int32
	real := disableTailscaleClientLogs
	disableTailscaleClientLogs = func() { calls.Add(1) }
	clientLogsOptedOut.Store(false)
	t.Cleanup(func() {
		disableTailscaleClientLogs = real
		clientLogsOptedOut.Store(false)
	})
	optOutOfClientLogs()
	return &calls
}

func TestStatusesCarryTheEmbeddedTailscaleVersion(t *testing.T) {
	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}
	s.sendStatus("starting", "host", "", "", "", "")
	_, d, _ := decodeStatus(t, buf.Bytes())
	want := strings.TrimSpace(tailscaleroot.VersionDotTxt)
	if d.TailscaleVersion != want || want == "" {
		t.Fatalf("tailscaleVersion = %q, want the module's VERSION.txt %q", d.TailscaleVersion, want)
	}
	if !strings.HasPrefix(d.TailscaleVersionLong, want) {
		t.Fatalf("tailscaleVersionLong = %q, want it to start with %q", d.TailscaleVersionLong, want)
	}
	if d.ProtocolVersion != 11 {
		t.Fatalf("protocolVersion = %d", d.ProtocolVersion)
	}
}

// tsnet:version needs no node: a core asks before tsnet:start.
func TestVersionAnswersBeforeAnyStart(t *testing.T) {
	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}
	s.handleVersion(json.RawMessage(`{"requestId":"v1"}`))
	var ev struct {
		Event string            `json:"event"`
		Data  versionResultData `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatal(err)
	}
	short, long := tailscaleVersions()
	want := versionResultData{RequestID: "v1", ProtocolVersion: sidecarProtocolVersion, TailscaleVersion: short, TailscaleVersionLong: long}
	if ev.Event != "tsnet:versionResult" || ev.Data != want {
		t.Fatalf("got %s %+v, want %+v", ev.Event, ev.Data, want)
	}
}

func TestStartDataReadsTheClientLogOptOut(t *testing.T) {
	for raw, want := range map[string]string{
		`{"clientLogUpload":false}`: "false",
		`{"clientLogUpload":true}`:  "true",
		`{}`:                        "nil",
	} {
		var d startData
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			t.Fatal(err)
		}
		got := "nil"
		if d.ClientLogUpload != nil {
			got = map[bool]string{true: "true", false: "false"}[*d.ClientLogUpload]
		}
		if got != want {
			t.Errorf("%s: clientLogUpload = %s, want %s", raw, got, want)
		}
	}
}

// The switches cannot be undone within a process, so they are thrown once.
func TestOptOutThrowsTheSwitchesOnce(t *testing.T) {
	calls := optedOut(t)
	optOutOfClientLogs()
	if calls.Load() != 1 || !clientLogsOptedOut.Load() {
		t.Fatalf("switch calls = %d, opted out = %v", calls.Load(), clientLogsOptedOut.Load())
	}
}

func TestLoggingRequiredNeedsTheOptOutAndTheFlowLogCapability(t *testing.T) {
	flowLogs := &ipnstate.Status{Self: &ipnstate.PeerStatus{
		CapMap: tailcfg.NodeCapMap{tailcfg.CapabilityDataPlaneAuditLogs: nil},
	}}
	plain := &ipnstate.Status{Self: &ipnstate.PeerStatus{}}

	clientLogsOptedOut.Store(false)
	if loggingRequired(flowLogs) {
		t.Fatal("a node that uploads its logs is never refused for them")
	}
	optedOut(t)
	if !loggingRequired(flowLogs) {
		t.Fatal("an opted-out node on a flow-log tailnet must be reported")
	}
	if loggingRequired(plain) || loggingRequired(&ipnstate.Status{}) || loggingRequired(nil) {
		t.Fatal("no capability (or no Self) is not a refusal")
	}
}

// An opted-out node that tsnet stopped for the tailnet's flow logs is
// reported once while start waits, instead of waiting for a Running that
// never comes.
func TestPollUntilRunningReportsLoggingRequiredOnce(t *testing.T) {
	optedOut(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var polls atomic.Int32
	mux := http.NewServeMux()
	serveStatus(t, mux, func() ipnstate.Status {
		polls.Add(1)
		return ipnstate.Status{BackendState: "Stopped", Self: &ipnstate.PeerStatus{
			CapMap: tailcfg.NodeCapMap{tailcfg.CapabilityDataPlaneAuditLogs: nil},
		}}
	})
	lc := fakeLocalAPIMux(t, mux)
	events := make(peerWatchEvents, 16)
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(events)
	go s.pollUntilRunning(ctx, lc, "myhost")

	if ev := nextEvent(t, events, 5*time.Second); ev.Event != "tsnet:loggingRequired" {
		t.Fatalf("first event = %s %s", ev.Event, ev.Data)
	}
	for polls.Load() < 4 {
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case raw := <-events:
		t.Fatalf("reported again: %s", raw)
	case <-time.After(100 * time.Millisecond):
	}
}

// A node Tailnet Lock has not signed still reaches Running; its lock state
// goes out before the running events, so the core knows before its start
// returns.
func TestPollUntilRunningSendsTheLockStatusFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := http.NewServeMux()
	serveStatus(t, mux, runningSelf)
	serveLock(mux, func() lockStatusData { return lockStatusData{Enabled: true, Signed: false} })
	lc := fakeLocalAPIMux(t, mux)
	events := make(peerWatchEvents, 16)
	s := newTestShim()
	defer s.cancel()
	s.ctx = ctx
	s.writer = json.NewEncoder(events)
	go s.pollUntilRunning(ctx, lc, "myhost")

	var names []string
	for len(names) < 3 {
		names = append(names, nextEvent(t, events, 5*time.Second).Event)
	}
	if strings.Join(names, ",") != "tsnet:lockStatus,tsnet:status,tsnet:started" {
		t.Fatalf("order = %v", names)
	}
}

// The monitor re-sends the lock state when it changes, and only then.
func TestMonitorStateReportsLockChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reads atomic.Int32
	mux := http.NewServeMux()
	serveStatus(t, mux, runningSelf)
	serveLock(mux, func() lockStatusData {
		// Unsigned for three reads, then a signing node signs us.
		return lockStatusData{Enabled: true, Signed: reads.Add(1) > 3}
	})
	lc := fakeLocalAPIMux(t, mux)
	events := make(peerWatchEvents, 64)
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(events)
	baseline := lockStatusData{Enabled: true, Signed: false}
	go s.monitorStateEvery(ctx, lc, "myhost", "", &baseline, 10*time.Millisecond)

	ev := awaitEvent(t, events, "tsnet:lockStatus", 5*time.Second)
	var got lockStatusData
	if err := json.Unmarshal(ev.Data, &got); err != nil {
		t.Fatal(err)
	}
	if got != (lockStatusData{Enabled: true, Signed: true}) {
		t.Fatalf("lockStatus = %+v", got)
	}
	for reads.Load() < 10 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	for {
		select {
		case raw := <-events:
			if strings.Contains(string(raw), "tsnet:lockStatus") {
				t.Fatalf("an unchanged lock state was re-sent: %s", raw)
			}
			continue
		case <-time.After(50 * time.Millisecond):
		}
		break
	}
}

// A backend state change reaches the core through the IPN bus long before
// the minute's poll: here the poll never fires at all.
func TestBackendStateChangeArrivesThroughTheBus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	state := "Running"
	notify := make(chan ipn.Notify, 1)
	watching := make(chan struct{}, 1)
	mux := http.NewServeMux()
	serveStatus(t, mux, func() ipnstate.Status {
		mu.Lock()
		defer mu.Unlock()
		st := runningSelf()
		st.BackendState = state
		return st
	})
	mux.HandleFunc("/localapi/v0/watch-ipn-bus", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		watching <- struct{}{}
		for {
			select {
			case <-r.Context().Done():
				return
			case n := <-notify:
				if err := json.NewEncoder(w).Encode(n); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		}
	})
	lc := fakeLocalAPIMux(t, mux)
	events := make(peerWatchEvents, 16)
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(events)
	go s.monitorStateEvery(ctx, lc, "myhost", "", nil, time.Hour)

	select {
	case <-watching:
	case <-time.After(5 * time.Second):
		t.Fatal("the monitor never watched the bus")
	}
	mu.Lock()
	state = "NeedsLogin"
	mu.Unlock()
	needsLogin := ipn.NeedsLogin
	notify <- ipn.Notify{State: &needsLogin}

	ev := awaitEvent(t, events, "tsnet:stateChange", 3*time.Second)
	var d stateChangeData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		t.Fatal(err)
	}
	if d.State != "NeedsLogin" {
		t.Fatalf("stateChange = %+v", d)
	}
	cancel() // before the server closes with the stream open
}

func TestLogoutAnswersWithTheRequestID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		wantEvent string
	}{
		{"signed out", http.StatusNoContent, "tsnet:loggedOut"},
		{"control unreachable", http.StatusInternalServerError, "tsnet:error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			mux := http.NewServeMux()
			mux.HandleFunc("/localapi/v0/logout", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				if tc.status != http.StatusNoContent {
					http.Error(w, "control server unreachable", tc.status)
					return
				}
				w.WriteHeader(tc.status)
			})
			lc := fakeLocalAPIMux(t, mux)
			events := make(peerWatchEvents, 4)
			s := newTestShim()
			defer s.cancel()
			s.writer = json.NewEncoder(events)
			s.logout(s.lifecycleCtx(), lc, "out-1")

			ev := nextEvent(t, events, 5*time.Second)
			var d struct {
				RequestID string `json:"requestId"`
				Code      string `json:"code"`
			}
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				t.Fatal(err)
			}
			if ev.Event != tc.wantEvent || d.RequestID != "out-1" || posts.Load() != 1 {
				t.Fatalf("got %s %s after %d POSTs", ev.Event, ev.Data, posts.Load())
			}
			if tc.wantEvent == "tsnet:error" && d.Code != "LOGOUT_ERROR" {
				t.Fatalf("code = %q", d.Code)
			}
		})
	}
}

func TestLogoutWithoutANodeFailsItsWaiter(t *testing.T) {
	var buf bytes.Buffer
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(&buf)
	s.handleLogout(json.RawMessage(`{"requestId":"out-2"}`))
	var ev struct {
		Event string    `json:"event"`
		Data  errorData `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Event != "tsnet:error" || ev.Data.Code != "NOT_RUNNING" || ev.Data.RequestID != "out-2" {
		t.Fatalf("got %s %+v", ev.Event, ev.Data)
	}
}
