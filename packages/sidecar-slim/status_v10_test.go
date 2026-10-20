package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
)

// Protocol 10: statuses name the coordination server, and running statuses
// carry the node's own key expiry.

// effectiveControlURL must follow tsnet's own rule (Server.getControlURL,
// then the backend default), or the core would be told the wrong server.
func TestEffectiveControlURLFollowsTsnet(t *testing.T) {
	t.Setenv("TS_CONTROL_URL", "")
	if got := effectiveControlURL(""); got != ipn.DefaultControlURL {
		t.Fatalf("default = %q, want %q", got, ipn.DefaultControlURL)
	}
	t.Setenv("TS_CONTROL_URL", "https://hs.example.com")
	if got := effectiveControlURL(""); got != "https://hs.example.com" {
		t.Fatalf("with TS_CONTROL_URL = %q, want the env value", got)
	}
	if got := effectiveControlURL("https://configured.example.com"); got != "https://configured.example.com" {
		t.Fatalf("configured = %q, want the configured URL to win", got)
	}
}

// Every status carries the lifecycle's control URL once start has set it;
// before that the key is omitted rather than empty.
func TestStatusesCarryTheControlURL(t *testing.T) {
	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}
	s.sendStatus("error", "", "", "", "boom", "")
	if bytes.Contains(buf.Bytes(), []byte("controlUrl")) {
		t.Fatalf("no lifecycle yet, yet the status names a server: %s", buf.String())
	}

	buf.Reset()
	s.setControlURL("https://hs.example.com")
	s.sendStatus("starting", "host", "", "", "", "")
	var ev struct {
		Event string     `json:"event"`
		Data  statusData `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Data.ControlURL != "https://hs.example.com" || ev.Data.ProtocolVersion != sidecarProtocolVersion {
		t.Fatalf("starting status = %+v", ev.Data)
	}
	if ev.Data.KeyExpiry != "" || strings.Contains(buf.String(), "keyExpiry") {
		t.Fatalf("only running statuses carry a key expiry: %s", buf.String())
	}
}

func TestRunningStatusRendersTheSelfKeyExpiry(t *testing.T) {
	at := time.Date(2026, 6, 24, 12, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	st := &ipnstate.Status{
		BackendState: "Running",
		TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.7")},
		Self:         &ipnstate.PeerStatus{ID: "nSELF", UserID: 42, DNSName: "myhost.tailnet.ts.net.", KeyExpiry: &at},
		User:         map[tailcfg.UserID]tailcfg.UserProfile{42: {ID: 42, LoginName: "alice@example.com"}},
	}
	got := runningStatus(st, "myhost")
	want := statusData{
		State: "running", Hostname: "myhost", DNSName: "myhost.tailnet.ts.net", TailscaleIP: "100.64.0.7",
		NodeID: "nSELF", LoginName: "alice@example.com", KeyExpiry: "2026-06-24T10:00:00Z",
	}
	if got != want {
		t.Fatalf("runningStatus = %+v\nwant          %+v", got, want)
	}

	// tsnet reports no expiry when it is disabled (nil), and a zero time is
	// the same thing: both omit the key.
	st.Self.KeyExpiry = nil
	if got := runningStatus(st, "myhost"); got.KeyExpiry != "" {
		t.Fatalf("disabled expiry rendered as %q", got.KeyExpiry)
	}
	zero := time.Time{}
	st.Self.KeyExpiry = &zero
	if got := runningStatus(st, "myhost"); got.KeyExpiry != "" {
		t.Fatalf("zero expiry rendered as %q", got.KeyExpiry)
	}
	// A status without Self (never Running in practice) does not panic.
	if got := runningStatus(&ipnstate.Status{BackendState: "Running"}, "h"); got.State != "running" {
		t.Fatalf("bare status = %+v", got)
	}
}

type wireStatus struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func decodeStatus(t *testing.T, raw []byte) (string, statusData, string) {
	t.Helper()
	var ev wireStatus
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	var d statusData
	if len(ev.Data) > 0 {
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatalf("decode data: %v", err)
		}
	}
	return ev.Event, d, string(ev.Data)
}

// The real status loop against a fake LocalAPI: both running events carry the
// key expiry and the control URL.
func TestPollUntilRunningReportsKeyExpiryAndControlURL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	lc, _ := fakeLocalAPI(t, func() ipnstate.Status {
		return ipnstate.Status{
			BackendState: "Running",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.7")},
			Self:         &ipnstate.PeerStatus{ID: "nSELF", UserID: 42, DNSName: "myhost.tailnet.ts.net.", KeyExpiry: &at},
			User:         map[tailcfg.UserID]tailcfg.UserProfile{42: {ID: 42, LoginName: "alice@example.com"}},
		}
	})
	events := make(peerWatchEvents, 64)
	s := newTestShim()
	defer s.cancel()
	s.ctx = ctx
	s.writer = json.NewEncoder(events)
	s.setControlURL("https://hs.example.com")
	go s.pollUntilRunning(ctx, lc, "myhost")

	seen := map[string]statusData{}
	timeout := time.After(10 * time.Second)
	for len(seen) < 2 {
		select {
		case raw := <-events:
			name, d, _ := decodeStatus(t, raw)
			if name == "tsnet:status" || name == "tsnet:started" {
				seen[name] = d
			}
		case <-timeout:
			t.Fatalf("saw only %v", seen)
		}
	}
	for name, d := range seen {
		if d.KeyExpiry != "2030-01-02T03:04:05Z" || d.ControlURL != "https://hs.example.com" ||
			d.ProtocolVersion != sidecarProtocolVersion || d.NodeID != "nSELF" {
			t.Errorf("%s = %+v", name, d)
		}
	}
}

// The monitor re-sends the running facts when the key expiry moves — and
// only then: an admin extends the expiry, then disables it.
func TestMonitorStateResendsRunningStatusWhenKeyExpiryMoves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	extended := time.Date(2030, 7, 1, 0, 0, 0, 0, time.UTC)
	var polls atomic.Int32
	lc, _ := fakeLocalAPI(t, func() ipnstate.Status {
		n := polls.Add(1)
		st := ipnstate.Status{
			BackendState: "Running",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.7")},
			Self:         &ipnstate.PeerStatus{ID: "nSELF", UserID: 42, DNSName: "myhost.tailnet.ts.net."},
			User:         map[tailcfg.UserID]tailcfg.UserProfile{42: {ID: 42, LoginName: "alice@example.com"}},
		}
		switch {
		case n <= 3:
			st.Self.KeyExpiry = &first // unchanged: nothing to say
		case n <= 6:
			st.Self.KeyExpiry = &extended
		default:
			// disabled: no expiry
		}
		return st
	})
	events := make(peerWatchEvents, 64)
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(events)
	s.setControlURL(ipn.DefaultControlURL)
	go s.monitorStateEvery(ctx, lc, "myhost", "2030-01-01T00:00:00Z", 10*time.Millisecond)

	var expiries []string
	timeout := time.After(10 * time.Second)
	for polls.Load() < 12 {
		select {
		case raw := <-events:
			name, d, data := decodeStatus(t, raw)
			if name != "tsnet:status" {
				t.Fatalf("unexpected event %s %s", name, data)
			}
			if d.State != "running" || d.LoginName != "alice@example.com" || d.DNSName != "myhost.tailnet.ts.net" ||
				d.ControlURL != ipn.DefaultControlURL {
				t.Fatalf("re-sent status lost its running facts: %s", data)
			}
			if d.KeyExpiry == "" && strings.Contains(data, "keyExpiry") {
				t.Fatalf("a disabled expiry must omit the key: %s", data)
			}
			expiries = append(expiries, d.KeyExpiry)
		case <-timeout:
			t.Fatalf("monitor stalled after %d polls; expiries %v", polls.Load(), expiries)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	// Drain what the last ticks emitted.
	for {
		select {
		case raw := <-events:
			_, d, _ := decodeStatus(t, raw)
			expiries = append(expiries, d.KeyExpiry)
			continue
		case <-time.After(50 * time.Millisecond):
		}
		break
	}
	want := []string{"2030-07-01T00:00:00Z", ""}
	if len(expiries) != len(want) || expiries[0] != want[0] || expiries[1] != want[1] {
		t.Fatalf("re-sent expiries = %q, want %q (one per change, none for an unchanged value)", expiries, want)
	}
}
