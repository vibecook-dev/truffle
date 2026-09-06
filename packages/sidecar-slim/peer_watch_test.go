package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

type peerWatchEvents chan []byte

func (w peerWatchEvents) Write(p []byte) (int, error) {
	w <- append([]byte(nil), p...)
	return len(p), nil
}

// Exercise the real LocalAPI streaming client and Truffle's event producer.
// A NetMap-only watcher misses every change below on Tailscale 1.102/macOS.
func TestPeerWatchWithoutLegacyNetMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	peerKey := key.NewNode().Public()
	status := ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{}}
	notifications := make(chan ipn.Notify, 8)
	statusReads := make(chan struct{}, 16)
	watchMasks := make(chan ipn.NotifyWatchOpt, 4)
	disconnect := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/localapi/v0/status":
			mu.Lock()
			err := json.NewEncoder(w).Encode(status)
			mu.Unlock()
			if err != nil {
				t.Errorf("write status: %v", err)
			}
			statusReads <- struct{}{}
		case "/localapi/v0/watch-ipn-bus":
			mask, err := strconv.ParseUint(r.URL.Query().Get("mask"), 0, 64)
			if err != nil {
				t.Errorf("parse watch mask: %v", err)
			}
			watchMasks <- ipn.NotifyWatchOpt(mask)
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-disconnect:
					return
				case n := <-notifications:
					if err := json.NewEncoder(w).Encode(n); err != nil {
						return
					}
					w.(http.Flusher).Flush()
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	defer cancel() // cancel before closing a server with an open stream
	lc := &local.Client{Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, api.Listener.Addr().String())
	}}
	events := make(peerWatchEvents, 16)
	s := newTestShim()
	defer s.cancel()
	s.ctx = ctx
	s.writer = json.NewEncoder(events)
	s.startPeerWatch(lc)

	awaitRead := func() {
		t.Helper()
		select {
		case <-statusReads:
		case <-time.After(3 * time.Second):
			t.Fatal("peer notification did not refresh status before the 30-second poll")
		}
	}
	awaitMask := func() {
		t.Helper()
		select {
		case mask := <-watchMasks:
			if mask&(ipn.NotifyPeerChanges|ipn.NotifyPeerPatches) == 0 {
				t.Errorf("watch does not subscribe to peer deltas: %v", mask)
			}
			if mask&ipn.NotifyNoNetMap == 0 {
				t.Errorf("watch still requests legacy full maps: %v", mask)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("peer watcher did not connect")
		}
	}
	awaitEvent := func(want string) peerChangedData {
		t.Helper()
		select {
		case raw := <-events:
			var ev struct {
				Event string          `json:"event"`
				Data  peerChangedData `json:"data"`
			}
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatalf("decode event: %v", err)
			}
			if ev.Event != "tsnet:peerChanged" || ev.Data.ChangeType != want || ev.Data.PeerID != "peer-1" {
				t.Fatalf("unexpected event: %s", raw)
			}
			return ev.Data
		case <-time.After(3 * time.Second):
			t.Fatalf("no %s event before the 30-second poll", want)
			return peerChangedData{}
		}
	}
	awaitMask()
	awaitRead() // seed an empty peer set without fabricating joined events

	mu.Lock()
	status.Peer[peerKey] = &ipnstate.PeerStatus{ID: "peer-1", HostName: "one", Online: true}
	mu.Unlock()
	notifications <- ipn.Notify{PeersChanged: []*tailcfg.Node{{ID: 1, StableID: "peer-1"}}}
	awaitRead()
	if got := awaitEvent("joined"); got.Peer == nil || !got.Peer.Online {
		t.Fatalf("joined event lost peer status: %+v", got)
	}

	mu.Lock()
	status.Peer[peerKey].Online = false
	mu.Unlock()
	online := false
	notifications <- ipn.Notify{PeerChangedPatch: []*tailcfg.PeerChange{{NodeID: 1, Online: &online}}}
	awaitRead()
	if got := awaitEvent("updated"); got.Peer == nil || got.Peer.Online {
		t.Fatalf("patch did not update peer status: %+v", got)
	}

	// Losing the stream must reconnect and preserve the existing baseline.
	disconnect <- struct{}{}
	select {
	case raw := <-events:
		var ev event
		if err := json.Unmarshal(raw, &ev); err != nil || ev.Event != "tsnet:watchPeersError" {
			t.Fatalf("unexpected disconnect event: %s", raw)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch disconnect was not reported")
	}
	awaitMask()
	awaitRead()
	mu.Lock()
	delete(status.Peer, peerKey)
	mu.Unlock()
	notifications <- ipn.Notify{PeersRemoved: []tailcfg.NodeID{1}}
	awaitRead()
	awaitEvent("left") // a fabricated joined event on reconnect fails here
}
