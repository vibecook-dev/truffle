package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"tailscale.com/hostinfo"
	"tailscale.com/ipn"
	"tailscale.com/tailcfg"
)

// Protocol 12 (T8): a node announces its app in Hostinfo.App, and every peer
// row carries the peer's Hostinfo.App, so the core admits by the tag rather
// than by the hostname prefix.

func TestStartDataReadsApp(t *testing.T) {
	var d startData
	if err := json.Unmarshal([]byte(`{"hostname":"vibefield-studio","app":"truffle:vibefield"}`), &d); err != nil {
		t.Fatal(err)
	}
	if d.App != "truffle:vibefield" {
		t.Fatalf("App = %q", d.App)
	}
}

// applyApp must reach the Hostinfo tsnet sends to control, and an empty tag
// must clear a previous start's.
func TestApplyAppSetsHostinfo(t *testing.T) {
	t.Cleanup(func() { hostinfo.SetApp("") })
	applyApp("truffle:vibefield")
	if got := hostinfo.New().App; got != "truffle:vibefield" {
		t.Fatalf("Hostinfo.App = %q, want truffle:vibefield", got)
	}
	applyApp("")
	if got := hostinfo.New().App; got != "" {
		t.Fatalf("Hostinfo.App after clearing = %q, want empty", got)
	}
}

func nodeWithApp(id tailcfg.NodeID, app string) *tailcfg.Node {
	return &tailcfg.Node{ID: id, Hostinfo: (&tailcfg.Hostinfo{App: app}).View()}
}

// The cache asks the LocalAPI once per peer, follows full-node notifies (a
// Hostinfo change arrives as one), forgets removed peers, and never caches a
// failed lookup.
func TestPeerAppCacheFetchesOnceAndFollowsNotifies(t *testing.T) {
	ctx := context.Background()
	var c peerAppCache
	fetches := 0
	app := "truffle:a"
	fetch := func(_ context.Context, id tailcfg.NodeID) (*tailcfg.Node, error) {
		fetches++
		return nodeWithApp(id, app), nil
	}
	if got := c.appFor(ctx, 7, fetch); got != "truffle:a" {
		t.Fatalf("first lookup = %q", got)
	}
	if got := c.appFor(ctx, 7, fetch); got != "truffle:a" || fetches != 1 {
		t.Fatalf("second lookup = %q after %d fetches, want a cache hit", got, fetches)
	}
	c.observe(&ipn.Notify{PeersChanged: []*tailcfg.Node{nodeWithApp(7, "truffle:b")}})
	if got := c.appFor(ctx, 7, fetch); got != "truffle:b" || fetches != 1 {
		t.Fatalf("after a full-node notify = %q (%d fetches), want truffle:b from the notify", got, fetches)
	}
	c.observe(&ipn.Notify{PeersRemoved: []tailcfg.NodeID{7}})
	app = "truffle:c"
	if got := c.appFor(ctx, 7, fetch); got != "truffle:c" || fetches != 2 {
		t.Fatalf("after removal = %q (%d fetches), want a fresh lookup", got, fetches)
	}
	failing := func(context.Context, tailcfg.NodeID) (*tailcfg.Node, error) {
		fetches++
		return nil, errors.New("no such peer")
	}
	if got := c.appFor(ctx, 9, failing); got != "" {
		t.Fatalf("failed lookup = %q, want empty", got)
	}
	before := fetches
	c.appFor(ctx, 9, failing)
	if fetches != before+1 {
		t.Fatal("a failed lookup must not be cached")
	}
	c.reset()
	c.appFor(ctx, 7, fetch)
	if fetches != before+2 {
		t.Fatal("reset must forget every peer")
	}
}

// A node control sent without Hostinfo has no app, and must not crash the
// watcher (its view's accessors would dereference nil).
func TestPeerAppCacheToleratesANodeWithoutHostinfo(t *testing.T) {
	var c peerAppCache
	c.observe(&ipn.Notify{PeersChanged: []*tailcfg.Node{{ID: 3}}})
	fetch := func(context.Context, tailcfg.NodeID) (*tailcfg.Node, error) {
		return &tailcfg.Node{ID: 4}, nil
	}
	if got := c.appFor(context.Background(), 3, fetch); got != "" {
		t.Fatalf("observed node without Hostinfo: app = %q", got)
	}
	if got := c.appFor(context.Background(), 4, fetch); got != "" {
		t.Fatalf("fetched node without Hostinfo: app = %q", got)
	}
}

// A tag arriving after a peer joined (its Hostinfo landed late) must surface
// as an update, or the core would never learn it.
func TestWatchPeerChangedSeesApp(t *testing.T) {
	old := peerInfo{ID: "n1", Hostname: "vibefield-studio"}
	cur := old
	cur.App = "truffle:vibefield"
	if !watchPeerChanged(old, cur) {
		t.Fatal("an App change must count as a change")
	}
}
