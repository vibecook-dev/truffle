package main

import (
	"tailscale.com/ipn/ipnstate"
	"testing"
)

func TestPingProjectionDistinguishesPaths(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      ipnstate.PingResult
		direct     bool
		derp, peer string
	}{
		{name: "direct", input: ipnstate.PingResult{Endpoint: "192.0.2.1:1234"}, direct: true},
		{name: "derp", input: ipnstate.PingResult{DERPRegionID: 1, DERPRegionCode: "ord"}, derp: "ord"},
		{name: "peer-relay", input: ipnstate.PingResult{PeerRelay: "100.64.1.1:1234:vni:2"}, peer: "100.64.1.1:1234:vni:2"},
		{name: "no-path", input: ipnstate.PingResult{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pingResultToData(&tc.input)
			if got.Direct != tc.direct || got.Relay != tc.derp || got.PeerRelay != tc.peer {
				t.Fatalf("wrong path: %+v", got)
			}
		})
	}
}
func TestPeerProjectionAndWatchIncludeRelayActivity(t *testing.T) {
	before := statusPeerToInfo(&ipnstate.PeerStatus{ID: "peer", Relay: "ord"}, nil)
	after := statusPeerToInfo(&ipnstate.PeerStatus{ID: "peer", Relay: "ord", Active: true, PeerRelay: "100.64.1.1:1234:vni:2"}, nil)
	if !after.Active || after.PeerRelay == "" || !watchPeerChanged(before, after) {
		t.Fatalf("path transition lost: %+v", after)
	}
}
