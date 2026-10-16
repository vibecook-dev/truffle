package main

import (
	"fmt"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"tailscale.com/tsnet"
	"tailscale.com/wgengine/netstack"
)

// tailnetTimeWait is the TIME_WAIT duration of this node's userspace TCP stack
// (gVisor's default is 60 s).
//
// Why it matters: tsnet (without a TUN) accepts tailnet connections through
// netstack's TCP forwarder, not a gVisor listening endpoint. When a SYN arrives
// for a 4-tuple that is still in TIME_WAIT here, gVisor only reopens it if a
// listening endpoint exists (RFC 1122 "SYN in TIME-WAIT"); with the forwarder
// there is none, so the SYN — and every retransmission — is silently dropped
// until TIME_WAIT expires. Whenever this node closes an accepted connection
// first (an HTTP server answering `Connection: close`, a responder closing a
// data socket), the peer's next dial that happens to pick the same random
// source port (16000–65535) hangs for up to the whole TIME_WAIT. At 24 new
// connections/s that was ~3% of dials stalling until the 30 s dial timeout; at
// ~1k/s the 60 s TIME_WAIT pool covers most of the port range.
//
// A few seconds still absorbs a retransmitted FIN and any segment delayed in
// the tunnel, while bounding a collision to one SYN retransmission (gVisor's
// initial RTO is 1 s). Reusing a 4-tuple early is safe for the same reason
// Linux's TIME_WAIT reuse is: ISNs advance 15.6 M per second per 4-tuple (64 ns
// clock), so a new incarnation's window lies far beyond the old one's
// sequence space, and WireGuard drops packets that are older than its replay
// window.
const tailnetTimeWait = time.Second

// tuneNetstack applies the sidecar's TCP stack options to a started server.
// It is best effort: a tsnet that no longer exposes its netstack keeps
// gVisor's defaults, which only costs the TIME_WAIT behaviour above.
func tuneNetstack(srv *tsnet.Server) error {
	sys := srv.Sys()
	if sys == nil {
		return fmt.Errorf("tsnet server has no subsystems")
	}
	impl, ok := sys.Netstack.GetOK()
	if !ok {
		return fmt.Errorf("tsnet server has no netstack")
	}
	ns, ok := impl.(*netstack.Impl)
	if !ok {
		return fmt.Errorf("unexpected netstack type %T", impl)
	}
	return setTimeWait(ns, tailnetTimeWait)
}

// transportOptionSetter is the slice of *netstack.Impl that setTimeWait needs,
// so a test can check the option it sets without a running tsnet.
type transportOptionSetter interface {
	SetTransportProtocolOption(tcpip.TransportProtocolNumber, tcpip.SettableTransportProtocolOption) tcpip.Error
}

func setTimeWait(ns transportOptionSetter, d time.Duration) error {
	opt := tcpip.TCPTimeWaitTimeoutOption(d)
	if err := ns.SetTransportProtocolOption(tcp.ProtocolNumber, &opt); err != nil {
		return fmt.Errorf("set TCP TIME_WAIT timeout: %v", err)
	}
	return nil
}
