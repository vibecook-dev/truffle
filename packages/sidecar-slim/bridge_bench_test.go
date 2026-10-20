package main

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/loopback"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// This file only uses bridgeCopy and helpers that predate the copy-loop
// changes, so it runs unchanged against an older sidecar for before/after
// numbers.

// gonetPair returns both ends of a TCP connection through an in-process gVisor
// netstack over a loopback NIC: the same gonet.TCPConn type tsnet hands the
// sidecar for every tailnet connection (deadlines via time.AfterFunc, writes
// into gVisor's send buffer), with tsnet's TCP buffer and SACK settings
// (wgengine/netstack, v1.104.1). No Tailscale, no network: the loopback NIC's
// 64 KiB MTU makes it far cheaper per byte than a real tailnet path, which
// leaves the bridge loop's own costs MORE visible, not less.
func gonetPair(tb testing.TB) (client, server net.Conn) {
	tb.Helper()
	st := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})
	rx := tcpip.TCPReceiveBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: tcp.DefaultSendBufferSize, Max: 8 << 20}
	tx := tcpip.TCPSendBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: tcp.DefaultReceiveBufferSize, Max: 6 << 20}
	sack := tcpip.TCPSACKEnabled(true)
	for _, opt := range []tcpip.SettableTransportProtocolOption{&rx, &tx, &sack} {
		if err := st.SetTransportProtocolOption(tcp.ProtocolNumber, opt); err != nil {
			tb.Fatalf("netstack option %T: %v", opt, err)
		}
	}
	if err := st.CreateNIC(1, loopback.New()); err != nil {
		tb.Fatalf("CreateNIC: %v", err)
	}
	addr := tcpip.AddrFrom4([4]byte{127, 0, 0, 1})
	if err := st.AddProtocolAddress(1, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: addr.WithPrefix(),
	}, stack.AddressProperties{}); err != nil {
		tb.Fatalf("AddProtocolAddress: %v", err)
	}
	st.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})

	full := tcpip.FullAddress{NIC: 1, Addr: addr, Port: 4242}
	ln, err := gonet.ListenTCP(st, full, ipv4.ProtocolNumber)
	if err != nil {
		tb.Fatalf("gonet listen: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()
	dialed, err := gonet.DialTCP(st, full, ipv4.ProtocolNumber)
	if err != nil {
		tb.Fatalf("gonet dial: %v", err)
	}
	server = <-accepted
	ln.Close()
	if server == nil {
		tb.Fatal("gonet accept failed")
	}
	tb.Cleanup(func() {
		dialed.Close()
		server.Close()
		st.Close()
	})
	return dialed, server
}

// BenchmarkBridgeCopyBulk streams 64 MiB backend → player through one
// bridgeCopy: the direction of a media response (flux writes into the bridge's
// loopback socket, the sidecar copies into the tailnet connection). "loopback"
// puts a loopback TCP socket on the tailnet side as well; "gonet" puts a real
// gVisor connection there, as tsnet does. Reports CPU per MB for the whole
// process (both ends plus the copy), so differences come from the copy loop.
func BenchmarkBridgeCopyBulk(b *testing.B) {
	b.Run("loopback", func(b *testing.B) {
		benchBridgeBulk(b, func(b *testing.B) (net.Conn, net.Conn) { return tcpPairB(b) })
	})
	b.Run("gonet", func(b *testing.B) {
		benchBridgeBulk(b, func(b *testing.B) (net.Conn, net.Conn) { return gonetPair(b) })
	})
}

func benchBridgeBulk(b *testing.B, tailnetPair func(*testing.B) (net.Conn, net.Conn)) {
	const total = 64 << 20
	chunk := bytes.Repeat([]byte{0x5A}, 256<<10)
	b.SetBytes(total)
	b.ReportAllocs()
	var cpu time.Duration
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		player, tsnetSide := tailnetPair(b)
		localSide, backend := tcpPairB(b)
		copied := make(chan struct{})
		go func() {
			bridgeCopy(tsnetSide, localSide, 10*time.Minute)
			close(copied)
		}()
		done := make(chan int64, 1)
		go func() {
			n, _ := io.Copy(io.Discard, player)
			done <- n
		}()
		start := cpuTime()
		b.StartTimer()
		for sent := 0; sent < total; sent += len(chunk) {
			if _, err := backend.Write(chunk); err != nil {
				b.Fatal(err)
			}
		}
		backend.Close()
		if n := <-done; n != total {
			b.Fatalf("player got %d of %d bytes", n, total)
		}
		b.StopTimer()
		cpu += cpuTime() - start
		player.Close()
		<-copied
	}
	b.ReportMetric(cpu.Seconds()*1000/float64(b.N)/(float64(total)/1e6), "cpu-ms/MB")
}
