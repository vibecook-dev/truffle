package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestUDPRejectsLoginOnlyProtocolV5(t *testing.T) {
	var buf bytes.Buffer
	s := &shim{writer: json.NewEncoder(&buf)}
	s.handleListenPacket(json.RawMessage(`{"port":0,"relayVersion":5,"requestId":"legacy"}`))
	if !bytes.Contains(buf.Bytes(), []byte("LISTEN_PACKET_ERROR")) || !bytes.Contains(buf.Bytes(), []byte("protocol v6")) {
		t.Fatalf("legacy relay must be rejected before starting a listener: %s", buf.String())
	}
}

func udpTestConn(t *testing.T) net.PacketConn {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}
func udpTestRead(t *testing.T, pc net.PacketConn) []byte {
	t.Helper()
	pc.SetReadDeadline(time.Now().Add(time.Second))
	var buf [2048]byte
	n, _, err := pc.ReadFrom(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buf[:n]...)
}
func udpTestSend(t *testing.T, pc net.PacketConn, payload []byte, to net.Addr) {
	t.Helper()
	if _, err := pc.WriteTo(payload, to); err != nil {
		t.Fatal(err)
	}
}
func TestUDPRelayCapabilityFixture(t *testing.T) {
	token := udpRelayToken(bytes.Repeat([]byte{17}, 32), "bind-123")
	if hex.EncodeToString(token) != "0afef70c2ee12a62eaf433a58ba7588c38e639c34c5fabf9efcc90b1044c8aec" {
		t.Fatal("Rust/Go capability mismatch")
	}
	if bytes.Equal(token, udpRelayToken(bytes.Repeat([]byte{17}, 32), "bind-124")) {
		t.Fatal("capability reused between bindings")
	}
}
func TestUDPRelayAuthenticatesAndPinsOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pc, local, owner, attacker, remote := udpTestConn(t), udpTestConn(t), udpTestConn(t), udpTestConn(t), udpTestConn(t)
	relay := &udpRelay{cancel: cancel}
	relay.attach(pc, local)
	token := udpRelayToken(bytes.Repeat([]byte{17}, 32), "binding")
	done := make(chan struct{})
	go func() { defer close(done); runUDPRelay(ctx, relay, token) }()
	t.Cleanup(func() {
		relay.close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("relay goroutines leaked")
		}
	})
	udpTestSend(t, attacker, []byte(udpRegister), local.LocalAddr())
	udpTestSend(t, attacker, append([]byte(udpRegister), make([]byte, 32)...), local.LocalAddr())
	udpTestSend(t, owner, append([]byte(udpRegister), token...), local.LocalAddr())
	if got := udpTestRead(t, owner); string(got) != udpRegistered {
		t.Fatalf("bad ACK: %q", got)
	}
	addr := remote.LocalAddr().(*net.UDPAddr)
	framed := append([]byte(nil), addr.IP.To4()...)
	framed = binary.BigEndian.AppendUint16(framed, uint16(addr.Port))
	framed = append(framed, []byte("payload")...)
	// A replay of even the correct capability cannot replace a bound owner.
	udpTestSend(t, attacker, append([]byte(udpRegister), token...), local.LocalAddr())
	udpTestSend(t, attacker, framed, local.LocalAddr())
	udpTestSend(t, owner, framed, local.LocalAddr())
	if got := udpTestRead(t, remote); string(got) != "payload" {
		t.Fatalf("bad payload: %q", got)
	}
	remote.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
	if _, _, err := remote.ReadFrom(make([]byte, 128)); err == nil {
		t.Fatal("attacker forwarded an extra packet")
	}
	udpTestSend(t, remote, []byte("response"), pc.LocalAddr())
	if got := udpTestRead(t, owner); len(got) < 6 || !bytes.Equal(got[6:], []byte("response")) {
		t.Fatalf("bad inbound payload %q", got)
	}
	attacker.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
	if _, _, err := attacker.ReadFrom(make([]byte, 128)); err == nil {
		t.Fatal("attacker stole inbound delivery")
	}
}
func TestUDPRelayCancelledBeforeAttachClosesBothSockets(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	relay := &udpRelay{cancel: cancel}
	relay.close()
	pc, local := udpTestConn(t), udpTestConn(t)
	if relay.attach(pc, local) {
		t.Fatal("cancelled binding became live")
	}
	for _, conn := range []net.PacketConn{pc, local} {
		if _, err := conn.WriteTo([]byte("x"), conn.LocalAddr()); err == nil {
			t.Fatal("cancelled socket left open")
		}
	}
}
func TestUDPStaleReleaseCannotCloseReplacementBinding(t *testing.T) {
	shim := newTestShim()
	defer shim.cancel()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	replacement := &udpRelay{cancel: cancel}
	replacement.attach(udpTestConn(t), udpTestConn(t))
	defer replacement.close()
	shim.udpRelays["new-binding"] = replacement
	shim.handleUnlistenPacket(json.RawMessage(`{"relayId":"old-binding"}`))
	if replacement.closed || shim.udpRelays["new-binding"] != replacement {
		t.Fatal("stale release closed replacement")
	}
	shim.handleUnlistenPacket(json.RawMessage(`{"relayId":"new-binding"}`))
	if !replacement.closed || len(shim.udpRelays) != 0 {
		t.Fatal("current binding not released")
	}
}
