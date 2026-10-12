package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

const udpRegister = "TRUFFLE_UDP_REGISTER"
const udpRegistered = "TRUFFLE_UDP_READY"

// The per-binding capability never crosses JSON or logs. Both processes derive
// it from their private bridge session key and the unique request ID.
func udpRelayToken(session []byte, id string) []byte {
	mac := hmac.New(sha256.New, session)
	mac.Write([]byte("truffle-udp-v6\x00"))
	mac.Write([]byte(id))
	return mac.Sum(nil)
}

type udpRelay struct {
	mu        sync.Mutex
	port      uint16
	tsnetConn net.PacketConn
	localConn net.PacketConn
	cancel    context.CancelFunc
	closed    bool
}

func (r *udpRelay) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.cancel()
	if r.tsnetConn != nil {
		r.tsnetConn.Close()
	}
	if r.localConn != nil {
		r.localConn.Close()
	}
}
func (r *udpRelay) attach(tsnetPC, localPC net.PacketConn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		tsnetPC.Close()
		localPC.Close()
		return false
	}
	r.tsnetConn = tsnetPC
	r.localConn = localPC
	r.port = uint16(tsnetPC.LocalAddr().(*net.UDPAddr).Port)
	return true
}

func (s *shim) handleListenPacket(data json.RawMessage) {
	var d listenPacketData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("invalid listenPacket data: %v", err))
		return
	}
	if d.RelayVersion != 6 || d.RequestID == "" {
		s.sendErrorFor("LISTEN_PACKET_ERROR", "UDP requires authenticated relay protocol v6", d.RequestID)
		return
	}
	srv := s.getServer()
	if srv == nil {
		s.sendErrorFor("NOT_RUNNING", "node not running", d.RequestID)
		return
	}
	session, _ := s.bridgeParams()
	if len(session) != 32 {
		s.sendErrorFor("LISTEN_PACKET_ERROR", "missing bridge session", d.RequestID)
		return
	}
	token := udpRelayToken(session, d.RequestID)
	ctx, cancel := context.WithCancel(s.lifecycleCtx())
	relay := &udpRelay{cancel: cancel}
	// Reserve the ID before starting asynchronous work. A cancellation arriving
	// before ListenPacket finishes can close this same lease, not a reused port.
	s.udpRelayMu.Lock()
	if _, exists := s.udpRelays[d.RequestID]; exists {
		s.udpRelayMu.Unlock()
		cancel()
		s.sendErrorFor("LISTEN_PACKET_ERROR", "duplicate UDP binding ID", d.RequestID)
		return
	}
	s.udpRelays[d.RequestID] = relay
	s.udpRelayMu.Unlock()
	go func() {
		defer s.recoverPanic("handleListenPacket")
		defer relay.close()
		defer func() {
			s.udpRelayMu.Lock()
			if s.udpRelays[d.RequestID] == relay {
				delete(s.udpRelays, d.RequestID)
			}
			s.udpRelayMu.Unlock()
		}()
		fail := func(err error) { s.sendErrorFor("LISTEN_PACKET_ERROR", err.Error(), d.RequestID) }
		lc, err := srv.LocalClient()
		if err != nil {
			fail(err)
			return
		}
		statusCtx, stop := context.WithTimeout(ctx, statusTimeout)
		status, err := lc.StatusWithoutPeers(statusCtx)
		stop()
		if err != nil {
			fail(err)
			return
		}
		var ip string
		for _, addr := range status.TailscaleIPs {
			if addr.Is4() {
				ip = addr.String()
				break
			}
		}
		if ip == "" {
			fail(fmt.Errorf("UDP relay requires a Tailscale IPv4 address"))
			return
		}
		if ctx.Err() != nil {
			return
		}
		pc, err := srv.ListenPacket("udp", net.JoinHostPort(ip, fmt.Sprint(d.Port)))
		if err != nil {
			fail(err)
			return
		}
		local, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			pc.Close()
			fail(err)
			return
		}
		if !relay.attach(pc, local) {
			return
		}
		// Unclaimed relays are bounded even if the caller vanishes before it
		// receives the JSON response or its runtime shuts down during binding.
		local.SetReadDeadline(time.Now().Add(10 * time.Second))
		s.sendEvent("tsnet:listeningPacket", listeningPacketData{Port: relay.port, LocalPort: uint16(local.LocalAddr().(*net.UDPAddr).Port), RequestID: d.RequestID})
		runUDPRelay(ctx, relay, token)
	}()
}

// runUDPRelay authenticates one loopback source, pins it for this lease, then
// forwards datagrams. It never injects diagnostic payloads into application IO.
func runUDPRelay(ctx context.Context, r *udpRelay, token []byte) {
	stop := context.AfterFunc(ctx, r.close)
	defer stop()
	local, pc := r.localConn, r.tsnetConn
	buf := make([]byte, 65536)
	var owner net.Addr
	for owner == nil {
		n, sender, err := local.ReadFrom(buf)
		if err != nil {
			return
		}
		addr, ok := sender.(*net.UDPAddr)
		if !ok || !addr.IP.IsLoopback() {
			continue
		}
		if n != len(udpRegister)+32 || string(buf[:len(udpRegister)]) != udpRegister || subtle.ConstantTimeCompare(buf[len(udpRegister):n], token) != 1 {
			continue
		}
		owner = sender
	}
	local.SetReadDeadline(time.Time{})
	if _, err := local.WriteTo([]byte(udpRegistered), owner); err != nil {
		return
	}
	// Close both directions when either ends. Cancellation also interrupts IO
	// through relay.close(), including an idle ReadFrom.
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer r.close()
		incoming := make([]byte, 65536)
		framed := make([]byte, 65536+6)
		for {
			n, remote, err := pc.ReadFrom(incoming)
			if err != nil {
				return
			}
			addr, ok := remote.(*net.UDPAddr)
			if !ok {
				continue
			}
			ip := addr.IP.To4()
			if ip == nil {
				continue
			}
			copy(framed[:4], ip)
			binary.BigEndian.PutUint16(framed[4:6], uint16(addr.Port))
			copy(framed[6:], incoming[:n])
			if _, err := local.WriteTo(framed[:n+6], owner); err != nil {
				return
			}
		}
	}()
	defer func() { r.close(); <-done }()
	for {
		n, sender, err := local.ReadFrom(buf)
		if err != nil {
			return
		}
		if sender.String() != owner.String() {
			continue
		}
		if n >= len(udpRegister) && string(buf[:len(udpRegister)]) == udpRegister {
			continue
		}
		if n < 6 {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		// Copy an IPv4-sized address; net.IPv4 creates a mapped IPv6 value
		// that gVisor's IPv4 PacketConn cannot route.
		addr := &net.UDPAddr{IP: net.IP(append([]byte(nil), buf[:4]...)), Port: int(binary.BigEndian.Uint16(buf[4:6]))}
		// An unreachable destination is per-datagram and does not kill the relay.
		if _, err := pc.WriteTo(buf[6:n], addr); err != nil && ctx.Err() != nil {
			return
		}
	}
}

func (s *shim) handleUnlistenPacket(data json.RawMessage) {
	var d struct {
		RelayID   string `json:"relayId"`
		RequestID string `json:"requestId,omitempty"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("UNLISTEN_PACKET_ERROR", err.Error())
		return
	}
	if d.RelayID == "" {
		s.sendErrorFor("UNLISTEN_PACKET_ERROR", "relayId required", d.RequestID)
		return
	}
	s.udpRelayMu.Lock()
	relay := s.udpRelays[d.RelayID]
	delete(s.udpRelays, d.RelayID)
	s.udpRelayMu.Unlock()
	if relay != nil {
		relay.close()
	}
	// Idempotent: retries, timed-out binds, and late Drop cleanup cannot close
	// a different socket that happens to have reused the same UDP port.
	s.sendEvent("tsnet:unlistenedPacket", map[string]string{"relayId": d.RelayID, "requestId": d.RequestID})
}
