// Public-only HTTP serving on the node's Tailscale Funnel (protocol 15).
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/tailcfg/nodecap"
	"tailscale.com/tsnet"
)

const funnelConnLimit = 256
const funnelHeaderLimit = 64 << 10

var funnelPorts = []uint16{443, 8443, 10000}

type funnelStatus struct {
	State     string   `json:"state"`
	Ports     []uint16 `json:"ports"`
	EnableURL string   `json:"enableUrl,omitempty"`
	Text      string   `json:"text,omitempty"`
	Reason    string   `json:"reason,omitempty"`
	RequestID string   `json:"requestId,omitempty"`
}

func classifyFunnel(st *ipnstate.Status, control *tailcfg.QueryFeatureResponse, queryErr error) funnelStatus {
	result := funnelStatus{Ports: []uint16{}}
	if st == nil || st.Self == nil {
		result.State, result.Reason = "unavailable", "node status is unavailable"
		return result
	}
	for _, port := range funnelPorts {
		if ipn.CheckFunnelPort(port, st.Self) == nil {
			result.Ports = append(result.Ports, port)
		}
	}
	switch {
	case !st.Self.HasCap(nodecap.HTTPS) || len(st.CertDomains) == 0:
		result.State = "needsHttps"
	case !st.Self.HasCap(nodecap.Funnel):
		result.State = "needsGrant"
	case len(result.Ports) == 0:
		result.State, result.Reason = "unavailable", "no granted Funnel ports"
	default:
		result.State = "ready"
		return result
	}
	if queryErr != nil {
		result.State, result.Reason = "unavailable", queryErr.Error()
	} else if control != nil {
		result.EnableURL, result.Text = control.URL, control.Text
		if !control.Complete && !control.ShouldWait && control.URL == "" && control.Text != "" {
			result.State, result.Reason = "unavailable", control.Text
		}
	}
	return result
}

func readFunnelStatus(ctx context.Context, lc *local.Client) (funnelStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return funnelStatus{}, err
	}
	result := classifyFunnel(st, nil, nil)
	if result.State == "ready" {
		return result, nil
	}
	feature, err := lc.QueryFeature(ctx, "funnel")
	return classifyFunnel(st, feature, err), nil
}

func (s *shim) handleFunnelStatus(raw json.RawMessage) {
	id := requestIDOf(raw)
	srv := s.getServer()
	if srv == nil {
		s.sendErrorFor("NOT_RUNNING", "node not running", id)
		return
	}
	lc, err := s.localClientFor(srv)
	if err != nil {
		s.sendErrorFor("FUNNEL_UNAVAILABLE", err.Error(), id)
		return
	}
	lifecycle := s.lifecycleCtx()
	go func() {
		defer s.recoverPanic("funnel:status")
		st, err := readFunnelStatus(lifecycle, lc)
		if err != nil {
			s.sendErrorFor("FUNNEL_UNAVAILABLE", err.Error(), id)
			return
		}
		st.RequestID = id
		s.sendEvent("funnel:statusResult", st)
	}()
}

func (st funnelStatus) equal(other funnelStatus) bool {
	return st.State == other.State && st.EnableURL == other.EnableURL && st.Text == other.Text && st.Reason == other.Reason && slices.Equal(st.Ports, other.Ports)
}
func (s *shim) watchFunnelChanges(ctx context.Context, lc *local.Client, changes <-chan struct{}) {
	defer s.recoverPanic("funnel:changed")
	var last funnelStatus
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
		}
		st, err := readFunnelStatus(ctx, lc)
		if err != nil || ctx.Err() != nil {
			continue
		}
		if !st.equal(last) {
			last = st
			s.sendEvent("funnel:changed", st)
		}
	}
}

func validatePublicServe(d proxyAddData) *proxyAddFailure {
	if !d.Funnel {
		return nil
	}
	switch {
	case d.Tls != nil && !*d.Tls:
		return &proxyAddFailure{"INVALID_FUNNEL", "Funnel requires TLS"}
	case !slices.Contains(funnelPorts, d.ListenPort):
		return &proxyAddFailure{"INVALID_FUNNEL", "Funnel requires port 443, 8443 or 10000"}
	case d.AllowNonLoopback:
		return &proxyAddFailure{"INVALID_FUNNEL", "Funnel accepts only loopback backends"}
	case len(d.Allow) > 0 || len(d.AcceptAppCaps) > 0:
		return &proxyAddFailure{"INVALID_FUNNEL", "public visitors have no tailnet login or capabilities"}
	}
	for _, r := range d.Routes {
		if len(r.Allow) > 0 {
			return &proxyAddFailure{"INVALID_FUNNEL", "a public route cannot restrict tailnet logins"}
		}
	}
	return nil
}

func (st funnelStatus) allowPort(port uint16) *proxyAddFailure {
	switch st.State {
	case "needsHttps":
		return &proxyAddFailure{"FUNNEL_HTTPS_OFF", "HTTPS certificates must be enabled"}
	case "needsGrant":
		return &proxyAddFailure{"FUNNEL_NOT_GRANTED", "this node needs a Funnel grant"}
	case "unavailable":
		if st.Reason == "no granted Funnel ports" {
			return &proxyAddFailure{"FUNNEL_PORT_NOT_ALLOWED", st.Reason}
		}
		return &proxyAddFailure{"FUNNEL_UNAVAILABLE", st.Reason}
	}
	if !slices.Contains(st.Ports, port) {
		return &proxyAddFailure{"FUNNEL_PORT_NOT_ALLOWED", fmt.Sprintf("port %d is outside this node's grant", port)}
	}
	return nil
}

// Truffle owns the tsnet node's three public listener marks. Preserve any
// other host and any private Serve configuration when recovering from a crash.
func clearFunnelMarks(ctx context.Context, lc *local.Client, domains []string, ports []uint16) error {
	sc, err := lc.GetServeConfig(ctx)
	if err != nil || sc == nil {
		return err
	}
	changed := false
	for _, domain := range domains {
		for _, port := range ports {
			key := ipn.HostPort(fmt.Sprintf("%s:%d", domain, port))
			if sc.AllowFunnel[key] {
				delete(sc.AllowFunnel, key)
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	return lc.SetServeConfig(ctx, sc)
}
func (s *shim) recoverFunnel(ctx context.Context, lc *local.Client, domains []string) error {
	s.funnelMu.Lock()
	defer s.funnelMu.Unlock()
	if s.funnelRecovered || len(domains) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	if err := clearFunnelMarks(ctx, lc, domains, funnelPorts); err != nil {
		return err
	}
	s.funnelRecovered = true
	return nil
}

type funnelAddrKey struct{}

func funnelSource(ctx context.Context, conn net.Conn) context.Context {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		switch c := tlsConn.NetConn().(type) {
		case *limitedFunnelConn:
			return context.WithValue(ctx, funnelAddrKey{}, c.visitor)
		case *ipn.FunnelConn:
			return context.WithValue(ctx, funnelAddrKey{}, c.Src.Addr().Unmap())
		}
	}
	return ctx
}
func setFunnelHeaders(h http.Header, visitor netip.Addr) {
	h.Del("Forwarded")
	h.Del("X-Forwarded-Host")
	h.Del("X-Real-IP")
	h.Set("Tailscale-Funnel-Request", "?1")
	h.Set("X-Forwarded-For", visitor.String())
	h.Set("X-Forwarded-Proto", "https")
}

// Limit raw connections below TLS. net/http must see *tls.Conn to handshake
// and negotiate HTTP/2. Its ordinary Close and hijacked Close both return the
// semaphore slot. Tailscale's ListenFunnel returns an unhandshaken TLS conn.
type limitedFunnelConn struct {
	net.Conn
	visitor netip.Addr
	once    sync.Once
	slots   chan struct{}
}

func (c *limitedFunnelConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { <-c.slots })
	return err
}

type funnelListener struct {
	net.Listener
	cfg      *tls.Config
	slots    chan struct{}
	done     chan struct{}
	once     sync.Once
	configMu *sync.Mutex
	closeErr error
}

func (l *funnelListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	tc, ok := c.(*tls.Conn)
	if !ok {
		c.Close()
		<-l.slots
		return nil, errors.New("Funnel listener returned a non-TLS connection")
	}
	fc, ok := tc.NetConn().(*ipn.FunnelConn)
	if !ok || !fc.Src.IsValid() {
		c.Close()
		<-l.slots
		return nil, errors.New("Funnel connection missing visitor address")
	}
	return tls.Server(&limitedFunnelConn{Conn: fc, visitor: fc.Src.Addr().Unmap(), slots: l.slots}, l.cfg), nil
}
func (l *funnelListener) Close() error {
	l.once.Do(func() { close(l.done); l.configMu.Lock(); defer l.configMu.Unlock(); l.closeErr = l.Listener.Close() })
	return l.closeErr
}

func (s *shim) listenFunnel(ctx context.Context, srv *tsnet.Server, lc *local.Client, addr string, port uint16) (net.Listener, error) {
	st, err := readFunnelStatus(ctx, lc)
	if err != nil {
		return nil, &proxyAddFailure{"FUNNEL_UNAVAILABLE", err.Error()}
	}
	if failure := st.allowPort(port); failure != nil {
		return nil, failure
	}
	domain := s.getDNSName()
	if err := s.recoverFunnel(ctx, lc, []string{domain}); err != nil {
		return nil, err
	}
	s.funnelMu.Lock()
	defer s.funnelMu.Unlock()
	cfg := &tls.Config{GetCertificate: lc.GetCertificate, NextProtos: []string{"h2", "http/1.1"}, MinVersion: tls.VersionTLS12}
	sc, err := lc.GetServeConfig(ctx)
	if err != nil {
		return nil, err
	}
	hp := ipn.HostPort(fmt.Sprintf("%s:%d", domain, port))
	alreadyMarked := sc != nil && sc.AllowFunnel[hp]
	ln, err := srv.ListenFunnel("tcp", addr, tsnet.FunnelOnly(), tsnet.FunnelTLSConfig(cfg))
	if err != nil {
		if !alreadyMarked {
			err = errors.Join(err, clearFunnelMarks(ctx, lc, []string{domain}, []uint16{port}))
		}
		return nil, err
	}
	return &funnelListener{Listener: ln, cfg: cfg, slots: make(chan struct{}, funnelConnLimit), done: make(chan struct{}), configMu: &s.funnelMu}, nil
}

func (f *proxyAddFailure) Error() string { return f.message }

type funnelErrorLog struct{ handshakes atomic.Uint64 }

func (l *funnelErrorLog) Write(p []byte) (int, error) {
	if strings.HasPrefix(string(p), "http: TLS handshake error") {
		l.handshakes.Add(1)
		return len(p), nil
	}
	return log.Writer().Write(p)
}
