package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/tailcfg/nodecap"
)

func TestFunnelSharedVectors(t *testing.T) {
	b, err := os.ReadFile("../../crates/truffle-core/src/proxy/funnel_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		proxyAddData
		Valid bool
	}
	if err = json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			failure := validatePublicServe(c.proxyAddData)
			if (failure == nil) != c.Valid {
				t.Fatalf("valid=%v failure=%v", c.Valid, failure)
			}
		})
	}
}

func TestFunnelStatusAndRefusals(t *testing.T) {
	const ports = nodecap.Cap("https://tailscale.com/cap/funnel-ports?ports=443,8443")
	feature := &tailcfg.QueryFeatureResponse{URL: "https://example.test/enable", Text: "Approve sharing", ShouldWait: true}
	for _, tc := range []struct {
		name        string
		domains     []string
		caps        []nodecap.Cap
		control     *tailcfg.QueryFeatureResponse
		err         error
		state, code string
	}{
		{"HTTPS off", nil, []nodecap.Cap{nodecap.Funnel, ports}, feature, nil, "needsHttps", "FUNNEL_HTTPS_OFF"},
		{"no grant", []string{"node.ts.net"}, []nodecap.Cap{nodecap.HTTPS, ports}, feature, nil, "needsGrant", "FUNNEL_NOT_GRANTED"},
		{"ready", []string{"node.ts.net"}, []nodecap.Cap{nodecap.HTTPS, nodecap.Funnel, ports}, nil, nil, "ready", ""},
		{"no port", []string{"node.ts.net"}, []nodecap.Cap{nodecap.HTTPS, nodecap.Funnel}, nil, nil, "unavailable", "FUNNEL_PORT_NOT_ALLOWED"},
		{"unsupported control", nil, nil, nil, errors.New("no feature endpoint"), "unavailable", "FUNNEL_UNAVAILABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caps := tailcfg.NodeCapMap{}
			for _, cap := range tc.caps {
				caps[cap] = nil
			}
			st := classifyFunnel(&ipnstate.Status{CertDomains: tc.domains, Self: &ipnstate.PeerStatus{CapMap: caps}}, tc.control, tc.err)
			if st.State != tc.state {
				t.Fatalf("status = %+v", st)
			}
			if tc.control != nil && (st.EnableURL != feature.URL || st.Text != feature.Text) {
				t.Fatal("lost control instructions")
			}
			if fail := st.allowPort(443); fail == nil && tc.code != "" || fail != nil && fail.code != tc.code {
				t.Fatalf("port 443: %v", fail)
			}
			if st.State == "ready" {
				if !slices.Equal(st.Ports, []uint16{443, 8443}) || st.allowPort(10000).code != "FUNNEL_PORT_NOT_ALLOWED" {
					t.Fatalf("unexpected grant: %+v", st)
				}
			}
		})
	}
}

type testFunnelTLS struct {
	net.Listener
	cfg *tls.Config
}

func (l testFunnelTLS) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return tls.Server(&ipn.FunnelConn{Conn: c, Src: netip.MustParseAddrPort("192.0.2.45:1234")}, l.cfg), nil
}

func TestPublicListenerIdentityAndLimits(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(r.Header) }))
	defer backend.Close()
	certs := httptest.NewTLSServer(http.NotFoundHandler())
	defer certs.Close()
	cfg := &tls.Config{Certificates: certs.TLS.Certificates, NextProtos: []string{"h2", "http/1.1"}}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	l := &funnelListener{Listener: testFunnelTLS{raw, cfg}, cfg: cfg, slots: make(chan struct{}, funnelConnLimit), done: make(chan struct{}), configMu: &mu}
	s := newTestShim()
	defer s.handleStop()
	s.setDNSName("test.example.ts.net")
	// Nil LocalAPI makes any accidental WhoIs call fail immediately.
	d := proxyAddData{ID: "public", ListenPort: 443, Funnel: true, Routes: []proxyRouteData{{Prefix: "/", TargetURL: backend.URL}}}
	s.startProxyAdd(nil, nil, d, func(bool, string) (net.Listener, error) { return l, nil })
	entry := waitForProxyEntry(t, s, d.ID)
	if entry.server.MaxHeaderBytes != 64<<10 || cap(l.slots) != 256 {
		t.Fatal("unexpected public limits")
	}
	transport := certs.Client().Transport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = true
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", "https://"+raw.Addr().String()+"/", nil)
	for _, k := range []string{"Tailscale-User-Login", "Tailscale-Node-Id", "Tailscale-Funnel-Request", "X-Forwarded-For", "Forwarded", "X-Real-IP"} {
		req.Header.Set(k, "forged")
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var h http.Header
	if err = json.NewDecoder(res.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	if res.ProtoMajor != 2 || h.Get("Tailscale-Funnel-Request") != "?1" || h.Get("X-Forwarded-For") != "192.0.2.45" || h.Get("X-Forwarded-Proto") != "https" {
		t.Fatalf("%s headers: %v", res.Proto, h)
	}
	for _, k := range []string{"Tailscale-User-Login", "Tailscale-Node-Id", "Forwarded", "X-Real-IP"} {
		if h.Get(k) != "" {
			t.Fatalf("forwarded %s", k)
		}
	}
	if len(s.identityCache) != 0 {
		t.Fatal("public request populated WhoIs cache")
	}
	old := entry.table.Load()
	updateTestRoutes(t, s, d.ID, []proxyRouteData{{Prefix: "/", TargetURL: backend.URL, Allow: []string{"*"}}})
	if entry.table.Load() != old {
		t.Fatal("public update accepted a tailnet login gate")
	}
	// The same port remains available on the private plane.
	privateLn := newTrackingListener()
	s.startProxyAdd(nil, nil, proxyAddData{ID: "private", ListenPort: 443, Routes: []proxyRouteData{}}, func(bool, string) (net.Listener, error) { return privateLn, nil })
	waitForProxyEntry(t, s, "private")
}

func TestFunnelConnectionCapAndClose(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	l := &funnelListener{Listener: testFunnelTLS{base, &tls.Config{}}, cfg: &tls.Config{}, slots: make(chan struct{}, 1), done: make(chan struct{}), configMu: &mu}
	defer l.Close()
	c1, err := net.Dial("tcp", base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	a1, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := net.Dial("tcp", base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	ch := make(chan net.Conn, 1)
	go func() { c, _ := l.Accept(); ch <- c }()
	select {
	case <-ch:
		t.Fatal("cap allowed a second open socket")
	case <-time.After(30 * time.Millisecond):
	}
	a1.Close()
	select {
	case a2 := <-ch:
		a2.Close()
	case <-time.After(time.Second):
		t.Fatal("slot was not returned")
	}
	logger := &funnelErrorLog{}
	logger.Write([]byte("http: TLS handshake error from test"))
	if logger.handshakes.Load() != 1 {
		t.Fatal("handshake was not counted")
	}
}

func TestFunnelChangesDeduplicated(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var writes bytes.Buffer
	s := newTestShim()
	defer s.cancel()
	s.writer = json.NewEncoder(&writes)
	kicks := make(chan struct{})
	polled := make(chan struct{}, 4)
	var mu sync.Mutex
	var allow bool
	lc := &local.Client{OmitAuth: true, Transport: testLocalTransport(func(r *http.Request) (*http.Response, error) {
		var data any
		if strings.Contains(r.URL.Path, "query-feature") {
			data = tailcfg.QueryFeatureResponse{URL: "https://example.test"}
		} else {
			caps := tailcfg.NodeCapMap{nodecap.HTTPS: nil, nodecap.Cap("https://tailscale.com/cap/funnel-ports?ports=443"): nil}
			mu.Lock()
			if allow {
				caps[nodecap.Funnel] = nil
			}
			mu.Unlock()
			data = ipnstate.Status{CertDomains: []string{"node.ts.net"}, Self: &ipnstate.PeerStatus{CapMap: caps}}
			polled <- struct{}{}
		}
		b, _ := json.Marshal(data)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(b)), Header: make(http.Header), Request: r}, nil
	})}
	done := make(chan struct{})
	go func() { defer close(done); s.watchFunnelChanges(ctx, lc, kicks) }()
	kicks <- struct{}{}
	<-polled
	kicks <- struct{}{}
	<-polled
	mu.Lock()
	allow = true
	mu.Unlock()
	kicks <- struct{}{}
	<-polled
	// Handshake with the next receive so the preceding event is fully written.
	kicks <- struct{}{}
	stop()
	<-done
	if got := strings.Count(writes.String(), "funnel:changed"); got != 2 {
		t.Fatalf("got %d changes: %s", got, writes.String())
	}
}
