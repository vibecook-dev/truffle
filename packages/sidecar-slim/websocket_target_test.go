package main

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// Drive both paths through the real proxy listener. A discovery GET succeeding
// must not hide a WebSocket upgrade that loses the backend's private path.
func TestProxyWebSocketPreservesTargetPath(t *testing.T) {
	for _, tc := range []struct {
		name, targetSuffix, prefix, requestURI, wantURI string
		strip, legacy                                   bool
	}{
		{"private root", "/t/test-secret", "/", "/", "/t/test-secret/", false, false},
		{"escaped path and queries", "/t/test-secret?target=1", "/", "/hello%2Fworld?client=2", "/t/test-secret/hello%2Fworld?target=1&client=2", false, false},
		{"terminal mount", "", "/cell/abc", "/cell/abc/control?client=2", "/control?client=2", true, false},
		{"legacy target", "", "/", "/control?client=2", "/control?client=2", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, upgrade := range []bool{false, true} {
				name := "HTTP"
				if upgrade {
					name = "WebSocket"
				}
				t.Run(name, func(t *testing.T) {
					type seenRequest struct{ uri, host, login string }
					seen := make(chan seenRequest, 1)
					backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						seen <- seenRequest{r.RequestURI, r.Host, r.Header.Get(hdrUserLogin)}
						if upgrade {
							conn, rw, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Errorf("backend hijack: %v", err)
								return
							}
							defer conn.Close()
							_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
							_ = rw.Flush()
							return
						}
						w.WriteHeader(http.StatusOK)
					}))
					defer backend.Close()
					target, err := url.Parse(backend.URL)
					if err != nil {
						t.Fatal(err)
					}
					listener, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					s := newTestShim()
					defer s.cancel()
					defer s.handleStop()
					s.setDNSName("test.tail-example.ts.net")
					tlsOn := false
					data := proxyAddData{
						ID: "target-path", Name: "target-path", Tls: &tlsOn,
						ListenPort: uint16(listener.Addr().(*net.TCPAddr).Port),
						Allow:      []string{"alice@example.com"},
						Routes:     []proxyRouteData{{Prefix: tc.prefix, TargetURL: backend.URL + tc.targetSuffix, StripPrefix: tc.strip}},
					}
					if tc.legacy {
						port, _ := strconv.Atoi(target.Port())
						data.Routes = nil
						data.TargetHost, data.TargetPort, data.TargetScheme = target.Hostname(), uint16(port), "http"
					}
					s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) { return listener, nil })
					waitForProxyEntry(t, s, data.ID)
					conn, err := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					// Supply the verified identity at the existing WhoIs cache seam.
					s.storeCachedIdentity(conn.LocalAddr().String(), peerIdentityData{LoginName: "alice@example.com"}, time.Now())
					req, err := http.NewRequest(http.MethodGet, "http://phone.tail-example.ts.net"+tc.requestURI, nil)
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set(hdrUserLogin, "forged@example.com")
					if upgrade {
						req.Header.Set("Connection", "Upgrade")
						req.Header.Set("Upgrade", "websocket")
					}
					if err := req.Write(conn); err != nil {
						t.Fatal(err)
					}
					res, err := http.ReadResponse(bufio.NewReader(conn), req)
					if err != nil {
						t.Fatal(err)
					}
					defer res.Body.Close()
					wantStatus := http.StatusOK
					if upgrade {
						wantStatus = http.StatusSwitchingProtocols
					} else {
						_, _ = io.Copy(io.Discard, res.Body)
					}
					if res.StatusCode != wantStatus {
						t.Fatalf("status = %d, want %d", res.StatusCode, wantStatus)
					}
					select {
					case got := <-seen:
						if got.uri != tc.wantURI {
							t.Errorf("backend URI = %q, want %q", got.uri, tc.wantURI)
						}
						if got.host != target.Host {
							t.Errorf("backend Host = %q, want %q", got.host, target.Host)
						}
						if got.login != "alice@example.com" {
							t.Errorf("backend login = %q, want verified identity", got.login)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("backend did not receive the request")
					}
				})
			}
		})
	}
}
