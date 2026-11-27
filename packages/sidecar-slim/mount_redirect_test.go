package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBareDirectoryMountRedirect(t *testing.T) {
	dir := t.TempDir()
	writeServeFile(t, dir, "index.html", "app")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer backend.Close()
	s := newTestShim()
	defer s.handleStop()
	s.setDNSName("node.example.ts.net")
	data := proxyAddData{ID: "mounts", Routes: []proxyRouteData{
		{Prefix: "/p", Dir: dir, Headers: []proxyHeaderData{{Name: "Referrer-Policy", Value: "no-referrer"}}},
		{Prefix: "/a b", Dir: dir}, {Prefix: "//other.test", Dir: dir}, {Prefix: "/", Dir: dir},
		{Prefix: "/api", TargetURL: backend.URL},
	}}
	l := newTrackingListener()
	s.startProxyAdd(nil, nil, data, func(bool, string) (net.Listener, error) { return l, nil })
	handler := waitForProxyEntry(t, s, data.ID).server.Handler
	for _, tc := range []struct {
		method, path string
		code         int
		location     string
	}{
		{"GET", "/p?key=a%2Fb", 308, "/p/?key=a%2Fb"}, {"HEAD", "/p", 308, "/p/"},
		{"GET", "/a%20b?", 308, "/a%20b/?"}, {"GET", "//other.test", 308, "/other.test/"},
		{"GET", "/p/", 200, ""}, {"GET", "/", 200, ""}, {"GET", "/api", 204, ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "http://node.example"+tc.path, nil)
			s.storeCachedIdentity(r.RemoteAddr, peerIdentityData{}, time.Now())
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.code || w.Header().Get("Location") != tc.location {
				t.Fatalf("got %d %q; want %d %q", w.Code, w.Header().Get("Location"), tc.code, tc.location)
			}
			if r.URL.Path == "/p" && w.Header().Get("Referrer-Policy") != "no-referrer" {
				t.Fatal("redirect lost route policy")
			}
		})
	}
}
