package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"tailscale.com/client/local"
)

type testLocalTransport func(*http.Request) (*http.Response, error)

func (f testLocalTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func serveOnLoopback(t *testing.T, data proxyAddData) (*shim, *proxyEntry, string) {
	t.Helper()
	s := newTestShim()
	t.Cleanup(s.handleStop)
	s.setDNSName("test.example.ts.net")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	data.ListenPort = uint16(l.Addr().(*net.TCPAddr).Port)
	lc := &local.Client{OmitAuth: true, Transport: testLocalTransport(func(*http.Request) (*http.Response, error) { return nil, fmt.Errorf("anonymous test client") })}
	s.startProxyAdd(nil, lc, data, func(bool, string) (net.Listener, error) { return l, nil })
	t.Cleanup(func() { l.Close() })
	return s, waitForProxyEntry(t, s, data.ID), "http://" + l.Addr().String()
}

func updateTestRoutes(t *testing.T, s *shim, id string, routes []proxyRouteData) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"id": id, "routes": routes, "requestId": "replace"})
	if err != nil {
		t.Fatal(err)
	}
	s.handleProxyUpdate(raw)
}

func TestLiveRoutesPreserveAndRevokeStreams(t *testing.T) {
	canceled := make(chan struct{})
	advance := make(chan struct{})
	back := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, "first")
		w.(http.Flusher).Flush()
		select {
		case <-advance:
		case <-r.Context().Done():
			return
		}
		fmt.Fprintln(w, "second")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer back.Close()
	live := proxyRouteData{Prefix: "/live", TargetURL: back.URL}
	s, entry, base := serveOnLoopback(t, proxyAddData{ID: "streams", Routes: []proxyRouteData{live}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", base+"/live", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "first\n" {
		t.Fatalf("first event %q %v", line, err)
	}
	old := entry.table.Load().routes[0]
	updateTestRoutes(t, s, "streams", []proxyRouteData{live, {Prefix: "/site", Dir: t.TempDir()}})
	if entry.table.Load().byKey[old.key] != old {
		t.Fatal("unchanged route rebound")
	}
	close(advance)
	if line, err := reader.ReadString('\n'); err != nil || line != "second\n" {
		t.Fatalf("event after update %q %v", line, err)
	}
	working := entry.table.Load()
	updateTestRoutes(t, s, "streams", []proxyRouteData{live, {Prefix: "/bad", Dir: t.TempDir() + "/missing"}})
	if entry.table.Load() != working {
		t.Fatal("failed update replaced the table")
	}
	updateTestRoutes(t, s, "streams", []proxyRouteData{})
	waitForSignal(t, canceled, "removed stream cancellation")
	after, err := http.Get(base + "/live")
	if err != nil {
		t.Fatal(err)
	}
	defer after.Body.Close()
	if after.StatusCode != 404 {
		t.Fatalf("empty table returned %d", after.StatusCode)
	}
}

func TestLiveRouteClosesHijackedSocket(t *testing.T) {
	back := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer c.Close()
		fmt.Fprint(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		rw.Flush()
		io.Copy(io.Discard, c)
	}))
	defer back.Close()
	s, _, base := serveOnLoopback(t, proxyAddData{ID: "socket", Routes: []proxyRouteData{{Prefix: "/ws", TargetURL: back.URL}}})
	c, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(4 * time.Second))
	fmt.Fprint(c, "GET /ws HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(c)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("upgrade %v, %v", resp, err)
	}
	updateTestRoutes(t, s, "socket", []proxyRouteData{})
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("revocation read = %v", err)
	}
}

func TestLiveRouteTableSnapshotAndStaticRootLifetime(t *testing.T) {
	var dirs []string
	for _, v := range []string{"one", "two"} {
		d := t.TempDir()
		writeServeFile(t, d, "index.html", v)
		dirs = append(dirs, d)
	}
	version := func(i int) []proxyRouteData {
		return []proxyRouteData{{Prefix: "/", Dir: dirs[i], Headers: []proxyHeaderData{{Name: "X-Version", Value: []string{"one", "two"}[i]}}}}
	}
	s, entry, base := serveOnLoopback(t, proxyAddData{ID: "snapshot", Routes: version(0)})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 24 {
				r, err := http.Get(base)
				if err != nil {
					t.Error(err)
					return
				}
				body, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil || r.StatusCode != 200 || string(body) != r.Header.Get("X-Version") {
					t.Errorf("mixed response: %d %q %q %v", r.StatusCode, body, r.Header.Get("X-Version"), err)
					return
				}
			}
		}()
	}
	for i := range 32 {
		updateTestRoutes(t, s, "snapshot", version(i%2))
	}
	wg.Wait()
	root := newTrackingStaticRoot()
	setStaticRootOpener(t, func(string) (staticRoot, error) { return root, nil })
	updateTestRoutes(t, s, "snapshot", []proxyRouteData{{Prefix: "/held", Dir: t.TempDir()}})
	held, _ := entry.match("/held")
	if held == nil {
		t.Fatal("missing static route")
	}
	updateTestRoutes(t, s, "snapshot", []proxyRouteData{})
	if root.closeCalls.Load() != 0 {
		t.Fatal("closed in-flight directory")
	}
	held.releaseFile()
	if root.closeCalls.Load() != 1 {
		t.Fatal("retired directory wasn't closed exactly once")
	}
}
