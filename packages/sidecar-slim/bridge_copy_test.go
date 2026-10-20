package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// tcpPair returns the two ends of a loopback TCP connection. Real TCP (not
// net.Pipe) so CloseWrite half-closes work exactly as on the bridge.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() {
		dialed.Close()
		server.Close()
	})
	return dialed, server
}

// bridged wires "player" <-> [tsnetSide | bridgeCopy | localSide] <-> "backend",
// the shape of a tailnet client reaching the Rust server through the sidecar.
func bridged(t *testing.T, idle time.Duration) (player, backend net.Conn, done <-chan struct{}) {
	t.Helper()
	player, tsnetSide := tcpPair(t)
	localSide, backend := tcpPair(t)
	finished := make(chan struct{})
	go func() {
		bridgeCopy(tsnetSide, localSide, idle)
		close(finished)
	}()
	return player, backend, finished
}

// The P0 regression (flux research/06 §3.4): a client sends one request and
// then only reads while the server streams for 3× the idle timeout. The quiet
// client→server direction must not be half-closed (the backend must not see
// EOF), and every streamed byte must arrive.
func TestBridgeCopyOneWayStreamOutlivesIdleTimeout(t *testing.T) {
	const idle = 150 * time.Millisecond
	player, backend, done := bridged(t, idle)

	request := []byte("GET /movie.mkv HTTP/1.1\r\nRange: bytes=0-\r\n\r\n")
	if _, err := player.Write(request); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(request))
	if _, err := io.ReadFull(backend, got); err != nil {
		t.Fatalf("backend did not receive the request: %v", err)
	}

	// Watch the backend's read side: it may only end when the test closes
	// the player at the very end, never during the stream.
	backendEOF := make(chan time.Duration, 1)
	start := time.Now()
	go func() {
		var b [1]byte
		_, err := backend.Read(b[:])
		if err == nil {
			err = errors.New("unexpected byte")
		}
		backendEOF <- time.Since(start)
	}()

	// Player reads everything the backend streams.
	received := make(chan []byte, 1)
	go func() {
		all, _ := io.ReadAll(player)
		received <- all
	}()

	// Stream 4 KiB every 10 ms for 3× the idle timeout.
	var sent bytes.Buffer
	chunk := bytes.Repeat([]byte{0xAB}, 4096)
	for time.Since(start) < 3*idle {
		if _, err := backend.Write(chunk); err != nil {
			t.Fatalf("backend write failed mid-stream after %v: %v", time.Since(start), err)
		}
		sent.Write(chunk)
		select {
		case at := <-backendEOF:
			t.Fatalf("client→server direction was half-closed at %v while the stream was active (idle timeout %v)", at, idle)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}

	// End of response: the backend closes its write half (like a server that
	// finished a Connection: close response). The player must get every byte.
	if err := backend.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case all := <-received:
		if !bytes.Equal(all, sent.Bytes()) {
			t.Fatalf("player received %d bytes, backend sent %d", len(all), sent.Len())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("player never saw the end of the stream")
	}
	select {
	case at := <-backendEOF:
		t.Fatalf("client→server direction was half-closed at %v before the client closed it", at)
	default:
	}

	// Now the connection is idle both ways: the bridge reaps it.
	select {
	case <-done:
	case <-time.After(10 * idle):
		t.Fatal("bridge was not reaped after going idle in both directions")
	}
}

// A connection silent in both directions is reaped about one timeout after the
// last activity, and both ends are closed (not merely half-closed).
func TestBridgeCopyReapsWhenIdleBothWays(t *testing.T) {
	const idle = 100 * time.Millisecond
	player, backend, done := bridged(t, idle)
	start := time.Now()
	select {
	case <-done:
	case <-time.After(20 * idle):
		t.Fatal("idle bridge was never reaped")
	}
	if elapsed := time.Since(start); elapsed < idle {
		t.Fatalf("reaped after %v, before the %v idle timeout", elapsed, idle)
	}
	for name, c := range map[string]net.Conn{"player": player, "backend": backend} {
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		var b [1]byte
		if _, err := c.Read(b[:]); err == nil || isTimeout(err) {
			t.Errorf("%s side still open after the reap: %v", name, err)
		}
	}
}

// Activity in one direction defers the reap of the other: the idle clock
// restarts at the last byte in EITHER direction.
func TestBridgeCopyReapIsMeasuredFromLastActivity(t *testing.T) {
	const idle = 150 * time.Millisecond
	player, backend, done := bridged(t, idle)
	go io.Copy(io.Discard, player)

	start := time.Now()
	var last time.Time
	// Trickle server→client traffic for 2× the timeout, then stop.
	for time.Since(start) < 2*idle {
		if _, err := backend.Write([]byte("tick")); err != nil {
			t.Fatal(err)
		}
		last = time.Now()
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(20 * idle):
		t.Fatal("bridge was not reaped after traffic stopped")
	}
	if since := time.Since(last); since < idle-20*time.Millisecond {
		t.Fatalf("reaped %v after the last byte, before the %v idle timeout", since, idle)
	}
}

// A real EOF still half-closes: the client finishing its upload must reach the
// backend as EOF while the response keeps flowing the other way.
func TestBridgeCopyPropagatesHalfCloseOnEOF(t *testing.T) {
	const idle = 5 * time.Second
	player, backend, _ := bridged(t, idle)

	if _, err := player.Write([]byte("upload")); err != nil {
		t.Fatal(err)
	}
	if err := player.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = backend.SetReadDeadline(time.Now().Add(2 * time.Second))
	body, err := io.ReadAll(backend)
	if err != nil {
		t.Fatalf("backend did not see EOF after the client's half-close: %v", err)
	}
	if string(body) != "upload" {
		t.Fatalf("backend got %q", body)
	}
	if _, err := backend.Write([]byte("response")); err != nil {
		t.Fatal(err)
	}
	backend.Close()
	_ = player.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := io.ReadAll(player)
	if err != nil || string(resp) != "response" {
		t.Fatalf("player got %q, %v after half-close", resp, err)
	}
}

// A write that stalls (the client stopped reading, e.g. a paused player) with
// nothing moving the other way is an idle connection too: it is reaped rather
// than pinning the goroutine forever.
func TestBridgeCopyReapsStalledWrite(t *testing.T) {
	const idle = 100 * time.Millisecond
	// net.Pipe is unbuffered, so the bridge's write to the player blocks on
	// the first byte the player does not read.
	player, tsnetSide := net.Pipe()
	defer player.Close()
	localSide, backend := tcpPair(t)
	done := make(chan struct{})
	go func() {
		bridgeCopy(tsnetSide, localSide, idle)
		close(done)
	}()
	if _, err := backend.Write([]byte("frame the player never reads")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(20 * idle):
		t.Fatal("stalled write was never reaped")
	}
}

func TestIsTimeout(t *testing.T) {
	if !isTimeout(errDeadline{}) {
		t.Error("a net.Error with Timeout() must count as a timeout (gVisor gonet shape)")
	}
	if isTimeout(io.EOF) {
		t.Error("EOF is not a timeout")
	}
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	_ = c1.SetReadDeadline(time.Now().Add(-time.Second))
	var b [1]byte
	if _, err := c1.Read(b[:]); !isTimeout(err) {
		t.Errorf("os.ErrDeadlineExceeded must count as a timeout, got %v", err)
	}
}

// errDeadline mimics gonet's timeoutError: a net.Error that is not
// os.ErrDeadlineExceeded.
type errDeadline struct{}

func (errDeadline) Error() string   { return "i/o timeout" }
func (errDeadline) Timeout() bool   { return true }
func (errDeadline) Temporary() bool { return true }

// BenchmarkBridgeCopyThroughput streams 64 MiB per iteration backend→player
// through one bridgeCopy over real loopback TCP: the per-chunk cost of the
// shared activity clock (one atomic store + monotonic read per read/write)
// against the copy itself. Compare with the pre-fix idleCopy by running the
// same benchmark on the parent commit.
func BenchmarkBridgeCopyThroughput(b *testing.B) {
	const total = 64 << 20
	chunk := bytes.Repeat([]byte{0x5A}, 256<<10)
	b.SetBytes(total)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		player, tsnetSide := tcpPairB(b)
		localSide, backend := tcpPairB(b)
		go bridgeCopy(tsnetSide, localSide, time.Minute)
		done := make(chan int64)
		go func() {
			n, _ := io.Copy(io.Discard, player)
			done <- n
		}()
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
		player.Close()
	}
}

func tcpPairB(b *testing.B) (net.Conn, net.Conn) {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	return dialed, <-accepted
}
