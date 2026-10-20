package main

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// countingConn records what the copy loop asks of a connection: how often it
// sets deadlines, and the largest buffer it reads into.
type countingConn struct {
	net.Conn
	readDeadlines  atomic.Int64
	writeDeadlines atomic.Int64
	maxRead        atomic.Int64
}

func (c *countingConn) SetReadDeadline(t time.Time) error {
	c.readDeadlines.Add(1)
	return c.Conn.SetReadDeadline(t)
}

func (c *countingConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadlines.Add(1)
	return c.Conn.SetWriteDeadline(t)
}

func (c *countingConn) Read(p []byte) (int, error) {
	for {
		cur := c.maxRead.Load()
		if int64(len(p)) <= cur || c.maxRead.CompareAndSwap(cur, int64(len(p))) {
			break
		}
	}
	return c.Conn.Read(p)
}

// CloseWrite keeps half-close working through the wrapper (bridgeCopy
// type-asserts halfCloser).
func (c *countingConn) CloseWrite() error {
	if hc, ok := c.Conn.(halfCloser); ok {
		return hc.CloseWrite()
	}
	return nil
}

// streamThroughBridge sends one request player → backend, then streams total
// bytes backend → player through a bridgeCopy whose two sides are wrapped in
// countingConns, and checks every byte arrives.
func streamThroughBridge(t *testing.T, tailnetPair func(testing.TB) (net.Conn, net.Conn), total int) (tsnetSide, localSide *countingConn) {
	t.Helper()
	player, ts := tailnetPair(t)
	local, backend := tcpPair(t)
	tsnetSide, localSide = &countingConn{Conn: ts}, &countingConn{Conn: local}
	copied := make(chan struct{})
	go func() {
		bridgeCopy(tsnetSide, localSide, time.Minute)
		close(copied)
	}()

	request := []byte("GET /movie.mkv HTTP/1.1\r\nRange: bytes=0-\r\n\r\n")
	if _, err := player.Write(request); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(backend, make([]byte, len(request))); err != nil {
		t.Fatalf("backend did not get the request: %v", err)
	}
	got := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, player)
		got <- n
	}()
	chunk := bytes.Repeat([]byte{0xA5}, 256<<10)
	for sent := 0; sent < total; sent += len(chunk) {
		if _, err := backend.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	backend.Close()
	select {
	case n := <-got:
		if n != int64(total) {
			t.Fatalf("player got %d of %d bytes", n, total)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stream stalled")
	}
	player.Close()
	select {
	case <-copied:
	case <-time.After(5 * time.Second):
		t.Fatal("bridgeCopy did not finish after both ends closed")
	}
	return tsnetSide, localSide
}

func loopbackPairTB(tb testing.TB) (net.Conn, net.Conn) { return tcpPair(tb.(*testing.T)) }

var tailnetSides = []struct {
	name string
	pair func(testing.TB) (net.Conn, net.Conn)
}{
	{"loopback", loopbackPairTB},
	{"gonet", gonetPair},
}

// The copy loop used to set a read and a write deadline for every 32 KiB
// chunk — on gonet each one stops a timer and allocates a fresh AfterFunc.
// A busy stream now re-arms at most about once a second (rearmSlack), so a
// 32 MiB stream sets a handful, not thousands.
func TestBridgeCopyRearmsDeadlinesRarelyOnABusyStream(t *testing.T) {
	const total = 32 << 20
	for _, side := range tailnetSides {
		t.Run(side.name, func(t *testing.T) {
			start := time.Now()
			ts, local := streamThroughBridge(t, side.pair, total)
			elapsed := time.Since(start)
			sets := ts.readDeadlines.Load() + ts.writeDeadlines.Load() +
				local.readDeadlines.Load() + local.writeDeadlines.Load()
			// Per direction: one arm per reader/writer plus one per second of
			// streaming, plus the re-arms of the quiet request direction.
			limit := int64(16 + 8*elapsed.Seconds())
			if sets > limit {
				t.Fatalf("%d deadline sets for a %d MiB stream in %v (limit %d); the loop re-arms per chunk",
					sets, total>>20, elapsed, limit)
			}
			t.Logf("%d deadline sets for %d MiB in %v", sets, total>>20, elapsed)
		})
	}
}

// A bulk direction (a read filled the 32 KiB buffer) moves to a 256 KiB
// buffer; the request direction, which only ever carried a few bytes, keeps
// the small one — an idle keep-alive connection must not pin 256 KiB.
func TestBridgeCopyGrowsTheBufferOnlyForBulkStreams(t *testing.T) {
	for _, side := range tailnetSides {
		t.Run(side.name, func(t *testing.T) {
			ts, local := streamThroughBridge(t, side.pair, 8<<20)
			if got := local.maxRead.Load(); got != 256<<10 {
				t.Fatalf("bulk direction read into at most %d bytes, want a 256 KiB buffer", got)
			}
			if got := ts.maxRead.Load(); got != 32<<10 {
				t.Fatalf("request direction read into %d bytes, want it to keep 32 KiB", got)
			}
		})
	}
}

// The re-arm slack never delays a reap: a connection that goes quiet after a
// stream is still reaped one timeout after its last byte, over gonet too.
func TestBridgeCopyReapStaysExactAfterAStream(t *testing.T) {
	const idle = 200 * time.Millisecond
	for _, side := range tailnetSides {
		t.Run(side.name, func(t *testing.T) {
			player, ts := side.pair(t)
			local, backend := tcpPair(t)
			finished := make(chan time.Time, 1)
			go func() {
				bridgeCopy(ts, local, idle)
				finished <- time.Now()
			}()
			// Stream for a while (several slack periods), then stop.
			chunk := bytes.Repeat([]byte{1}, 64<<10)
			go io.Copy(io.Discard, player)
			streamUntil := time.Now().Add(3 * idle)
			for time.Now().Before(streamUntil) {
				if _, err := backend.Write(chunk); err != nil {
					t.Fatal(err)
				}
				time.Sleep(5 * time.Millisecond)
			}
			lastByte := time.Now()
			select {
			case at := <-finished:
				quiet := at.Sub(lastByte)
				if quiet < idle-20*time.Millisecond {
					t.Fatalf("reaped %v after the last byte, before the %v idle timeout", quiet, idle)
				}
				if quiet > idle+500*time.Millisecond {
					t.Fatalf("reaped %v after the last byte; want about %v", quiet, idle)
				}
			case <-time.After(10 * idle):
				t.Fatal("idle connection never reaped")
			}
		})
	}
}
