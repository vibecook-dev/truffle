// tsnet-bench provides a native Go/tsnet TCP baseline without the Rust bridge.
// The wire protocol matches truffle-bench transport: u64 size, bytes, SHA-256 ACK.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"time"

	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

const maxBytes = 1 << 30

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	mode := flag.String("mode", "client", "serve or client (run on separate hosts)")
	addr := flag.String("addr", "", "server tailnet IP:port, required for client")
	name := flag.String("name", "truffle-bench-wire-reference", "ephemeral node hostname")
	port := flag.Int("port", 9421, "server TCP port")
	size := flag.Uint64("bytes", 10<<20, "payload bytes, 1..1GiB")
	count := flag.Int("iterations", 5, "measured transfers")
	warmup := flag.Int("warmup", 1, "untimed transfers")
	budget := flag.Duration("timeout", 120*time.Second, "per-transfer deadline")
	flag.Parse()
	if (*mode != "serve" && *mode != "client") || *size == 0 || *size > maxBytes || *count < 1 || *count > 10000 || *warmup < 0 || *warmup > 1000 || *port < 1 || *port > 65535 || *budget <= 0 {
		return fmt.Errorf("invalid benchmark arguments")
	}
	key := os.Getenv("TRUFFLE_TEST_AUTHKEY")
	if key == "" {
		return fmt.Errorf("TRUFFLE_TEST_AUTHKEY is required")
	}
	state, err := os.MkdirTemp("", "truffle-native-bench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(state)
	server := &tsnet.Server{Dir: state, Hostname: *name, AuthKey: key, Ephemeral: true, Logf: func(string, ...any) {}}
	defer server.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	startup, stop := context.WithTimeout(ctx, 60*time.Second)
	status, err := server.Up(startup)
	stop()
	if err != nil {
		return err
	}
	if *mode == "serve" {
		listener, err := server.Listen("tcp", fmt.Sprintf(":%d", *port))
		if err != nil {
			return err
		}
		defer listener.Close()
		fmt.Fprintf(os.Stderr, "ready IPs=%v port=%d\n", status.TailscaleIPs, *port)
		go func() { <-ctx.Done(); listener.Close() }()
		slots := make(chan struct{}, 64)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			select {
			case slots <- struct{}{}:
				go func() {
					defer func() { <-slots }()
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(*budget))
					if err := receive(conn); err != nil {
						log.Print(err)
					}
				}()
			default:
				conn.Close()
			}
		}
	}
	host, _, err := net.SplitHostPort(*addr)
	if err != nil {
		return fmt.Errorf("-addr must be a server IP:port: %w", err)
	}
	lc, err := server.LocalClient()
	if err != nil {
		return err
	}
	probe := func() any {
		ctx, done := context.WithTimeout(ctx, 10*time.Second)
		defer done()
		ip, err := net.ResolveIPAddr("ip", host)
		if err != nil {
			return err.Error()
		}
		addr, ok := netip.AddrFromSlice(ip.IP)
		if !ok {
			return "invalid IP"
		}
		p, err := lc.Ping(ctx, addr.Unmap(), tailcfg.PingDisco)
		if err != nil {
			return err.Error()
		}
		return p
	}
	before := probe()
	samples := make([]map[string]any, 0, *count)
	payload := make([]byte, 64<<10)
	for i := range payload {
		payload[i] = 0x5a
	}
	for iteration := 0; iteration < *warmup+*count; iteration++ {
		dialCtx, done := context.WithTimeout(ctx, *budget)
		t := time.Now()
		conn, err := server.Dial(dialCtx, "tcp", *addr)
		done()
		if err != nil {
			return err
		}
		connectMS := float64(time.Since(t)) / float64(time.Millisecond)
		conn.SetDeadline(time.Now().Add(*budget))
		t = time.Now()
		err = send(conn, *size, payload)
		elapsed := time.Since(t)
		conn.Close()
		if err != nil {
			return err
		}
		if iteration >= *warmup {
			samples = append(samples, map[string]any{"bytes": *size, "connect_ms": connectMS, "transfer_ms": float64(elapsed) / float64(time.Millisecond), "mb_per_sec": float64(*size) / 1e6 / elapsed.Seconds()})
		}
	}
	build, _ := debug.ReadBuildInfo()
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"schema_version": 2, "workload": "native-tsnet-tcp", "timestamp_utc": time.Now().UTC(), "topology": "separate-processes", "before": before, "after": probe(), "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "build": build, "params": map[string]any{"bytes": *size, "iterations": *count, "warmup": *warmup}, "results": map[string]any{"valid": true, "samples": samples}})
}
func receive(conn io.ReadWriter) error {
	var size uint64
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		return err
	}
	if size == 0 || size > maxBytes {
		return fmt.Errorf("invalid size %d", size)
	}
	hash := sha256.New()
	if _, err := io.CopyN(hash, conn, int64(size)); err != nil {
		return err
	}
	_, err := conn.Write(hash.Sum(nil))
	return err
}
func send(conn io.ReadWriter, size uint64, payload []byte) error {
	if err := binary.Write(conn, binary.BigEndian, size); err != nil {
		return err
	}
	hash := sha256.New()
	for left := size; left > 0; {
		n := min(left, uint64(len(payload)))
		if _, err := conn.Write(payload[:n]); err != nil {
			return err
		}
		hash.Write(payload[:n])
		left -= n
	}
	ack := make([]byte, sha256.Size)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return err
	}
	if string(ack) != string(hash.Sum(nil)) {
		return fmt.Errorf("receiver checksum mismatch")
	}
	return nil
}
