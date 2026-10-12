package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func TestValidatedTransfer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() { defer server.Close(); done <- receive(server) }()
	if err := send(client, 65553, bytes.Repeat([]byte{0x5a}, 65536)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestRejectPartialAndOversizedTransfers(t *testing.T) {
	for _, size := range []uint64{0, maxBytes + 1, 5} {
		var b bytes.Buffer
		binary.Write(&b, binary.BigEndian, size)
		b.Write([]byte{1, 2})
		if err := receive(&b); err == nil {
			t.Fatalf("accepted invalid/partial size %d", size)
		}
	}
}
func TestCorruptAckFails(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	go func() { defer server.Close(); io.CopyN(io.Discard, server, 9); server.Write(make([]byte, 32)) }()
	if err := send(client, 1, []byte{0x5a}); err == nil {
		t.Fatal("accepted corrupt checksum")
	}
}
