package ktp

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// craftedShortPlaintext returns a header-only datagram (payload length 0)
// whose last four header bytes happen to be a valid CRC32C of the first 24:
// it parses, and its "checksum" verifies. Anyone can build one.
func craftedShortPlaintext(t *testing.T, pt wire.PacketType) []byte {
	t.Helper()
	for i := 0; i < 1<<24; i++ {
		pkt := wire.AppendHeader(nil, wire.Header{Type: pt, Version: wire.Version,
			DstCID: wire.ConnID{byte(i), byte(i >> 8), byte(i >> 16)}})
		sum := wire.Checksum(pkt[:24])
		if sum&0xffff != 0 { // bytes 26..27 are the payload length: must stay 0
			continue
		}
		pkt[24], pkt[25] = byte(sum>>24), byte(sum>>16)
		if _, err := wire.ParseHeader(pkt); err != nil {
			t.Fatal(err)
		}
		if _, ok := wire.VerifyChecksum(pkt); !ok {
			t.Fatal("crafted checksum does not verify")
		}
		return pkt
	}
	t.Fatal("no crafted packet found")
	return nil
}

// A plaintext packet too short to hold a header plus its CRC must be
// rejected, not sliced out of range: without a cluster key OpenRaw accepts
// such packets from anyone, so this was a remote crash.
func TestShortPlaintextPacketsAreRejected(t *testing.T) {
	raw := craftedShortPlaintext(t, wire.RawPacket)
	if _, err := OpenRaw(&Config{}, raw); err == nil {
		t.Fatal("28-byte raw packet opened")
	}
	if _, err := (*protector)(nil).open(wire.Header{}, raw[:wire.HeaderLen]); err == nil {
		t.Fatal("header-only plaintext packet opened")
	}
	// The same shape aimed at a client mid-handshake (Handshake packet) and
	// as a stateless reset.
	client, err := NewClientConn(testConfig(rand.New(rand.NewPCG(1, 2))), time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, pt := range []wire.PacketType{wire.HandshakePacket, wire.ResetPacket} {
		pkt := craftedShortPlaintext(t, pt)
		copy(pkt[2:10], client.localCID[:])
		// The CID changed the checksum; recraft over the packet number.
		for i := 0; i < 1<<24; i++ {
			pkt[18], pkt[19], pkt[20] = byte(i), byte(i>>8), byte(i>>16)
			sum := wire.Checksum(pkt[:24])
			if sum&0xffff == 0 {
				pkt[24], pkt[25] = byte(sum>>24), byte(sum>>16)
				break
			}
		}
		client.HandlePacket(pkt, client.created) // must not panic
	}
}
