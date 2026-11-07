package ktp

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// KTP has a second, independent implementation: the Rust server inside
// kvnode (kvnode/src/ktp). These vectors pin the byte-level contract between
// the two -- codec, key schedule, handshake messages, packet protection and
// stateless resets -- from fixed inputs. This test fails when the Go output
// drifts from the committed file; kvnode's tests fail when Rust drifts from it.
//
//	cd kvgw && go test ./netstack/ktp -run TestRustVectors -update-vectors

var updateVectors = flag.Bool("update-vectors", false, "rewrite kvnode/tests/ktp_vectors.txt from the Go implementation")

const vectorsPath = "../../../kvnode/tests/ktp_vectors.txt"

func seqBytes(start byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = start + byte(i)
	}
	return b
}

type vectorFile struct{ b strings.Builder }

func (v *vectorFile) section(s string) { fmt.Fprintf(&v.b, "\n# %s\n", s) }

// line writes "name field..."; []byte fields are hex ("-" when empty).
func (v *vectorFile) line(name string, fields ...any) {
	v.b.WriteString(name)
	for _, f := range fields {
		switch x := f.(type) {
		case []byte:
			if len(x) == 0 {
				v.b.WriteString(" -")
			} else {
				fmt.Fprintf(&v.b, " %x", x)
			}
		default:
			fmt.Fprintf(&v.b, " %v", x)
		}
	}
	v.b.WriteByte('\n')
}

func frameBytes(fs ...wire.Frame) []byte {
	var b []byte
	for _, f := range fs {
		b = f.Append(b)
	}
	return b
}

func buildVectors(t *testing.T) string {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	v := &vectorFile{}
	v.b.WriteString("# KTP/1 test vectors, generated from the Go implementation by\n" +
		"# kvgw/netstack/ktp/vectors_test.go. kvnode's Rust KTP server must reproduce\n" +
		"# every line (cargo test). Regenerate after a deliberate protocol change:\n" +
		"#   cd kvgw && go test ./netstack/ktp -run TestRustVectors -update-vectors\n" +
		"# Format: name field...; byte strings are hex, \"-\" when empty.\n")

	v.section("varint <value> <encoding>")
	for _, x := range []uint64{0, 1, 37, 63, 64, 15293, 16383, 16384, 494878333, 1073741823, 1073741824,
		151288809941952652, wire.MaxVarint} {
		v.line("varint", x, wire.AppendVarint(nil, x))
	}

	v.section("crc32c <input> <checksum, big-endian>")
	for _, in := range [][]byte{nil, []byte("kvstore"), seqBytes(0, 32)} {
		var sum [4]byte
		c := wire.Checksum(in)
		sum[0], sum[1], sum[2], sum[3] = byte(c>>24), byte(c>>16), byte(c>>8), byte(c)
		v.line("crc32c", in, sum[:])
	}

	v.section("header: type 1rtt, dcid 0102..08, scid 1112..18, pn 0x0102030405060708, payload len 0x1234")
	var dcid, scid wire.ConnID
	copy(dcid[:], seqBytes(0x01, 8))
	copy(scid[:], seqBytes(0x11, 8))
	v.line("header", wire.AppendHeader(nil, wire.Header{Type: wire.OneRTTPacket, Version: wire.Version,
		DstCID: dcid, SrcCID: scid, PacketNumber: 0x0102030405060708, PayloadLen: 0x1234}))

	v.section("frames (the values are fixed in kvnode/src/ktp/wire.rs tests too)")
	frames := []struct {
		name string
		f    wire.Frame
	}{
		{"padding", wire.Padding{N: 5}},
		{"ping", wire.Ping{}},
		{"ack", wire.Ack{Delay: 1234, Ranges: []wire.AckRange{{Smallest: 990, Largest: 1000}, {Smallest: 900, Largest: 950}, {Smallest: 7, Largest: 7}}}},
		{"ack_single", wire.Ack{Delay: 0, Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}},
		{"reset_stream", wire.ResetStream{StreamID: 4, Code: 0x101, FinalSize: 70000}},
		{"stop_sending", wire.StopSending{StreamID: 8, Code: 0x102}},
		{"handshake", wire.Handshake{Data: []byte("hs-data")}},
		{"stream", wire.Stream{StreamID: 12, Offset: 65536, Data: []byte("stream-data")}},
		{"stream_fin", wire.Stream{StreamID: 0, Offset: 0, Fin: true}},
		{"max_data", wire.MaxData{Max: 1 << 33}},
		{"max_stream_data", wire.MaxStreamData{StreamID: 16, Max: 2 << 20}},
		{"max_streams", wire.MaxStreams{Max: 300}},
		{"data_blocked", wire.DataBlocked{Limit: 8 << 20}},
		{"stream_data_blocked", wire.StreamDataBlocked{StreamID: 20, Limit: 2 << 20}},
		{"connection_close", wire.ConnectionClose{Code: 0x0a, Reason: "protocol violation"}},
		{"handshake_done", wire.HandshakeDone{}},
		{"datagram", wire.Datagram{Data: []byte("dgram")}},
	}
	var all []wire.Frame
	for _, x := range frames {
		v.line("frame."+x.name, x.f.Append(nil))
		all = append(all, x.f)
	}
	v.line("frames.all", frameBytes(all...))
	// A STREAM frame without the length flag runs to the end of the payload
	// (Go never sends one; both parsers must accept it).
	nolen := append([]byte{wire.TypeStream | 0x01, 24, 5}, "tail"...)
	if fs, err := wire.ParseFrames(nolen); err != nil || len(fs) != 1 ||
		fs[0].(wire.Stream).StreamID != 24 || string(fs[0].(wire.Stream).Data) != "tail" || !fs[0].(wire.Stream).Fin {
		t.Fatalf("no-length stream frame: %v %v", fs, err)
	}
	v.line("frame.stream_nolen", nolen)
	// Payloads both parsers must reject.
	for i, bad := range [][]byte{
		{wire.TypeAck, 5, 0, 0, 6},                        // first range larger than largest
		{wire.TypeAck, 10, 0, byte(wire.MaxAckRanges), 0}, // too many ranges
		{wire.TypeAck, 10, 0, 1, 2, 7, 0},                 // gap runs below zero
		{wire.TypeHandshake, 9, 1, 2},                     // length past the end
		{0x1f},                                            // unknown frame type
		{wire.TypeStream | 0x02, 1, 0x40},                 // truncated varint
	} {
		if _, err := wire.ParseFrames(bad); err == nil {
			t.Fatalf("bad payload %d parsed: %x", i, bad)
		}
		v.line("frames.bad", bad)
	}

	v.section("handshake from fixed keys")
	clusterKey := []byte("kvstore vector cluster key")
	cEph, err := ecdh.X25519().NewPrivateKey(seqBytes(0x01, 32))
	must(err)
	sEph, err := ecdh.X25519().NewPrivateKey(seqBytes(0x41, 32))
	must(err)
	cNonce, sNonce := seqBytes(0x81, 32), seqBytes(0xa1, 32)
	sID := ed25519.NewKeyFromSeed(seqBytes(0xc1, 32))
	cID := ed25519.NewKeyFromSeed(seqBytes(0xe1, 32))
	cParams := transportParams{maxData: 8 << 20, maxStreamData: 2 << 20, maxStreams: 256}
	sParams := transportParams{maxData: 4 << 20, maxStreamData: 1 << 20, maxStreams: 100}

	helloMsg := hello{eph: cEph.PublicKey().Bytes(), nonce: cNonce, params: cParams}.encode()
	if _, err := parseHello(helloMsg); err != nil {
		t.Fatal(err)
	}
	shared, err := sEph.ECDH(cEph.PublicKey())
	must(err)
	sh := serverHello{eph: sEph.PublicKey().Bytes(), nonce: sNonce, pub: sID.Public().(ed25519.PublicKey), params: sParams}
	core := sh.encodeCore()
	th := transcriptHash(helloMsg, core)
	keys, err := deriveKeys(shared, clusterKey, th)
	must(err)
	sig := ed25519.Sign(sID, signedMessage("server", th))
	tag := confirmTag(keys.confirm, th)
	serverHelloMsg := append(append(append([]byte(nil), core...), sig...), tag...)
	if _, err := parseServerHello(serverHelloMsg); err != nil {
		t.Fatal(err)
	}
	clientAuth := append([]byte{msgClientAuth}, cID.Public().(ed25519.PublicKey)...)
	clientAuth = append(clientAuth, ed25519.Sign(cID, signedMessage("client", th))...)

	v.line("hs.cluster_key", clusterKey)
	v.line("hs.client_eph_secret", seqBytes(0x01, 32))
	v.line("hs.client_eph_public", cEph.PublicKey().Bytes())
	v.line("hs.client_nonce", cNonce)
	v.line("hs.client_params", cParams.maxData, cParams.maxStreamData, cParams.maxStreams)
	v.line("hs.client_identity_seed", seqBytes(0xe1, 32))
	v.line("hs.client_identity_public", []byte(cID.Public().(ed25519.PublicKey)))
	v.line("hs.server_eph_secret", seqBytes(0x41, 32))
	v.line("hs.server_eph_public", sEph.PublicKey().Bytes())
	v.line("hs.server_nonce", sNonce)
	v.line("hs.server_params", sParams.maxData, sParams.maxStreamData, sParams.maxStreams)
	v.line("hs.server_identity_seed", seqBytes(0xc1, 32))
	v.line("hs.server_identity_public", []byte(sID.Public().(ed25519.PublicKey)))
	v.line("hs.hello", helloMsg)
	v.line("hs.shared", shared)
	v.line("hs.server_core", core)
	v.line("hs.transcript", th)
	v.line("hs.c2s_key", keys.c2sKey)
	v.line("hs.c2s_iv", keys.c2sIV)
	v.line("hs.s2c_key", keys.s2cKey)
	v.line("hs.s2c_iv", keys.s2cIV)
	v.line("hs.confirm_key", keys.confirm)
	v.line("hs.server_sig", sig)
	v.line("hs.confirm_tag", tag)
	v.line("hs.server_hello", serverHelloMsg)
	v.line("hs.client_auth", clientAuth)
	// The same transcript without a cluster key: the PSK salt is SHA-256 of
	// the label alone.
	nk, err := deriveKeys(shared, nil, th)
	must(err)
	v.line("hs.nokey.c2s_key", nk.c2sKey)
	v.line("hs.nokey.s2c_iv", nk.s2cIV)
	v.line("hs.nokey.confirm_tag", confirmTag(nk.confirm, th))

	v.section("packets: client cid 2122..28, server cid 3132..38, original dcid 4142..48")
	var clientCID, serverCID, origDCID wire.ConnID
	copy(clientCID[:], seqBytes(0x21, 8))
	copy(serverCID[:], seqBytes(0x31, 8))
	copy(origDCID[:], seqBytes(0x41, 8))
	v.line("pkt.client_cid", clientCID[:])
	v.line("pkt.server_cid", serverCID[:])
	v.line("pkt.orig_dcid", origDCID[:])

	// The client's Initial, exactly as pollInitial builds it.
	initPayload := wire.Handshake{Data: helloMsg}.Append(nil)
	initPayload = wire.Padding{N: MinInitialSize - wire.HeaderLen - 4 - len(initPayload)}.Append(initPayload)
	initial := (*protector)(nil).seal(nil, wire.Header{Type: wire.InitialPacket, Version: wire.Version,
		DstCID: origDCID, SrcCID: clientCID, PacketNumber: 0}, initPayload)
	if len(initial) != MinInitialSize {
		t.Fatalf("initial is %d bytes", len(initial))
	}
	v.line("pkt.initial", initial)

	// The server's reply as pollServerHello builds it: SERVER_HELLO, then
	// the ACK for the Initial (pn 0, held 0µs), CRC-protected.
	shPayload := frameBytes(wire.Handshake{Data: serverHelloMsg}, wire.Ack{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}})
	v.line("pkt.server_hello_payload", shPayload)
	v.line("pkt.server_hello", (*protector)(nil).seal(nil, wire.Header{Type: wire.HandshakePacket,
		Version: wire.Version, DstCID: clientCID, SrcCID: serverCID, PacketNumber: 0}, shPayload))

	// The client's first 1-RTT packet: CLIENT_AUTH, then a request.
	c2s, err := newProtector(keys.c2sKey, keys.c2sIV, false)
	must(err)
	c2sPayload := frameBytes(wire.Handshake{Data: clientAuth}, wire.Ack{Delay: 40, Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}},
		wire.Stream{StreamID: 0, Offset: 0, Data: []byte{5}})
	v.line("pkt.c2s_payload", c2sPayload)
	v.line("pkt.c2s", c2s.seal(nil, wire.Header{Type: wire.OneRTTPacket, Version: wire.Version,
		DstCID: serverCID, SrcCID: clientCID, PacketNumber: 1}, c2sPayload))

	// The server's answer: HANDSHAKE_DONE, ACK, the response bytes.
	s2c, err := newProtector(keys.s2cKey, keys.s2cIV, false)
	must(err)
	s2cPayload := frameBytes(wire.Ack{Delay: 3, Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, wire.HandshakeDone{},
		wire.Stream{StreamID: 0, Offset: 0, Data: []byte{0}})
	v.line("pkt.s2c_payload", s2cPayload)
	s2cPkt := s2c.seal(nil, wire.Header{Type: wire.OneRTTPacket, Version: wire.Version,
		DstCID: clientCID, SrcCID: serverCID, PacketNumber: 0x0102030405}, s2cPayload)
	v.line("pkt.s2c", s2cPkt)
	// Flipping any bit must fail authentication (header is AAD).
	for _, at := range []int{0, 18, wire.HeaderLen, len(s2cPkt) - 1} {
		bad := bytes.Clone(s2cPkt)
		bad[at] ^= 0x40
		h, err := wire.ParseHeader(bad)
		if err == nil {
			if recv, _ := newProtector(keys.s2cKey, keys.s2cIV, false); recv != nil {
				if _, err := recv.open(h, bad); err == nil {
					t.Fatalf("tampered byte %d opened", at)
				}
			}
		}
	}

	v.section("stateless reset")
	v.line("reset.token", resetToken(clusterKey, serverCID))
	v.line("reset.token_nokey", resetToken(nil, serverCID))
	// A 1-RTT packet for a connection the server forgot (it restarted): the
	// reply goes back to the client's CID, with the token for the dead
	// server CID.
	v.line("reset.packet", StatelessReset(&Config{ClusterKey: clusterKey}, wire.Header{Type: wire.OneRTTPacket,
		DstCID: serverCID, SrcCID: clientCID, PacketNumber: 9}))
	return v.b.String()
}

func TestRustVectors(t *testing.T) {
	got := buildVectors(t)
	if again := buildVectors(t); again != got {
		t.Fatal("vectors are not deterministic")
	}
	if *updateVectors {
		if err := os.WriteFile(vectorsPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatalf("%v (generate with -update-vectors)", err)
	}
	if string(want) != got {
		t.Fatalf("%s is stale: the Go KTP implementation no longer produces it. If the protocol change is "+
			"deliberate, regenerate with -update-vectors and update kvnode/src/ktp to match.", vectorsPath)
	}
}
