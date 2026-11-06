package wire

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 63, 64, 16383, 16384, 1073741823, 1073741824, MaxVarint} {
		b := AppendVarint(nil, v)
		if len(b) != VarintLen(v) {
			t.Fatalf("%d: len %d want %d", v, len(b), VarintLen(v))
		}
		got, n, err := ParseVarint(b)
		if err != nil || got != v || n != len(b) {
			t.Fatalf("%d: got %d n=%d err=%v", v, got, n, err)
		}
	}
	if _, _, err := ParseVarint([]byte{0xc0, 1}); err == nil {
		t.Fatal("truncated 8-byte varint accepted")
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{Type: OneRTTPacket, Version: Version, DstCID: ConnID{1, 2, 3}, SrcCID: ConnID{9}, PacketNumber: 1 << 40, PayloadLen: 3}
	pkt := append(AppendHeader(nil, h), 1, 2, 3)
	got, err := ParseHeader(pkt)
	if err != nil || got != h {
		t.Fatalf("got %+v err %v", got, err)
	}
	if _, err := ParseHeader(pkt[:len(pkt)-1]); err != ErrLength {
		t.Fatalf("length mismatch: %v", err)
	}
	bad := append([]byte(nil), pkt...)
	bad[0] = 0x90
	if _, err := ParseHeader(bad); err != ErrType {
		t.Fatalf("bad type: %v", err)
	}
}

func TestChecksum(t *testing.T) {
	b := AppendChecksum([]byte("hello"))
	if body, ok := VerifyChecksum(b); !ok || string(body) != "hello" {
		t.Fatal("checksum round trip")
	}
	b[1] ^= 0x10
	if _, ok := VerifyChecksum(b); ok {
		t.Fatal("bit flip not detected")
	}
}

func randomFrame(rng *rand.Rand) Frame {
	data := func() []byte {
		b := make([]byte, rng.IntN(40))
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		return b
	}
	v := func() uint64 { return rng.Uint64N(1 << uint(rng.IntN(62)+1)) }
	switch rng.IntN(14) {
	case 0:
		return Ping{}
	case 1:
		var rs []AckRange
		hi := v() + 1000
		for i := 0; i < 1+rng.IntN(MaxAckRanges) && hi > 3; i++ {
			lo := hi - min(hi, rng.Uint64N(5))
			rs = append(rs, AckRange{Smallest: lo, Largest: hi})
			if lo < 3 {
				break
			}
			hi = lo - 2 - min(lo-2, rng.Uint64N(5))
		}
		return Ack{Delay: v(), Ranges: rs}
	case 2:
		return ResetStream{StreamID: v(), Code: v(), FinalSize: v()}
	case 3:
		return StopSending{StreamID: v(), Code: v()}
	case 4:
		return Handshake{Data: data()}
	case 5:
		return Stream{StreamID: v(), Offset: v() >> 2, Data: data(), Fin: rng.IntN(2) == 0}
	case 6:
		return MaxData{Max: v()}
	case 7:
		return MaxStreamData{StreamID: v(), Max: v()}
	case 8:
		return MaxStreams{Max: v()}
	case 9:
		return DataBlocked{Limit: v()}
	case 10:
		return StreamDataBlocked{StreamID: v(), Limit: v()}
	case 11:
		return ConnectionClose{Code: v(), Reason: string(data())}
	case 12:
		return HandshakeDone{}
	default:
		return Datagram{Data: data()}
	}
}

// normalize maps nil and empty byte slices together for comparison.
func normalize(f Frame) Frame {
	switch x := f.(type) {
	case Handshake:
		x.Data = append([]byte{}, x.Data...)
		return x
	case Stream:
		x.Data = append([]byte{}, x.Data...)
		return x
	case Datagram:
		x.Data = append([]byte{}, x.Data...)
		return x
	}
	return f
}

func TestFramesRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 2000; i++ {
		var fs []Frame
		var b []byte
		for j := 0; j < 1+rng.IntN(5); j++ {
			f := randomFrame(rng)
			if got := len(f.Append(nil)); got != f.Len() {
				t.Fatalf("%T Len()=%d, encoded %d", f, f.Len(), got)
			}
			fs = append(fs, normalize(f))
			b = f.Append(b)
		}
		got, err := ParseFrames(b)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		for k := range got {
			got[k] = normalize(got[k])
		}
		if !reflect.DeepEqual(got, fs) {
			t.Fatalf("iteration %d:\n got %#v\nwant %#v", i, got, fs)
		}
	}
}

func TestPaddingCollapses(t *testing.T) {
	b := Padding{N: 7}.Append(Ping{}.Append(nil))
	fs, err := ParseFrames(b)
	if err != nil || len(fs) != 2 || fs[1].(Padding).N != 7 {
		t.Fatalf("%#v %v", fs, err)
	}
}

func TestAckContains(t *testing.T) {
	a := Ack{Ranges: []AckRange{{Smallest: 8, Largest: 10}, {Smallest: 2, Largest: 5}}}
	fs, err := ParseFrames(a.Append(nil))
	if err != nil {
		t.Fatal(err)
	}
	got := fs[0].(Ack)
	for pn, want := range map[uint64]bool{1: false, 2: true, 5: true, 6: false, 7: false, 8: true, 10: true, 11: false} {
		if got.Contains(pn) != want {
			t.Fatalf("Contains(%d) = %v", pn, !want)
		}
	}
	if got.Largest() != 10 {
		t.Fatal(got.Largest())
	}
}

func FuzzParseHeader(f *testing.F) {
	f.Add(AppendHeader(nil, Header{Type: InitialPacket, Version: Version}))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := ParseHeader(b)
		if err != nil {
			return
		}
		if !bytes.Equal(AppendHeader(nil, h), b[:HeaderLen]) {
			t.Fatalf("header does not round-trip")
		}
	})
}

func FuzzParseFrames(f *testing.F) {
	f.Add(Ack{Ranges: []AckRange{{1, 5}}}.Append(nil))
	f.Add(Stream{StreamID: 4, Offset: 9, Data: []byte("hi"), Fin: true}.Append(nil))
	f.Add([]byte{TypeAck, 5, 0, 1, 0, 9, 9})
	f.Fuzz(func(t *testing.T, b []byte) {
		fs, err := ParseFrames(b)
		if err != nil {
			return
		}
		var re []byte
		for _, fr := range fs {
			re = fr.Append(re)
		}
		again, err := ParseFrames(re)
		if err != nil || len(again) != len(fs) {
			t.Fatalf("re-encoded frames do not parse: %v", err)
		}
	})
}
