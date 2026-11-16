package ktp

import (
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

func establishedHarness(t *testing.T, seed uint64, link simnet.Link) *harness {
	t.Helper()
	h := newHarness(t, seed, link)
	h.dial()
	if !h.run(5*time.Second, h.established) {
		t.Fatal("not established")
	}
	return h
}

func TestPressureDelivered(t *testing.T) {
	h := establishedHarness(t, 11, cleanLink)
	cli, srv := h.conns[0], h.conns[1]
	if cli.PeerSpeaksPressure() || srv.PeerSpeaksPressure() {
		t.Fatal("peers speak pressure before any frame was sent")
	}
	if err := srv.SendPressure(Pressure{Credits: 7, Load: 650, Queue: 3}); err != nil {
		t.Fatal(err)
	}
	if !h.run(time.Second, func() bool { _, ok := cli.PeerPressure(); return ok }) {
		t.Fatal("pressure never arrived")
	}
	p, _ := cli.PeerPressure()
	if p.Credits != 7 || p.Load != 650 || p.Queue != 3 || p.Seq == 0 || p.Stream {
		t.Fatalf("got %+v", p)
	}
	if !cli.PeerSpeaksPressure() || srv.PeerSpeaksPressure() || !srv.SentPressure() {
		t.Fatal("capability flags wrong after one frame")
	}
	if st := cli.Stats(); st.PressureIn != 1 {
		t.Fatalf("PressureIn = %d", st.PressureIn)
	}
}

// Only the newest state per scope is sent: three updates queued before a
// Poll go out as one frame carrying the last.
func TestPressureCoalesces(t *testing.T) {
	h := establishedHarness(t, 12, cleanLink)
	cli, srv := h.conns[0], h.conns[1]
	for i := uint64(1); i <= 3; i++ {
		srv.SendPressure(Pressure{Credits: i})
	}
	h.run(time.Second, func() bool { _, ok := cli.PeerPressure(); return ok })
	h.run(200*time.Millisecond, func() bool { return false })
	p, _ := cli.PeerPressure()
	if p.Credits != 3 {
		t.Fatalf("credits = %d, want the latest (3)", p.Credits)
	}
	if n := srv.Stats().PressureOut; n != 1 {
		t.Fatalf("sent %d frames, want 1 (coalesced)", n)
	}
}

// A lost frame is resent; an older frame arriving late never overwrites a
// newer state.
func TestPressureLossAndReorder(t *testing.T) {
	h := establishedHarness(t, 13, cleanLink)
	cli, srv := h.conns[0], h.conns[1]
	// Lose everything the server sends for a moment, so the packet carrying
	// the frame is lost and must be repaired.
	h.links[1] = simnet.Link{Latency: 20 * time.Millisecond, Loss: 1}
	srv.SendPressure(Pressure{Credits: 5})
	h.run(30*time.Millisecond, func() bool { return false })
	h.links[1] = cleanLink
	if !h.run(5*time.Second, func() bool { p, ok := cli.PeerPressure(); return ok && p.Credits == 5 }) {
		t.Fatal("lost pressure frame was never repaired")
	}
	// Stale frame: hand the client a frame with an old sequence number.
	old := Pressure{Seq: 1, Credits: 99}
	cur, _ := cli.PeerPressure()
	if err := cli.handlePressure(old); err != nil {
		t.Fatal(err)
	}
	if p, _ := cli.PeerPressure(); p.Seq != cur.Seq || p.Credits != 5 {
		t.Fatalf("stale frame rolled state back: %+v", p)
	}
}

// Stream-scoped demand may arrive before the stream's first data (it is
// queued ahead of stream data); the receiver opens the stream for it, and
// forgets the state when the stream retires.
func TestPressureStreamScope(t *testing.T) {
	h := establishedHarness(t, 14, cleanLink)
	cli, srv := h.conns[0], h.conns[1]
	id, err := cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.SendPressure(Pressure{Stream: true, StreamID: 99, Credits: 1}); err == nil {
		t.Fatal("pressure for a stream that is not open was accepted")
	}
	cli.SendPressure(Pressure{Stream: true, StreamID: id, Credits: 16})
	cli.StreamWrite(id, []byte("request"))
	h.flush()
	if !h.run(time.Second, func() bool { _, ok := srv.PeerStreamPressure(id); return ok }) {
		t.Fatal("stream demand never arrived")
	}
	if got, ok := srv.AcceptStream(); !ok || got != id {
		t.Fatalf("stream not opened by its demand frame: %v %v", got, ok)
	}
	if p, _ := srv.PeerStreamPressure(id); p.Credits != 16 || !p.Stream {
		t.Fatalf("got %+v", p)
	}
	// Finish both halves; the state goes with the stream.
	cli.StreamCloseWrite(id)
	h.flush()
	h.run(200*time.Millisecond, func() bool { return false })
	buf := make([]byte, 64)
	for i := 0; i < 4; i++ {
		srv.StreamRead(id, buf)
	}
	srv.StreamCloseWrite(id)
	h.flush()
	h.run(time.Second, func() bool { _, ok := srv.PeerStreamPressure(id); return !ok })
	if _, ok := srv.PeerStreamPressure(id); ok {
		t.Fatalf("retired stream's pressure state was kept: %+v", srv.streams[id])
	}
	// A late frame for the retired stream is ignored, not an error.
	if err := srv.handlePressure(Pressure{Stream: true, StreamID: id, Seq: 1 << 40, Credits: 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := srv.PeerStreamPressure(id); ok {
		t.Fatal("retired stream resurrected by a late frame")
	}
}

func TestPressureClampsLoad(t *testing.T) {
	h := establishedHarness(t, 15, cleanLink)
	h.conns[1].SendPressure(Pressure{Load: 5000})
	h.run(time.Second, func() bool { _, ok := h.conns[0].PeerPressure(); return ok })
	if p, _ := h.conns[0].PeerPressure(); p.Load != wire.MaxLoad {
		t.Fatalf("load = %d, want clamped to %d", p.Load, wire.MaxLoad)
	}
}
