package ktp

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

var cleanLink = simnet.Link{Latency: 20 * time.Millisecond}

func TestHandshake(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		t.Run(fmt.Sprintf("insecure=%v", insecure), func(t *testing.T) {
			h := newHarness(t, 1, cleanLink)
			h.cfg[0].Insecure, h.cfg[1].Insecure = insecure, insecure
			h.dial()
			if !h.run(5*time.Second, h.established) {
				t.Fatalf("not established: client %v/%v server %v", h.conns[0].State(), h.conns[0].Err(), h.conns[1])
			}
			if !bytes.Equal(h.conns[0].PeerIdentity(), h.cfg[1].Identity.Public().(ed25519.PublicKey)) ||
				!bytes.Equal(h.conns[1].PeerIdentity(), h.cfg[0].Identity.Public().(ed25519.PublicKey)) {
				t.Fatal("peer identities do not match the configured keys")
			}
			// One round trip: the client is connected after ~1 RTT (40ms).
			if hs := h.conns[0].Stats().Handshake; hs > 60*time.Millisecond {
				t.Fatalf("client handshake took %v, want ~1 RTT", hs)
			}
		})
	}
}

func TestWrongClusterKeyNeverConnects(t *testing.T) {
	h := newHarness(t, 2, cleanLink)
	h.cfg[1].ClusterKey = []byte("a different key")
	h.dial()
	h.run(30*time.Second, func() bool { return h.conns[0].State() == StateClosed })
	if !errors.Is(h.conns[0].Err(), ErrClusterKeyMismatch) {
		t.Fatalf("client err = %v, want cluster key mismatch", h.conns[0].Err())
	}
	if h.conns[1] != nil && h.conns[1].Established() {
		t.Fatal("server established with a client holding the wrong key")
	}
}

func TestVerifyPeerRejects(t *testing.T) {
	h := newHarness(t, 3, cleanLink)
	h.cfg[1].VerifyPeer = func(ed25519.PublicKey) error { return errors.New("not on the allow-list") }
	h.dial()
	h.run(30*time.Second, func() bool {
		return h.conns[0].State() >= StateDraining && h.conns[1] != nil && h.conns[1].State() >= StateClosing
	})
	var ce *CloseError
	if !errors.As(h.conns[0].Err(), &ce) || ce.Code != CodePeerRejected || !ce.Remote {
		t.Fatalf("client err = %v, want remote CodePeerRejected", h.conns[0].Err())
	}
}

// pattern is the deterministic content of stream k at offset i.
func pattern(k, i int) byte { return byte(i*31 + k*7 ^ i>>8) }

// transfer opens `streams` client streams, each sending `size` bytes and a
// FIN, and has the server verify every byte. It returns the harness.
func transfer(t *testing.T, seed uint64, link simnet.Link, streams, size int, mutate func(*harness)) *harness {
	t.Helper()
	h := newHarness(t, seed, link)
	if mutate != nil {
		mutate(h)
	}
	h.dial()
	if !h.run(30*time.Second, h.established) {
		t.Fatalf("seed %d: no handshake (client %v)", seed, h.conns[0].Err())
	}
	c, s := h.conns[0], h.conns[1]
	written := map[StreamID]int{}
	var ids []StreamID
	for k := 0; k < streams; k++ {
		id, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	got := map[StreamID]int{}
	done := map[StreamID]bool{}
	readBuf := make([]byte, 64<<10)
	writeBuf := make([]byte, 32<<10)
	h.onStep = func() {
		for k, id := range ids {
			for written[id] < size {
				n := min(len(writeBuf), size-written[id])
				for i := 0; i < n; i++ {
					writeBuf[i] = pattern(k, written[id]+i)
				}
				w, err := c.StreamWrite(id, writeBuf[:n])
				if err != nil {
					t.Fatalf("seed %d: write: %v", seed, err)
				}
				written[id] += w
				if w < n {
					break
				}
			}
			if written[id] == size {
				c.StreamCloseWrite(id)
			}
		}
		for {
			id, ok := s.AcceptStream()
			if !ok {
				break
			}
			_ = id
		}
		for k, id := range ids {
			if done[id] {
				continue
			}
			for {
				n, err := s.StreamRead(id, readBuf)
				for i := 0; i < n; i++ {
					if readBuf[i] != pattern(k, got[id]+i) {
						t.Fatalf("seed %d: stream %d byte %d corrupted", seed, id, got[id]+i)
					}
				}
				got[id] += n
				if err == io.EOF {
					done[id] = true
					break
				}
				if err != nil && !errors.Is(err, ErrUnknownStream) {
					t.Fatalf("seed %d: read: %v", seed, err)
				}
				if n == 0 {
					break
				}
			}
		}
	}
	allDone := func() bool { return len(done) == streams }
	if !h.run(10*time.Minute, allDone) {
		t.Fatalf("seed %d: transfer stalled at %v (client %v %+v)", seed, got, c.Err(), c.Stats())
	}
	for _, id := range ids {
		if got[id] != size {
			t.Fatalf("seed %d: stream %d got %d bytes, want %d", seed, id, got[id], size)
		}
	}
	return h
}

func TestBulkTransferUnderFaults(t *testing.T) {
	seeds := 6
	size := 512 << 10
	if testing.Short() {
		seeds, size = 2, 256<<10
	}
	for _, loss := range []float64{0, 0.01, 0.05, 0.20} {
		for seed := 1; seed <= seeds; seed++ {
			name := fmt.Sprintf("loss=%.2f/seed=%d", loss, seed)
			t.Run(name, func(t *testing.T) {
				link := simnet.Link{Latency: 15 * time.Millisecond, Jitter: 5 * time.Millisecond,
					Loss: loss, Duplicate: 0.05, Reorder: 0.10, Corrupt: 0.01}
				h := transfer(t, uint64(seed)*1000+uint64(loss*100), link, 8, size, nil)
				st := h.conns[0].Stats()
				if loss >= 0.05 && st.PacketsLost == 0 {
					t.Fatalf("no losses detected at loss=%v: %+v", loss, st)
				}
			})
		}
	}
}

func TestBulkTransferCubic(t *testing.T) {
	link := simnet.Link{Latency: 25 * time.Millisecond, Loss: 0.02, Reorder: 0.05}
	h := transfer(t, 77, link, 4, 1<<20, func(h *harness) { h.cfg[0].Congestion = "cubic" })
	if st := h.conns[0].Stats(); st.Congestion != "cubic" {
		t.Fatalf("controller = %q", st.Congestion)
	}
}

// With a limited link and a bounded router queue, the sender should keep the
// pipe mostly full.
func TestBandwidthUtilisation(t *testing.T) {
	const bw = 2_000_000 // bytes/s
	link := simnet.Link{Latency: 20 * time.Millisecond, Bandwidth: bw, QueueBytes: 60_000}
	const size = 4 << 20
	h := transfer(t, 5, link, 1, size, nil)
	elapsed := h.now.Sub(h.start).Seconds()
	ideal := float64(size) / bw
	if util := ideal / elapsed; util < 0.7 {
		t.Fatalf("utilisation %.2f (%.2fs for %.2fs of data): %+v", util, elapsed, ideal, h.conns[0].Stats())
	}
}

func TestPacketNumbersNeverReused(t *testing.T) {
	last := [2]int64{-1, -1}
	link := simnet.Link{Latency: 10 * time.Millisecond, Loss: 0.1, Reorder: 0.1}
	transfer(t, 9, link, 2, 128<<10, func(h *harness) {
		h.sniff = func(from int, pkt []byte) {
			hd, err := wire.ParseHeader(pkt)
			if err != nil {
				t.Fatalf("sent an unparseable packet: %v", err)
			}
			if int64(hd.PacketNumber) <= last[from] {
				t.Fatalf("side %d reused packet number %d (last %d)", from, hd.PacketNumber, last[from])
			}
			last[from] = int64(hd.PacketNumber)
			if len(pkt) > DefaultMaxPacketSize {
				t.Fatalf("oversize packet: %d", len(pkt))
			}
		}
	})
}

// A receiver that stops reading must bound what the sender pushes at it.
func TestFlowControlBoundsBuffering(t *testing.T) {
	h := newHarness(t, 11, cleanLink)
	h.cfg[1].InitialMaxStreamData = 64 << 10
	h.cfg[1].InitialMaxData = 96 << 10
	h.dial()
	h.run(5*time.Second, h.established)
	c, s := h.conns[0], h.conns[1]
	id, _ := c.OpenStream()
	payload := bytes.Repeat([]byte("x"), 1<<20)
	written := 0
	h.onStep = func() {
		if written < len(payload) {
			n, _ := c.StreamWrite(id, payload[written:])
			written += n
		}
	}
	h.run(5*time.Second, func() bool { return false })
	if c.sentData > 64<<10 {
		t.Fatalf("sender sent %d bytes past a 64 KiB stream window", c.sentData)
	}
	if written > c.cfg.MaxSendBuffer {
		t.Fatalf("send buffer grew to %d", written)
	}
	// Resume reading: everything arrives.
	got := 0
	buf := make([]byte, 32<<10)
	h.onStep = func() {
		if written < len(payload) {
			n, _ := c.StreamWrite(id, payload[written:])
			written += n
			if written == len(payload) {
				c.StreamCloseWrite(id)
			}
		}
		for {
			n, err := s.StreamRead(id, buf)
			got += n
			if n == 0 || err != nil {
				break
			}
		}
	}
	if !h.run(time.Minute, func() bool { return got == len(payload) }) {
		cs := c.streams[id]
		ss := s.streams[id]
		t.Fatalf("after resuming reads got %d of %d; client sendMax=%d sendNext=%d sentData=%d sendMaxData=%d; server recvMax=%d readOff=%d needMax=%v recvMaxData=%d ctrl=%d; cstats=%+v", got, len(payload), cs.sendMax, cs.sendNext, c.sentData, c.sendMaxData, ss.recvMax, ss.readOff, ss.needMax, s.recvMaxData, len(s.ctrl), c.Stats())
	}
}

func TestIdleTimeoutAndKeepAlive(t *testing.T) {
	h := newHarness(t, 13, cleanLink)
	h.cfg[0].IdleTimeout, h.cfg[1].IdleTimeout = 2*time.Second, 2*time.Second
	h.dial()
	h.run(5*time.Second, h.established)
	h.run(10*time.Second, func() bool { return false })
	if !errors.Is(h.conns[0].Err(), ErrIdleTimeout) || !errors.Is(h.conns[1].Err(), ErrIdleTimeout) {
		t.Fatalf("errs = %v / %v, want idle timeout", h.conns[0].Err(), h.conns[1].Err())
	}

	h = newHarness(t, 14, cleanLink)
	h.cfg[0].IdleTimeout, h.cfg[1].IdleTimeout = 2*time.Second, 2*time.Second
	h.cfg[0].KeepAlive = 500 * time.Millisecond
	h.dial()
	h.run(5*time.Second, h.established)
	h.run(30*time.Second, func() bool { return false })
	if !h.established() {
		t.Fatalf("keep-alive did not keep the connection: %v / %v", h.conns[0].Err(), h.conns[1].Err())
	}
}

func TestClosePropagatesCode(t *testing.T) {
	h := newHarness(t, 15, cleanLink)
	h.dial()
	h.run(5*time.Second, h.established)
	h.conns[0].Close(CodeApplication+7, "bye")
	h.flush()
	h.run(10*time.Second, func() bool {
		return h.conns[0].State() == StateClosed && h.conns[1].State() == StateClosed
	})
	var ce *CloseError
	if !errors.As(h.conns[1].Err(), &ce) || ce.Code != CodeApplication+7 || ce.Reason != "bye" || !ce.Remote {
		t.Fatalf("server err = %v", h.conns[1].Err())
	}
	if _, err := h.conns[0].OpenStream(); err == nil {
		t.Fatal("OpenStream succeeded on a closed connection")
	}
}

func TestDatagrams(t *testing.T) {
	h := newHarness(t, 16, cleanLink)
	h.dial()
	h.run(5*time.Second, h.established)
	for i := 0; i < 20; i++ {
		if err := h.conns[0].SendDatagram([]byte(fmt.Sprintf("dg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.conns[0].SendDatagram(make([]byte, 5000)); !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversize datagram: %v", err)
	}
	h.flush()
	var got []string
	h.run(2*time.Second, func() bool {
		for {
			d, ok := h.conns[1].RecvDatagram()
			if !ok {
				break
			}
			got = append(got, string(d))
		}
		return len(got) == 20
	})
	if len(got) != 20 || got[0] != "dg-0" || got[19] != "dg-19" {
		t.Fatalf("datagrams: %v", got)
	}
}

// The server must not answer an unvalidated client with more than 3x what
// the client sent, whatever happens.
func TestAntiAmplification(t *testing.T) {
	h := newHarness(t, 17, simnet.Link{Latency: 10 * time.Millisecond})
	h.dial()
	// Deliver the first Initial, then cut the client off entirely.
	h.run(time.Second, func() bool { return h.conns[1] != nil })
	h.links[0].Loss = 1
	h.run(20*time.Second, func() bool { return false })
	s := h.conns[1]
	if s.bytesSent > 3*s.bytesRecv {
		t.Fatalf("server sent %d bytes to an unvalidated client that sent %d", s.bytesSent, s.bytesRecv)
	}
	if s.bytesSent == 0 {
		t.Fatal("server never replied")
	}
}

func TestStatelessReset(t *testing.T) {
	h := newHarness(t, 18, cleanLink)
	h.dial()
	h.run(5*time.Second, h.established)
	// The server "restarts": its state is gone. Whatever the client sends
	// next is answered with a reset.
	srv := h.conns[1]
	h.conns[1] = nil
	h.onStep = nil
	id, _ := h.conns[0].OpenStream()
	h.conns[0].StreamWrite(id, []byte("hello?"))
	h.flush()
	for i := 0; i < 50 && h.conns[0].State() != StateClosed; i++ {
		if len(h.q) == 0 {
			h.step()
			continue
		}
		d := h.q[0]
		h.now = d.at
		h.q = h.q[1:]
		if d.to == 1 {
			hd, err := PeekHeader(d.pkt)
			if err == nil && hd.Type == wire.OneRTTPacket {
				h.conns[0].HandlePacket(StatelessReset(h.cfg[1], hd), h.now)
			}
		}
	}
	if !errors.Is(h.conns[0].Err(), ErrStatelessReset) {
		t.Fatalf("client err = %v, want stateless reset", h.conns[0].Err())
	}
	// A forged reset (wrong cluster key) is ignored.
	h2 := newHarness(t, 19, cleanLink)
	h2.dial()
	h2.run(5*time.Second, h2.established)
	forged := StatelessReset(&Config{ClusterKey: []byte("evil")}, wire.Header{SrcCID: h2.conns[0].LocalCID(), DstCID: h2.conns[0].RemoteCID()})
	h2.conns[0].HandlePacket(forged, h2.now)
	if h2.conns[0].State() != StateEstablished {
		t.Fatal("forged reset closed the connection")
	}
	_ = srv
}

func TestStreamResetAndStopSending(t *testing.T) {
	h := newHarness(t, 20, cleanLink)
	h.dial()
	h.run(5*time.Second, h.established)
	c, s := h.conns[0], h.conns[1]
	id, _ := c.OpenStream()
	c.StreamWrite(id, []byte("partial"))
	h.flush()
	h.run(time.Second, func() bool { return s.StreamReadable(id) })
	c.StreamReset(id, 42)
	h.flush()
	var rerr error
	h.run(time.Second, func() bool {
		buf := make([]byte, 100)
		for {
			n, err := s.StreamRead(id, buf)
			if err != nil {
				rerr = err
				return true
			}
			if n == 0 {
				return false
			}
		}
	})
	var re *StreamResetError
	if !errors.As(rerr, &re) || re.Code != 42 || !re.Remote {
		t.Fatalf("read after reset: %v", rerr)
	}

	id2, _ := c.OpenStream()
	c.StreamWrite(id2, []byte("x"))
	h.flush()
	h.run(time.Second, func() bool { return s.StreamReadable(id2) })
	s.StreamStopSending(id2, 9)
	h.flush()
	var werr error
	h.run(time.Second, func() bool {
		_, werr = c.StreamWrite(id2, []byte("more"))
		return werr != nil
	})
	if !errors.As(werr, &re) || re.Code != 9 || !re.Stop {
		t.Fatalf("write after stop-sending: %v", werr)
	}
}

func TestStreamLimit(t *testing.T) {
	h := newHarness(t, 21, cleanLink)
	h.cfg[1].MaxStreams = 2
	h.dial()
	h.run(5*time.Second, h.established)
	c, s := h.conns[0], h.conns[1]
	a, _ := c.OpenStream()
	b, _ := c.OpenStream()
	if _, err := c.OpenStream(); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("third stream: %v", err)
	}
	// Finish both streams in both directions; the server grants more.
	for _, id := range []StreamID{a, b} {
		c.StreamCloseWrite(id)
	}
	h.flush()
	buf := make([]byte, 10)
	h.onStep = func() {
		for _, id := range []StreamID{a, b} {
			s.StreamRead(id, buf)
			s.StreamCloseWrite(id)
			c.StreamRead(id, buf)
		}
	}
	if !h.run(5*time.Second, func() bool { _, err := c.OpenStream(); return err == nil }) {
		t.Fatalf("stream credit never returned (server streams %d)", len(s.streams))
	}
}

func TestRawRoundTrip(t *testing.T) {
	cfg := &Config{ClusterKey: []byte("k")}
	pkt, err := SealRaw(cfg, []byte("gossip"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenRaw(cfg, pkt)
	if err != nil || string(got) != "gossip" {
		t.Fatalf("got %q %v", got, err)
	}
	if _, err := OpenRaw(&Config{ClusterKey: []byte("other")}, pkt); err == nil {
		t.Fatal("raw packet opened with the wrong cluster key")
	}
	pkt[len(pkt)-1] ^= 1
	if _, err := OpenRaw(cfg, pkt); err == nil {
		t.Fatal("tampered raw packet accepted")
	}
	plain, _ := SealRaw(&Config{}, []byte("open"))
	if got, err := OpenRaw(&Config{}, plain); err != nil || string(got) != "open" {
		t.Fatalf("insecure raw: %q %v", got, err)
	}
}

func FuzzHandlePacket(f *testing.F) {
	h := newHarness(&testing.T{}, 99, cleanLink)
	h.dial()
	h.run(5*time.Second, h.established)
	var seeds [][]byte
	h.sniff = func(_ int, pkt []byte) { seeds = append(seeds, pkt) }
	id, _ := h.conns[0].OpenStream()
	h.conns[0].StreamWrite(id, []byte("seed data"))
	h.flush()
	for _, s := range seeds {
		f.Add(s)
	}
	f.Add([]byte{0x30, 1})
	cfgs := h.cfg
	f.Fuzz(func(t *testing.T, pkt []byte) {
		// Fresh conns per input so state from one input cannot mask another.
		c, err := NewClientConn(cfgs[0], time.Unix(0, 0))
		if err != nil {
			t.Fatal(err)
		}
		c.HandlePacket(pkt, time.Unix(1, 0))
		c.Poll(make([]byte, 2048), time.Unix(1, 0))
		if s, err := NewServerConn(cfgs[1], pkt, time.Unix(0, 0)); err == nil {
			s.HandlePacket(pkt, time.Unix(1, 0))
			s.Poll(make([]byte, 2048), time.Unix(1, 0))
		}
	})
}

func FuzzOpenRaw(f *testing.F) {
	cfg := &Config{ClusterKey: []byte("k")}
	p, _ := SealRaw(cfg, []byte("x"))
	f.Add(p)
	f.Fuzz(func(t *testing.T, pkt []byte) {
		OpenRaw(cfg, pkt)
		OpenRaw(&Config{}, pkt)
	})
}

// FuzzFrames drives the frame handlers behind authentication: the input is
// sealed as a valid 1-RTT payload (insecure mode, so no keys are needed in
// the fuzzer) and delivered to an established server and client.
func FuzzFrames(f *testing.F) {
	f.Add(wire.Stream{StreamID: 0, Offset: 3, Data: []byte("abc"), Fin: true}.Append(nil))
	f.Add(wire.ResetStream{StreamID: 0, Code: 1, FinalSize: 10}.Append(nil))
	f.Add(wire.Ack{Ranges: []wire.AckRange{{Smallest: 0, Largest: 3}}}.Append(nil))
	f.Add(wire.MaxStreams{Max: 1 << 40}.Append(wire.StopSending{StreamID: 4}.Append(nil)))
	f.Fuzz(func(t *testing.T, payload []byte) {
		h := newHarness(t, 5, cleanLink)
		h.cfg[0].Insecure, h.cfg[1].Insecure = true, true
		h.dial()
		h.run(time.Second, h.established)
		for side, c := range h.conns {
			peer := h.conns[1-side]
			hd := wire.Header{Type: wire.OneRTTPacket, Version: wire.Version, DstCID: c.LocalCID(),
				SrcCID: peer.LocalCID(), PacketNumber: 1 << 20}
			pkt := peer.sendProt.seal(nil, hd, payload)
			c.HandlePacket(pkt, h.now)
			buf := make([]byte, 2048)
			for i := 0; i < 64 && c.Poll(buf, h.now) > 0; i++ {
			}
			c.HandleTimeout(h.now.Add(time.Hour))
		}
	})
}
