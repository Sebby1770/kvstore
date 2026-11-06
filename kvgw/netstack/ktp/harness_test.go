package ktp

import (
	"container/heap"
	"crypto/ed25519"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

// harness wires a client and a server Conn through a seeded, lossy,
// virtual-time link. Everything -- keys, connection IDs, losses, delays -- is
// drawn from the seed, so a failing seed replays exactly.
type harness struct {
	t      *testing.T
	seed   uint64
	now    time.Time
	start  time.Time
	rng    *rand.Rand
	cfg    [2]*Config
	conns  [2]*Conn // 0 = client, 1 = server
	links  [2]simnet.Link
	busy   [2]time.Time
	q      deliveryHeap
	seq    uint64
	buf    []byte
	sniff  func(from int, pkt []byte)
	onStep func()
	sent   [2]int // datagrams sent per side
}

type delivery struct {
	at  time.Time
	seq uint64
	to  int
	pkt []byte
}

type deliveryHeap []delivery

func (h deliveryHeap) Len() int { return len(h) }
func (h deliveryHeap) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}
func (h deliveryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *deliveryHeap) Push(x any)   { *h = append(*h, x.(delivery)) }
func (h *deliveryHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func testIdentity(rng *rand.Rand) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(rng.IntN(256))
	}
	return ed25519.NewKeyFromSeed(seed)
}

func testConfig(rng *rand.Rand) *Config {
	var s [32]byte
	for i := range s {
		s[i] = byte(rng.IntN(256))
	}
	return &Config{Identity: testIdentity(rng), ClusterKey: []byte("test-cluster-key"), Rand: rand.NewChaCha8(s)}
}

func newHarness(t *testing.T, seed uint64, link simnet.Link) *harness {
	rng := rand.New(rand.NewPCG(seed, 0x6b7470))
	h := &harness{t: t, seed: seed, rng: rng, buf: make([]byte, 65536)}
	h.now = time.Unix(1_700_000_000, 0)
	h.start = h.now
	h.cfg[0], h.cfg[1] = testConfig(rng), testConfig(rng)
	h.links[0], h.links[1] = link, link
	return h
}

func (h *harness) dial() {
	h.t.Helper()
	c, err := NewClientConn(h.cfg[0], h.now)
	if err != nil {
		h.t.Fatalf("seed %d: dial: %v", h.seed, err)
	}
	h.conns[0] = c
	h.flush()
}

// flush polls both sides until neither has anything to send, putting each
// datagram on the wire.
func (h *harness) flush() {
	for progress := true; progress; {
		progress = false
		for side, c := range h.conns {
			if c == nil {
				continue
			}
			for {
				n := c.Poll(h.buf, h.now)
				if n == 0 {
					break
				}
				progress = true
				h.transmit(side, append([]byte(nil), h.buf[:n]...))
			}
		}
	}
}

func (h *harness) transmit(from int, pkt []byte) {
	h.sent[from]++
	if h.sniff != nil {
		h.sniff(from, pkt)
	}
	l := h.links[from]
	depart := h.now
	if l.Bandwidth > 0 {
		if h.busy[from].After(depart) {
			depart = h.busy[from]
		}
		if l.QueueBytes > 0 && depart.Sub(h.now) > l.TransmitTime(l.QueueBytes) {
			return // router queue full: tail drop
		}
		h.busy[from] = depart.Add(l.TransmitTime(len(pkt)))
		depart = h.busy[from]
	}
	copies, corrupt := l.Fate(h.rng, len(pkt))
	if corrupt {
		simnet.CorruptPacket(h.rng, pkt)
	}
	for _, d := range copies {
		h.seq++
		heap.Push(&h.q, delivery{at: depart.Add(d), seq: h.seq, to: 1 - from, pkt: pkt})
	}
}

func (h *harness) deliver(d delivery) {
	if d.to == 1 && h.conns[1] == nil {
		c, err := NewServerConn(h.cfg[1], d.pkt, h.now)
		if err != nil {
			return // corrupted or not an Initial
		}
		h.conns[1] = c
		return
	}
	if c := h.conns[d.to]; c != nil {
		c.HandlePacket(d.pkt, h.now)
	}
}

// step advances virtual time to the next delivery or timer and processes it.
// It returns false when nothing is scheduled.
func (h *harness) step() bool { return h.stepBefore(time.Time{}) }

// stepBefore is step, except that it will not jump past limit (if set): it
// parks the clock at limit and reports false instead.
func (h *harness) stepBefore(limit time.Time) bool {
	next, ok := time.Time{}, false
	if len(h.q) > 0 {
		next, ok = h.q[0].at, true
	}
	for _, c := range h.conns {
		if c == nil {
			continue
		}
		if t, has := c.Timeout(); has && (!ok || t.Before(next)) {
			next, ok = t, true
		}
	}
	if !ok {
		return false
	}
	if !limit.IsZero() && next.After(limit) {
		h.now = limit
		return false
	}
	if next.After(h.now) {
		h.now = next
	}
	for len(h.q) > 0 && !h.q[0].at.After(h.now) {
		h.deliver(heap.Pop(&h.q).(delivery))
	}
	for _, c := range h.conns {
		if c == nil {
			continue
		}
		if t, has := c.Timeout(); has && !t.After(h.now) {
			c.HandleTimeout(h.now)
		}
	}
	if h.onStep != nil {
		h.onStep()
	}
	h.flush()
	return true
}

// run steps until done() or the virtual deadline; it reports success.
func (h *harness) run(limit time.Duration, done func() bool) bool {
	deadline := h.now.Add(limit)
	// Let the application act first: it may have work (a read that frees
	// credit) that no network event would otherwise prompt.
	if h.onStep != nil {
		h.onStep()
		h.flush()
	}
	for !done() {
		if !h.stepBefore(deadline) {
			return done()
		}
	}
	return true
}

func (h *harness) established() bool {
	return h.conns[0] != nil && h.conns[1] != nil && h.conns[0].Established() && h.conns[1].Established()
}
