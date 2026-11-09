// Package speculate masks tail latency with speculative, epidemic
// execution. Instead of waiting out a slow path until a timeout, a call
// that has not answered within its usual time is started again on another,
// less loaded node; the first copy to finish wins and a cancel signal tells
// every other copy to stop.
//
//   - Gossip spreads every node's load digest (handler pool load, queue
//     depth, observed latency) epidemically: each round a node pushes its
//     own digest and the freshest ones it knows to a few random peers, so
//     the whole cluster learns everyone's state in O(log n) rounds without
//     any central monitor.
//   - Executor ranks candidates by expected latency (local observations
//     where it has them, gossiped digests where it does not, inflated by
//     load and queue depth), runs the call on the best, and hedges to the
//     next best once the first has been out longer than its usual p95 --
//     the "tail at scale" rule, bounded by a hedge budget (a fraction of
//     primaries) so speculation can never multiply load.
//   - The winner cancels the losers twice over: their RPC streams are
//     reset (the server cancels the handler's context), and a cancel datagram
//     carrying the execution ID goes to every node that got a copy, so a
//     copy whose request is still in flight is refused on arrival (Registry
//     keeps a tombstone). Cancellation is epidemic: a cancelled handler's
//     context ends, which cancels any speculative calls it made in turn.
//
// Only idempotent work may be speculated: every copy may run.
package speculate

import (
	"encoding/json"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// Tag is the first byte of speculation datagrams.
const Tag = 0xD3

const (
	msgDigests = 1
	msgCancel  = 2
)

// Digest is one node's advertised state.
type Digest struct {
	Node  string        `json:"n"`
	Addr  string        `json:"a"`
	Seq   uint64        `json:"s"`
	Load  uint64        `json:"l"` // permille of handler capacity in use
	Queue uint64        `json:"q"`
	P50   time.Duration `json:"p50"`
	P99   time.Duration `json:"p99"`
	At    int64         `json:"t"` // origin's clock when issued (ns)
}

// Gossip disseminates digests epidemically. It is safe for concurrent use.
type Gossip struct {
	mu      sync.Mutex
	self    Digest
	table   map[string]Digest // by address
	rng     *rand.Rand
	fanout  int
	maxSend int
	maxAge  time.Duration
	now     func() int64
}

// NewGossip starts dissemination for the node at addr.
func NewGossip(node, addr string, seed uint64) *Gossip {
	return &Gossip{self: Digest{Node: node, Addr: addr}, table: map[string]Digest{},
		rng: rand.New(rand.NewPCG(seed, 0xd3)), fanout: 2, maxSend: 8, maxAge: 30 * time.Second,
		now: func() int64 { return time.Now().UnixNano() }}
}

// SetClock replaces the time source (tests).
func (g *Gossip) SetClock(f func() int64) {
	g.mu.Lock()
	g.now = f
	g.mu.Unlock()
}

// Update publishes this node's current state.
func (g *Gossip) Update(load, queue uint64, p50, p99 time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.self.Seq++
	g.self.Load, g.self.Queue, g.self.P50, g.self.P99, g.self.At = load, queue, p50, p99, g.now()
}

// Outgoing is a datagram to send.
type Outgoing struct {
	To      string
	Payload []byte
}

// Round picks fanout random peers and pushes them our digest plus the
// freshest others we hold.
func (g *Gossip) Round(peers []string) []Outgoing {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expireLocked()
	var targets []string
	for _, p := range peers {
		if p != g.self.Addr {
			targets = append(targets, p)
		}
	}
	if len(targets) == 0 {
		return nil
	}
	g.rng.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })
	targets = targets[:min(g.fanout, len(targets))]
	others := make([]Digest, 0, len(g.table))
	for _, d := range g.table {
		others = append(others, d)
	}
	sort.Slice(others, func(i, j int) bool { return others[i].At > others[j].At })
	batch := append([]Digest{g.self}, others[:min(len(others), g.maxSend-1)]...)
	b, _ := json.Marshal(batch)
	payload := append([]byte{Tag, msgDigests}, b...)
	out := make([]Outgoing, 0, len(targets))
	for _, t := range targets {
		out = append(out, Outgoing{To: t, Payload: payload})
	}
	return out
}

func (g *Gossip) expireLocked() {
	cut := g.now() - int64(g.maxAge)
	for a, d := range g.table {
		if d.At < cut {
			delete(g.table, a)
		}
	}
}

// merge keeps the newest digest per node.
func (g *Gossip) merge(ds []Digest) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, d := range ds {
		if d.Addr == "" || d.Addr == g.self.Addr {
			continue
		}
		if old, ok := g.table[d.Addr]; ok && old.Node == d.Node && old.Seq >= d.Seq {
			continue
		}
		g.table[d.Addr] = d
	}
}

// Get returns the digest known for addr.
func (g *Gossip) Get(addr string) (Digest, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if addr == g.self.Addr {
		return g.self, true
	}
	d, ok := g.table[addr]
	return d, ok
}

// All lists the digests known (including our own), sorted by address.
func (g *Gossip) All() []Digest {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := []Digest{g.self}
	for _, d := range g.table {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}
