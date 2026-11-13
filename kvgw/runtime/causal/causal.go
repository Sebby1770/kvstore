// Package causal is causal broadcast over an unreliable, unordered network
// (sans-IO). Messages are sent once, eagerly, as datagrams that may be
// lost, duplicated or reordered; the receiver buffers anything that arrives
// before its causal predecessors and delivers every message exactly once,
// after everything it depends on. Lost messages are repaired by
// anti-entropy: members periodically swap digests (vector clocks) and push
// each other what is missing. Nothing is ever coordinated -- no sequencer,
// no leader, no lock.
//
// This is the delivery layer conflict-free replicated data types need
// (package crdt): with causal delivery, delta mutations from many replicas
// can be applied in whatever order the network produces and still converge
// to one state.
//
// Retransmission buffers are bounded by causal stability, tracked with a
// matrix clock (package vtime): once every member is known to have a
// message, it is dropped from the log.
package causal

import (
	"errors"
	"math/rand/v2"
	"sort"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
	"github.com/Sebby1770/kvstore/kvgw/runtime/vtime"
)

// Tag is the first byte of every causal-broadcast datagram.
const Tag = 0xC5

const (
	msgData   = 1
	msgDigest = 2
)

// Config configures a Node.
type Config struct {
	Self string
	Rand *rand.Rand
	// MaxBuffer bounds out-of-order messages held for missing predecessors
	// (default 4096); beyond it new arrivals are dropped and anti-entropy
	// fetches them again later.
	MaxBuffer int
	// RepairBatch bounds messages pushed in answer to one digest (64).
	RepairBatch int
	// MaxPayload bounds a message's payload (default 1000 bytes: one
	// datagram with room for the header).
	MaxPayload int
}

// VC is a vector clock: origin -> highest sequence.
type VC map[string]uint64

// Delivery is one message handed to the application.
type Delivery struct {
	Origin  string
	Seq     uint64
	Deps    VC // what the origin had delivered when it sent this
	Payload []byte
}

// Outgoing is a datagram for the driver to send.
type Outgoing struct {
	To      string
	Payload []byte
}

// Stats counts delivery-layer activity.
type Stats struct {
	Broadcast  uint64 `json:"broadcast"`
	Delivered  uint64 `json:"delivered"`
	Duplicates uint64 `json:"duplicates"`
	Buffered   int    `json:"buffered"` // waiting for predecessors now
	Overflow   uint64 `json:"overflow"` // dropped for a full buffer
	Repaired   uint64 `json:"repaired"` // messages pushed by anti-entropy
	Logged     int    `json:"logged"`   // retained for repair now
	Collected  uint64 `json:"collected"`
	Malformed  uint64 `json:"malformed"`
}

type key struct {
	origin string
	seq    uint64
}

// Node is one member's causal-broadcast state machine.
type Node struct {
	cfg       Config
	seq       uint64
	delivered VC
	buffer    map[string]map[uint64]*Delivery
	nbuf      int
	log       map[string]map[uint64][]byte // encoded DATA, kept until stable
	nlog      int
	matrix    *vtime.Matrix
	peers     []string
	out       []Outgoing
	ready     []Delivery
	stats     Stats
}

// ErrTooLarge reports a payload that will not fit one datagram.
var ErrTooLarge = errors.New("causal: payload too large for one datagram")

// New starts a node.
func New(cfg Config) *Node {
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewPCG(1, 2))
	}
	if cfg.MaxBuffer <= 0 {
		cfg.MaxBuffer = 4096
	}
	if cfg.RepairBatch <= 0 {
		cfg.RepairBatch = 64
	}
	if cfg.MaxPayload <= 0 {
		cfg.MaxPayload = 1000
	}
	return &Node{cfg: cfg, delivered: VC{}, buffer: map[string]map[uint64]*Delivery{},
		log: map[string]map[uint64][]byte{}, matrix: vtime.NewMatrix(cfg.Self)}
}

// SetPeers sets the other members (the driver feeds it from membership).
// Departed members stop holding garbage collection back.
func (n *Node) SetPeers(peers []string) {
	var ps []string
	for _, p := range peers {
		if p != n.cfg.Self {
			ps = append(ps, p)
		}
	}
	sort.Strings(ps)
	n.peers = ps
	n.matrix.SetMembers(ps)
}

// Peers is the current peer list.
func (n *Node) Peers() []string { return append([]string(nil), n.peers...) }

// Delivered is this node's vector clock (a copy).
func (n *Node) Delivered() VC {
	out := VC{}
	for k, v := range n.delivered {
		out[k] = v
	}
	return out
}

// Broadcast sends payload to every member. It is delivered locally at once
// and the delivery returned (it does not appear in Deliveries).
func (n *Node) Broadcast(payload []byte) (Delivery, error) {
	if len(payload) > n.cfg.MaxPayload {
		return Delivery{}, ErrTooLarge
	}
	n.seq++
	d := Delivery{Origin: n.cfg.Self, Seq: n.seq, Deps: n.Delivered(), Payload: append([]byte(nil), payload...)}
	delete(d.Deps, n.cfg.Self)
	enc := encodeData(d)
	n.retain(d.Origin, d.Seq, enc)
	n.delivered[n.cfg.Self] = n.seq
	n.matrix.Observe(n.cfg.Self, n.cfg.Self, n.seq)
	for _, p := range n.peers {
		n.out = append(n.out, Outgoing{To: p, Payload: enc})
	}
	n.stats.Broadcast++
	return d, nil
}

func (n *Node) retain(origin string, seq uint64, enc []byte) {
	m := n.log[origin]
	if m == nil {
		m = map[uint64][]byte{}
		n.log[origin] = m
	}
	if _, ok := m[seq]; !ok {
		m[seq] = enc
		n.nlog++
	}
}

// Handle processes one datagram from a peer.
func (n *Node) Handle(from string, b []byte) error {
	if len(b) < 2 || b[0] != Tag {
		n.stats.Malformed++
		return errMalformed
	}
	switch b[1] {
	case msgData:
		d, err := decodeData(b)
		if err != nil {
			n.stats.Malformed++
			return err
		}
		n.receive(d, b)
	case msgDigest:
		sender, vc, err := decodeDigest(b)
		if err != nil {
			n.stats.Malformed++
			return err
		}
		n.handleDigest(sender, vc)
	default:
		n.stats.Malformed++
		return errMalformed
	}
	return nil
}

func (n *Node) receive(d Delivery, enc []byte) {
	if d.Origin == n.cfg.Self || d.Seq <= n.delivered[d.Origin] {
		n.stats.Duplicates++
		return
	}
	if q := n.buffer[d.Origin]; q != nil {
		if _, dup := q[d.Seq]; dup {
			n.stats.Duplicates++
			return
		}
	}
	if n.deliverable(&d) {
		n.deliver(d, enc)
		n.drain()
		return
	}
	if n.nbuf >= n.cfg.MaxBuffer {
		n.stats.Overflow++
		return
	}
	q := n.buffer[d.Origin]
	if q == nil {
		q = map[uint64]*Delivery{}
		n.buffer[d.Origin] = q
	}
	q[d.Seq] = &d
	n.nbuf++
}

func (n *Node) deliverable(d *Delivery) bool {
	if d.Seq != n.delivered[d.Origin]+1 {
		return false
	}
	for o, s := range d.Deps {
		if o != d.Origin && s > n.delivered[o] {
			return false
		}
	}
	return true
}

func (n *Node) deliver(d Delivery, enc []byte) {
	n.delivered[d.Origin] = d.Seq
	n.matrix.Observe(n.cfg.Self, d.Origin, d.Seq)
	n.matrix.Observe(d.Origin, d.Origin, d.Seq)
	n.matrix.MergeRow(d.Origin, d.Deps)
	if enc == nil {
		enc = encodeData(d)
	}
	n.retain(d.Origin, d.Seq, enc)
	n.ready = append(n.ready, d)
	n.stats.Delivered++
}

// drain delivers buffered messages that have become deliverable, until no
// more do (one delivery can unlock a chain across origins).
func (n *Node) drain() {
	for progress := true; progress; {
		progress = false
		for origin, q := range n.buffer {
			next := n.delivered[origin] + 1
			d, ok := q[next]
			if !ok || !n.deliverable(d) {
				continue
			}
			delete(q, next)
			n.nbuf--
			if len(q) == 0 {
				delete(n.buffer, origin)
			}
			n.deliver(*d, nil)
			progress = true
		}
	}
}

// Tick runs anti-entropy (send our digest to one random peer) and garbage
// collects stable messages. Call it periodically.
func (n *Node) Tick() {
	n.collect()
	if len(n.peers) == 0 {
		return
	}
	p := n.peers[n.cfg.Rand.IntN(len(n.peers))]
	n.out = append(n.out, Outgoing{To: p, Payload: encodeDigest(n.cfg.Self, n.delivered)})
}

func (n *Node) handleDigest(sender string, vc VC) {
	n.matrix.MergeRow(sender, vc)
	// Push what the sender is missing, oldest first per origin.
	sent := 0
	origins := make([]string, 0, len(n.log))
	for o := range n.log {
		origins = append(origins, o)
	}
	sort.Strings(origins)
	for _, o := range origins {
		for s := vc[o] + 1; s <= n.delivered[o] && sent < n.cfg.RepairBatch; s++ {
			if enc, ok := n.log[o][s]; ok {
				n.out = append(n.out, Outgoing{To: sender, Payload: enc})
				sent++
			}
		}
	}
	n.stats.Repaired += uint64(sent)
	// If the sender knows more than we do, show it our digest so it pushes
	// to us in turn.
	for o, s := range vc {
		if s > n.delivered[o] {
			n.out = append(n.out, Outgoing{To: sender, Payload: encodeDigest(n.cfg.Self, n.delivered)})
			break
		}
	}
}

func (n *Node) collect() {
	for origin, m := range n.log {
		stable := n.matrix.Stable(origin)
		for s := range m {
			if s <= stable {
				delete(m, s)
				n.nlog--
				n.stats.Collected++
			}
		}
		if len(m) == 0 {
			delete(n.log, origin)
		}
	}
}

// Outbox drains the datagrams to send.
func (n *Node) Outbox() []Outgoing {
	out := n.out
	n.out = nil
	return out
}

// Deliveries drains the messages delivered (in causal order).
func (n *Node) Deliveries() []Delivery {
	out := n.ready
	n.ready = nil
	return out
}

// Stats snapshots the counters.
func (n *Node) Stats() Stats {
	s := n.stats
	s.Buffered, s.Logged = n.nbuf, n.nlog
	return s
}

// ---- codec ----

var errMalformed = errors.New("causal: malformed datagram")

func appendStr(b []byte, s string) []byte {
	b = wire.AppendVarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendVC(b []byte, vc VC) []byte {
	keys := make([]string, 0, len(vc))
	for k := range vc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b = wire.AppendVarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = appendStr(b, k)
		b = wire.AppendVarint(b, vc[k])
	}
	return b
}

func encodeData(d Delivery) []byte {
	b := []byte{Tag, msgData}
	b = appendStr(b, d.Origin)
	b = wire.AppendVarint(b, d.Seq)
	b = appendVC(b, d.Deps)
	b = wire.AppendVarint(b, uint64(len(d.Payload)))
	return append(b, d.Payload...)
}

func encodeDigest(sender string, vc VC) []byte {
	b := []byte{Tag, msgDigest}
	b = appendStr(b, sender)
	return appendVC(b, vc)
}

type reader struct {
	b   []byte
	err bool
}

func (r *reader) varint() uint64 {
	if r.err {
		return 0
	}
	v, n, err := wire.ParseVarint(r.b)
	if err != nil {
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes(n uint64) []byte {
	if r.err || n > uint64(len(r.b)) {
		r.err = true
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) str() string { return string(r.bytes(r.varint())) }

func (r *reader) vc() VC {
	n := r.varint()
	if n > 1024 {
		r.err = true
		return nil
	}
	vc := VC{}
	for i := uint64(0); i < n && !r.err; i++ {
		k := r.str()
		vc[k] = r.varint()
	}
	return vc
}

func decodeData(b []byte) (Delivery, error) {
	r := &reader{b: b[2:]}
	d := Delivery{Origin: r.str(), Seq: r.varint(), Deps: r.vc()}
	d.Payload = append([]byte(nil), r.bytes(r.varint())...)
	if r.err || len(r.b) != 0 || d.Seq == 0 || d.Origin == "" {
		return Delivery{}, errMalformed
	}
	return d, nil
}

func decodeDigest(b []byte) (string, VC, error) {
	r := &reader{b: b[2:]}
	s, vc := r.str(), r.vc()
	if r.err || len(r.b) != 0 || s == "" {
		return "", nil, errMalformed
	}
	return s, vc, nil
}
