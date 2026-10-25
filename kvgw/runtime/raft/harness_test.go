package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
)

// tnode is one simulated server: a RawNode, its "disk" (a MemoryStorage that
// survives crashes), and its state machine (the list of applied payloads).
type tnode struct {
	id      string
	rn      *RawNode
	store   *MemoryStorage
	applied []string // payloads of applied normal entries, in order
	lastIdx uint64   // index of the last applied entry
	down    bool
	commit  uint64
}

// tcluster is a deterministic network of nodes with fault injection and
// invariant checks after every Ready.
type tcluster struct {
	t      *testing.T
	seed   uint64
	rng    *rand.Rand
	nodes  map[string]*tnode
	ids    []string
	net    []Message
	cut    func(from, to string) bool
	drop   float64
	dup    float64
	cfg    func(id string) *Config
	steps  int
	snapAt int // compact a node's log every snapAt applied entries (0 = never)

	committed map[uint64]Entry  // index -> the entry every node must apply there
	leaders   map[uint64]string // term -> leader
	acked     map[string]uint64 // proposal payload -> index, once applied by its proposer
	proposer  map[string]string // payload -> proposing node
}

func newCluster(t *testing.T, seed uint64, n int, opts ...func(*Config)) *tcluster {
	c := &tcluster{t: t, seed: seed, rng: rand.New(rand.NewPCG(seed, 0x4af7)), nodes: map[string]*tnode{},
		committed: map[uint64]Entry{}, leaders: map[uint64]string{}, acked: map[string]uint64{}, proposer: map[string]string{}}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	c.cfg = func(id string) *Config {
		cfg := &Config{ID: id, Peers: c.ids, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
			MaxSizePerMsg: 4096, MaxInflightMsgs: 64, Rand: rand.New(rand.NewPCG(c.rng.Uint64(), 1))}
		for _, o := range opts {
			o(cfg)
		}
		return cfg
	}
	for _, id := range c.ids {
		c.start(id, NewMemoryStorage())
	}
	return c
}

func (c *tcluster) start(id string, store *MemoryStorage) {
	cfg := c.cfg(id)
	cfg.Storage = store
	n := &tnode{id: id, store: store}
	snap, _ := store.Snapshot()
	if !snap.IsEmpty() {
		n.restoreFSM(snap)
		cfg.Applied = snap.Index
	}
	rn, err := NewRawNode(cfg)
	if err != nil {
		c.t.Fatalf("seed %d: start %s: %v", c.seed, id, err)
	}
	n.rn = rn
	n.commit = rn.Status().Commit
	c.nodes[id] = n
}

func (n *tnode) restoreFSM(s Snapshot) {
	n.applied = nil
	if len(s.Data) > 0 {
		json.Unmarshal(s.Data, &n.applied)
	}
	n.lastIdx = s.Index
}

func (c *tcluster) crash(id string) { c.nodes[id].down = true }

// restart brings a crashed node back from its durable storage only.
func (c *tcluster) restart(id string) {
	c.start(id, c.nodes[id].store)
}

func (c *tcluster) fail(format string, args ...any) {
	c.t.Helper()
	c.t.Fatalf("seed %d step %d: %s", c.seed, c.steps, fmt.Sprintf(format, args...))
}

// process handles one Ready for node n, checking invariants.
func (c *tcluster) process(n *tnode) {
	if n.down || !n.rn.HasReady() {
		return
	}
	rd := n.rn.Ready()
	// 1. Persist.
	if !rd.Snapshot.IsEmpty() {
		if err := n.store.ApplySnapshot(rd.Snapshot); err != nil {
			c.fail("%s: apply snapshot: %v", n.id, err)
		}
	}
	if err := n.store.Append(rd.Entries); err != nil {
		c.fail("%s: append: %v", n.id, err)
	}
	if !rd.HardState.IsEmpty() {
		if rd.HardState.Commit < n.commit {
			c.fail("%s: commit went backwards %d -> %d", n.id, n.commit, rd.HardState.Commit)
		}
		n.commit = rd.HardState.Commit
		n.store.SetHardState(rd.HardState)
	}
	// 2. Send.
	for _, m := range rd.Messages {
		c.send(m)
	}
	// 3. Apply.
	if !rd.Snapshot.IsEmpty() {
		n.restoreFSM(rd.Snapshot)
	}
	for _, e := range rd.CommittedEntries {
		if e.Index <= n.lastIdx {
			continue
		}
		if e.Index != n.lastIdx+1 {
			c.fail("%s: applied index %d after %d (gap)", n.id, e.Index, n.lastIdx)
		}
		if prev, ok := c.committed[e.Index]; ok {
			if prev.Term != e.Term || !bytes.Equal(prev.Data, e.Data) {
				c.fail("state machine safety: %s applied (t%d %q) at %d, others applied (t%d %q)",
					n.id, e.Term, e.Data, e.Index, prev.Term, prev.Data)
			}
		} else {
			c.committed[e.Index] = e
		}
		if e.Type == EntryNormal && len(e.Data) > 0 {
			n.applied = append(n.applied, string(e.Data))
			if c.proposer[string(e.Data)] == n.id {
				c.acked[string(e.Data)] = e.Index
			}
		}
		n.lastIdx = e.Index
	}
	// 4. Roles: election safety and leader completeness.
	if rd.SoftState != nil && rd.SoftState.Role == Leader {
		term := n.rn.Status().Term
		if other, ok := c.leaders[term]; ok && other != n.id {
			c.fail("election safety: %s and %s both lead term %d", other, n.id, term)
		}
		c.leaders[term] = n.id
		for idx, e := range c.committed {
			if t, err := n.rn.r.log.term(idx); err == nil && t != e.Term {
				c.fail("leader completeness: new leader %s has t%d at committed index %d (want t%d)", n.id, t, idx, e.Term)
			}
		}
	}
	n.rn.Advance(rd)
	// 5. Occasionally snapshot and compact.
	if c.snapAt > 0 && n.lastIdx > 0 {
		snap, _ := n.store.Snapshot()
		if n.lastIdx-snap.Index >= uint64(c.snapAt) {
			data, _ := json.Marshal(n.applied)
			if _, err := n.store.CreateSnapshot(n.lastIdx, n.rn.ConfState(), data); err == nil {
				n.store.Compact(n.lastIdx)
			}
		}
	}
}

func (c *tcluster) send(m Message) {
	if c.drop > 0 && c.rng.Float64() < c.drop {
		if m.Type == MsgSnap && c.nodes[m.From] != nil {
			c.nodes[m.From].rn.ReportSnapshot(m.To, false)
		}
		return
	}
	// Exercise the wire codec on every message.
	dec, err := UnmarshalMessage(m.Marshal())
	if err != nil {
		c.fail("codec: %v (%+v)", err, m)
	}
	c.net = append(c.net, dec)
	if c.dup > 0 && c.rng.Float64() < c.dup {
		c.net = append(c.net, dec)
	}
}

// deliver hands up to k in-flight messages (random order when shuffle) to
// their recipients.
func (c *tcluster) deliver(k int, shuffle bool) {
	if shuffle {
		c.rng.Shuffle(len(c.net), func(i, j int) { c.net[i], c.net[j] = c.net[j], c.net[i] })
	}
	if k > len(c.net) {
		k = len(c.net)
	}
	batch := c.net[:k]
	c.net = slices.Clone(c.net[k:])
	for _, m := range batch {
		to := c.nodes[m.To]
		if to == nil || to.down || c.nodes[m.From] == nil || c.nodes[m.From].down {
			continue
		}
		if c.cut != nil && c.cut(m.From, m.To) {
			continue
		}
		to.rn.Step(m)
	}
}

func (c *tcluster) sortedIDs() []string {
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// stabilize processes Readies and delivers messages until quiet.
func (c *tcluster) stabilize() {
	for i := 0; i < 1000; i++ {
		busy := false
		for _, id := range c.sortedIDs() {
			n := c.nodes[id]
			if !n.down && n.rn.HasReady() {
				busy = true
				c.process(n)
			}
		}
		if len(c.net) > 0 {
			busy = true
			c.deliver(len(c.net), false)
		}
		if !busy {
			return
		}
	}
}

func (c *tcluster) tickAll() {
	c.steps++
	for _, id := range c.sortedIDs() {
		if n := c.nodes[id]; !n.down {
			n.rn.Tick()
		}
	}
}

// run ticks the cluster for n rounds, stabilising after each.
func (c *tcluster) run(rounds int) {
	for i := 0; i < rounds; i++ {
		c.tickAll()
		c.stabilize()
	}
}

func (c *tcluster) leader() string {
	var lead string
	var term uint64
	for _, id := range c.sortedIDs() {
		n := c.nodes[id]
		if st := n.rn.Status(); !n.down && st.Role == Leader && st.Term >= term {
			lead, term = id, st.Term
		}
	}
	return lead
}

func (c *tcluster) propose(id, data string) error {
	c.proposer[data] = id
	return c.nodes[id].rn.Propose([]byte(data))
}

// checkConverged asserts every live node applied the same sequence and that
// every acknowledged proposal is in it.
func (c *tcluster) checkConverged() {
	c.t.Helper()
	var ref *tnode
	for _, id := range c.sortedIDs() {
		n := c.nodes[id]
		if n.down || !n.rn.r.promotable() && !n.rn.r.learners[id] {
			continue
		}
		if ref == nil {
			ref = n
			continue
		}
		if !slices.Equal(ref.applied, n.applied) || ref.lastIdx != n.lastIdx {
			c.fail("not converged: %s applied %d (idx %d), %s applied %d (idx %d)",
				ref.id, len(ref.applied), ref.lastIdx, n.id, len(n.applied), n.lastIdx)
		}
	}
	if ref == nil {
		return
	}
	// (A payload may legitimately apply twice: a forwarded proposal that the
	// network duplicates is two proposals. Raft promises order, not dedup.)
	have := map[string]bool{}
	for _, d := range ref.applied {
		have[d] = true
	}
	for d := range c.acked {
		if !have[d] {
			c.fail("acknowledged proposal %q lost", d)
		}
	}
}
