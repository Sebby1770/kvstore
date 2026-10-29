package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestElectsOneLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		c := newCluster(t, uint64(n), n)
		c.run(50)
		lead := c.leader()
		if lead == "" {
			t.Fatalf("n=%d: no leader", n)
		}
		leaders := 0
		for _, id := range c.sortedIDs() {
			if c.nodes[id].rn.Status().Role == Leader {
				leaders++
			}
		}
		if leaders != 1 {
			t.Fatalf("n=%d: %d leaders", n, leaders)
		}
	}
}

func TestReplicatesAndCommits(t *testing.T) {
	c := newCluster(t, 1, 3)
	c.run(30)
	lead := c.leader()
	for i := 0; i < 20; i++ {
		if err := c.propose(lead, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.run(5)
	c.checkConverged()
	if got := len(c.nodes["n1"].applied); got != 20 {
		t.Fatalf("applied %d entries", got)
	}
	// Followers forward proposals to the leader.
	for _, id := range c.sortedIDs() {
		if id != lead {
			c.propose(id, "from-"+id)
		}
	}
	c.run(5)
	c.checkConverged()
	if len(c.acked) != 22 {
		t.Fatalf("acknowledged %d of 22", len(c.acked))
	}
}

func TestReelectionAfterLeaderIsolated(t *testing.T) {
	c := newCluster(t, 2, 5)
	c.run(30)
	old := c.leader()
	oldTerm := c.nodes[old].rn.Status().Term
	c.cut = func(a, b string) bool { return a == old || b == old }
	c.run(60)
	lead := c.leader()
	if lead == "" || lead == old {
		// The isolated old leader may still think it leads (until check
		// quorum demotes it); a new one must exist among the others.
		for _, id := range c.sortedIDs() {
			if id != old && c.nodes[id].rn.Status().Role == Leader {
				lead = id
			}
		}
	}
	if lead == "" || lead == old {
		t.Fatal("no new leader among the majority")
	}
	if c.nodes[lead].rn.Status().Term <= oldTerm {
		t.Fatal("new leader did not advance the term")
	}
	// CheckQuorum: the isolated leader stepped down.
	if c.nodes[old].rn.Status().Role == Leader {
		t.Fatal("isolated leader still leads (check quorum)")
	}
	c.cut = nil
	c.run(30)
	c.propose(c.leader(), "after-heal")
	c.run(10)
	c.checkConverged()
}

// PreVote: a node partitioned away for a long time must not inflate its
// term, so when it returns it cannot depose a healthy leader.
func TestPreVotePreventsDisruption(t *testing.T) {
	c := newCluster(t, 3, 3)
	c.run(30)
	lead := c.leader()
	term := c.nodes[lead].rn.Status().Term
	var victim string
	for _, id := range c.sortedIDs() {
		if id != lead {
			victim = id
			break
		}
	}
	c.cut = func(a, b string) bool { return a == victim || b == victim }
	c.run(200)
	if vt := c.nodes[victim].rn.Status().Term; vt != term {
		t.Fatalf("isolated node's term went %d -> %d despite pre-vote", term, vt)
	}
	c.cut = nil
	c.run(30)
	if c.leader() != lead || c.nodes[lead].rn.Status().Term != term {
		t.Fatalf("leader disrupted: was %s/t%d now %s/t%d", lead, term, c.leader(), c.nodes[c.leader()].rn.Status().Term)
	}
}

func TestFollowerLogRepair(t *testing.T) {
	c := newCluster(t, 4, 3)
	c.run(30)
	lead := c.leader()
	// A follower misses a stretch of entries entirely.
	var lag string
	for _, id := range c.sortedIDs() {
		if id != lead {
			lag = id
			break
		}
	}
	c.cut = func(a, b string) bool { return a == lag || b == lag }
	for i := 0; i < 100; i++ {
		c.propose(lead, fmt.Sprintf("x%d", i))
		c.run(1)
	}
	c.cut = nil
	c.run(40)
	c.checkConverged()
}

func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 5, 3)
	c.snapAt = 10
	c.run(30)
	lead := c.leader()
	var lag string
	for _, id := range c.sortedIDs() {
		if id != lead {
			lag = id
			break
		}
	}
	c.crash(lag)
	for i := 0; i < 80; i++ {
		c.propose(lead, fmt.Sprintf("s%d", i))
		c.run(1)
	}
	if fi, _ := c.nodes[lead].store.FirstIndex(); fi < 20 {
		t.Fatalf("leader did not compact (first index %d)", fi)
	}
	c.restart(lag)
	c.run(60)
	c.checkConverged()
	if snap, _ := c.nodes[lag].store.Snapshot(); snap.IsEmpty() {
		t.Fatal("lagging node caught up without a snapshot (test did not exercise InstallSnapshot)")
	}
}

func TestMembershipChanges(t *testing.T) {
	c := newCluster(t, 6, 3)
	c.run(30)
	lead := c.leader()
	// Bring up n4 with empty state and no peers: it waits to be added.
	cfg := c.cfg("n4")
	cfg.Peers = nil
	cfg.Storage = NewMemoryStorage()
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.nodes["n4"] = &tnode{id: "n4", rn: rn, store: cfg.Storage.(*MemoryStorage)}
	if err := c.nodes[lead].rn.ProposeConfChange(ConfChange{Type: AddLearner, Node: "n4"}); err != nil {
		t.Fatal(err)
	}
	// A second change before the first applies is dropped.
	if err := c.nodes[lead].rn.ProposeConfChange(ConfChange{Type: AddVoter, Node: "n5"}); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("concurrent conf change: %v", err)
	}
	c.run(20)
	for i := 0; i < 10; i++ {
		c.propose(lead, fmt.Sprintf("m%d", i))
	}
	c.run(20)
	if got := len(c.nodes["n4"].applied); got != 10 {
		t.Fatalf("learner applied %d entries", got)
	}
	if err := c.nodes[lead].rn.ProposeConfChange(ConfChange{Type: AddVoter, Node: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.run(20)
	if st := c.nodes[lead].rn.Status(); len(st.Voters) != 4 {
		t.Fatalf("voters after promotion: %v", st.Voters)
	}
	// Remove the leader itself: someone else takes over.
	if err := c.nodes[lead].rn.ProposeConfChange(ConfChange{Type: RemoveNode, Node: lead}); err != nil {
		t.Fatal(err)
	}
	c.run(80)
	nl := c.leader()
	if nl == "" || nl == lead {
		t.Fatalf("no leader after removing %s (got %q)", lead, nl)
	}
	if st := c.nodes[nl].rn.Status(); len(st.Voters) != 3 {
		t.Fatalf("voters after removal: %v", st.Voters)
	}
	c.nodes[lead].down = true
	c.propose(nl, "after-removal")
	c.run(10)
	c.checkConverged()
}

func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 7, 3)
	c.run(30)
	lead := c.leader()
	var to string
	for _, id := range c.sortedIDs() {
		if id != lead {
			to = id
			break
		}
	}
	c.nodes[lead].rn.TransferLeadership(to)
	c.run(10)
	if c.leader() != to {
		t.Fatalf("leader is %s, want %s", c.leader(), to)
	}
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 8, 3)
	c.run(30)
	lead := c.leader()
	c.propose(lead, "w1")
	c.run(3)
	commit := c.nodes[lead].rn.Status().Commit
	var follower string
	for _, id := range c.sortedIDs() {
		if id != lead {
			follower = id
			break
		}
	}
	// A follower's read is forwarded and answered with the leader's commit.
	n := c.nodes[follower]
	n.rn.ReadIndex([]byte("r1"))
	var got []ReadState
	for i := 0; i < 20 && len(got) == 0; i++ {
		c.tickAll()
		for _, id := range c.sortedIDs() {
			nn := c.nodes[id]
			if nn.rn.HasReady() {
				rd := nn.rn.Ready()
				if id == follower {
					got = append(got, rd.ReadStates...)
				}
				for _, m := range rd.Messages {
					c.send(m)
				}
				nn.store.Append(rd.Entries)
				if !rd.HardState.IsEmpty() {
					nn.store.SetHardState(rd.HardState)
				}
				nn.rn.Advance(rd)
			}
		}
		c.deliver(len(c.net), false)
	}
	if len(got) != 1 || string(got[0].RequestCtx) != "r1" || got[0].Index < commit {
		t.Fatalf("read states %+v (commit %d)", got, commit)
	}
	// An isolated leader cannot answer: it cannot confirm leadership.
	c.cut = func(a, b string) bool { return a == lead || b == lead }
	c.nodes[lead].rn.ReadIndex([]byte("r2"))
	for i := 0; i < 5; i++ {
		c.tickAll()
		rd := c.nodes[lead].rn.Ready()
		if len(rd.ReadStates) > 0 {
			t.Fatal("isolated leader served a read")
		}
		c.nodes[lead].rn.Advance(rd)
		c.net = nil
	}
}

// Figure 8: a leader must not commit an entry from an earlier term by
// counting replicas -- only by committing an entry of its own term.
func TestNoCommitByCountingOldTerms(t *testing.T) {
	s := NewMemoryStorage()
	s.Append([]Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
	s.SetHardState(HardState{Term: 3, Commit: 1})
	r, err := newRaft(&Config{ID: "a", Storage: s})
	if err != nil {
		t.Fatal(err)
	}
	r.voters = map[string]bool{"a": true, "b": true, "c": true}
	r.becomeCandidate()
	r.becomeLeader() // term 4, appends an empty entry at index 3
	r.prs["b"].match = 2
	if r.maybeCommit() && r.log.committed == 2 {
		t.Fatal("committed an old-term entry by counting replicas")
	}
	r.prs["b"].match = 3
	if !r.maybeCommit() || r.log.committed != 3 {
		t.Fatalf("did not commit the current-term entry (commit %d)", r.log.committed)
	}
}

func TestChaos(t *testing.T) {
	seeds := 120
	rounds := 600
	if testing.Short() {
		seeds, rounds = 40, 300
	}
	terms, acked := 0, 0
	for seed := 1; seed <= seeds; seed++ {
		l, a := chaos(t, uint64(seed), rounds)
		terms += l
		acked += a
	}
	// Guard against a vacuous run: across seeds the faults must actually
	// have forced elections, and the cluster must still have made progress.
	if terms < 2*seeds || acked < seeds*rounds/5 {
		t.Fatalf("chaos too tame: %d leader terms, %d acknowledged proposals over %d seeds", terms, acked, seeds)
	}
	t.Logf("%d seeds: %d leader terms, %d acknowledged proposals", seeds, terms, acked)
}

// chaos runs a 5-node cluster through random partitions, crashes, restarts,
// message loss, duplication and reordering with continuous proposals; the
// harness checks election safety, state machine safety, leader completeness
// and commit monotonicity on every step. After healing, every acknowledged
// proposal must be applied everywhere in the same order.
func chaos(t *testing.T, seed uint64, rounds int) (leaders, acked int) {
	c := newCluster(t, seed, 5)
	c.snapAt = 25
	c.drop, c.dup = 0.05, 0.02
	rng := c.rng
	seq := 0
	for r := 0; r < rounds; r++ {
		switch x := rng.IntN(100); {
		case x < 4:
			// partition into two random groups
			side := map[string]int{}
			for _, id := range c.ids {
				side[id] = rng.IntN(2)
			}
			c.cut = func(a, b string) bool { return side[a] != side[b] }
		case x < 8:
			c.cut = nil
		case x < 11:
			id := c.ids[rng.IntN(len(c.ids))]
			if !c.nodes[id].down {
				c.crash(id)
			}
		case x < 16:
			for _, id := range c.ids {
				if c.nodes[id].down {
					c.restart(id)
					break
				}
			}
		}
		for i := 0; i < 2; i++ {
			id := c.ids[rng.IntN(len(c.ids))]
			if !c.nodes[id].down {
				seq++
				c.propose(id, fmt.Sprintf("s%d-p%d", seed, seq))
			}
		}
		c.tickAll()
		for k := 0; k < 3; k++ {
			for _, id := range c.sortedIDs() {
				c.process(c.nodes[id])
			}
			c.deliver(len(c.net)/2+1, true)
		}
	}
	// Heal everything and let the cluster settle.
	c.cut, c.drop, c.dup = nil, 0, 0
	for _, id := range c.ids {
		if c.nodes[id].down {
			c.restart(id)
		}
	}
	c.run(200)
	lead := c.leader()
	if lead == "" {
		c.fail("no leader after healing")
	}
	c.propose(lead, "final")
	c.run(20)
	c.checkConverged()
	return len(c.leaders), len(c.acked)
}

func TestFileStorageRecovery(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ents []Entry
	for i := uint64(1); i <= 20; i++ {
		ents = append(ents, Entry{Term: 1, Index: i, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	if err := fs.Save(HardState{Term: 1, Vote: "a", Commit: 15}, ents, Snapshot{}); err != nil {
		t.Fatal(err)
	}
	// A conflicting suffix from a new leader overwrites 18..20.
	if err := fs.Save(HardState{Term: 2, Commit: 17}, []Entry{{Term: 2, Index: 18, Data: []byte("x18")}}, Snapshot{}); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	fs, err = OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if li, _ := fs.LastIndex(); li != 18 {
		t.Fatalf("last index after replay %d, want 18 (conflicting suffix truncated)", li)
	}
	hs, _, _ := fs.InitialState()
	if hs.Term != 2 || hs.Commit != 17 {
		t.Fatalf("hard state %+v", hs)
	}
	fs.Close()

	// Torn tail: chop the last record in half.
	wal := filepath.Join(dir, walName)
	info, _ := os.Stat(wal)
	os.Truncate(wal, info.Size()-5)
	fs, err = OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fs.Recovery().TruncatedBytes == 0 {
		t.Fatal("torn tail not detected")
	}
	if li, _ := fs.LastIndex(); li != 18 {
		t.Fatalf("entries lost to a torn hard-state record: last %d", li)
	}
	fs.Close()

	// A flipped bit mid-file truncates from that record on, never past it.
	data, _ := os.ReadFile(wal)
	data[len(data)/2] ^= 0x40
	os.WriteFile(wal, data, 0o600)
	fs, err = OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	li, _ := fs.LastIndex()
	if li >= 18 || li == 0 {
		t.Fatalf("corruption not truncated sensibly: last index %d", li)
	}
	got, _ := fs.Entries(1, li+1, noLimit)
	for i, e := range got {
		if string(e.Data) != fmt.Sprintf("e%d", i+1) {
			t.Fatalf("entry %d corrupted: %q", i+1, e.Data)
		}
	}
	fs.Close()
}

func TestFileStorageSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	fs, _ := OpenFileStorage(dir)
	var ents []Entry
	for i := uint64(1); i <= 50; i++ {
		ents = append(ents, Entry{Term: 1, Index: i, Data: []byte{byte(i)}})
	}
	fs.Save(HardState{Term: 1, Commit: 50}, ents, Snapshot{})
	if _, err := fs.CreateSnapshot(40, ConfState{Voters: []string{"a"}}, []byte("state@40")); err != nil {
		t.Fatal(err)
	}
	if err := fs.Compact(40); err != nil {
		t.Fatal(err)
	}
	fs.Save(HardState{Term: 1, Commit: 51}, []Entry{{Term: 1, Index: 51}}, Snapshot{})
	fs.Close()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snap, _ := fs.Snapshot()
	first, _ := fs.FirstIndex()
	last, _ := fs.LastIndex()
	if snap.Index != 40 || string(snap.Data) != "state@40" || first != 41 || last != 51 {
		t.Fatalf("after reopen: snap %d %q, log [%d, %d]", snap.Index, snap.Data, first, last)
	}
	// An installed snapshot discards the conflicting log after it.
	fs.Save(HardState{Term: 3, Commit: 60}, nil, Snapshot{Index: 60, Term: 3, Conf: ConfState{Voters: []string{"a"}}, Data: []byte("s60")})
	fs.Close()
	fs2, _ := OpenFileStorage(dir)
	defer fs2.Close()
	if s, _ := fs2.Snapshot(); s.Index != 60 {
		t.Fatalf("installed snapshot not durable: %d", s.Index)
	}
	if li, _ := fs2.LastIndex(); li != 60 {
		t.Fatalf("stale entries survived a snapshot install: last %d", li)
	}
}

func TestFileStorageDrivesRaft(t *testing.T) {
	dir := t.TempDir()
	fs, _ := OpenFileStorage(dir)
	rn, err := NewRawNode(&Config{ID: "solo", Peers: []string{"solo"}, Storage: fs})
	if err != nil {
		t.Fatal(err)
	}
	drain := func(rn *RawNode) []string {
		var out []string
		for rn.HasReady() {
			rd := rn.Ready()
			if err := fs.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
				t.Fatal(err)
			}
			for _, e := range rd.CommittedEntries {
				if e.Type == EntryNormal && len(e.Data) > 0 {
					out = append(out, string(e.Data))
				}
			}
			rn.Advance(rd)
		}
		return out
	}
	drain(rn)
	rn.Campaign()
	drain(rn)
	rn.Propose([]byte("a"))
	rn.Propose([]byte("b"))
	if got := drain(rn); fmt.Sprint(got) != "[a b]" {
		t.Fatalf("applied %v", got)
	}
	fs.Close()
	fs, _ = OpenFileStorage(dir)
	rn, err = NewRawNode(&Config{ID: "solo", Storage: fs})
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(rn); fmt.Sprint(got) != "[a b]" {
		t.Fatalf("replayed %v after restart", got)
	}
	fs.Close()
}

func FuzzUnmarshalMessage(f *testing.F) {
	s := Snapshot{Index: 3, Term: 2, Conf: ConfState{Voters: []string{"a"}}, Data: []byte("x")}
	f.Add(Message{Type: MsgApp, From: "a", To: "b", Term: 2, Entries: []Entry{{Term: 1, Index: 1, Data: []byte("d")}}}.Marshal())
	f.Add(Message{Type: MsgSnap, From: "a", To: "b", Snapshot: &s}.Marshal())
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := UnmarshalMessage(b)
		if err != nil {
			return
		}
		again, err := UnmarshalMessage(m.Marshal())
		if err != nil || again.Type != m.Type || len(again.Entries) != len(m.Entries) {
			t.Fatalf("message does not round-trip: %v", err)
		}
		// Feeding it to a node must not panic either.
		rn, _ := NewRawNode(&Config{ID: "b", Peers: []string{"a", "b", "c"}, Storage: NewMemoryStorage()})
		rn.Step(m)
		if rn.HasReady() {
			rn.Advance(rn.Ready())
		}
	})
}
