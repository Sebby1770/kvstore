package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
)

// Config configures a node.
type Config struct {
	ID string
	// Peers are the initial voters, used only when Storage is empty. A node
	// joining an existing cluster leaves Peers empty and waits to be added.
	Peers []string
	// Learners are initial non-voting members (empty storage only).
	Learners []string
	// ElectionTick: ticks without hearing from a leader before campaigning
	// (randomised in [ElectionTick, 2×ElectionTick)). Default 10.
	ElectionTick int
	// HeartbeatTick: ticks between leader heartbeats. Default 1.
	HeartbeatTick int
	Storage       Storage
	// Applied is the last index the application has applied (on restart).
	Applied uint64
	// PreVote runs a non-binding election first, so a node that was
	// partitioned away cannot disrupt a stable leader by inflating terms.
	PreVote bool
	// CheckQuorum makes a leader step down when it has not heard from a
	// quorum within an election timeout, and lets followers ignore vote
	// requests while they have a live leader (a leader lease).
	CheckQuorum bool
	// MaxSizePerMsg bounds the entry bytes in one append (default 1 MiB).
	MaxSizePerMsg uint64
	// MaxInflightMsgs bounds unacknowledged appends per follower (default 256).
	MaxInflightMsgs int
	// Rand randomises election timeouts (seeded from ID if nil).
	Rand *rand.Rand
}

const noLimit = 1 << 62

type progressState uint8

const (
	stateProbe progressState = iota
	stateReplicate
	stateSnapshot
)

func (s progressState) String() string { return [...]string{"probe", "replicate", "snapshot"}[s] }

type progress struct {
	match, next     uint64
	state           progressState
	probeSent       bool
	pendingSnapshot uint64
	snapWait        int // heartbeat responses seen while waiting on a snapshot
	recentActive    bool
	inflight        []uint64 // last index of each in-flight append
	learner         bool
}

func (p *progress) becomeProbe() {
	if p.state == stateSnapshot {
		p.next = max(p.match+1, p.pendingSnapshot+1)
	} else {
		p.next = p.match + 1
	}
	p.state, p.probeSent, p.pendingSnapshot, p.inflight = stateProbe, false, 0, nil
}

func (p *progress) becomeReplicate() {
	p.state, p.probeSent, p.pendingSnapshot, p.inflight = stateReplicate, false, 0, nil
	p.next = p.match + 1
}

func (p *progress) becomeSnapshot(i uint64) {
	p.state, p.probeSent, p.pendingSnapshot, p.inflight = stateSnapshot, false, i, nil
	p.snapWait = 0
}

func (p *progress) maybeUpdate(n uint64) bool {
	updated := false
	if p.match < n {
		p.match = n
		updated = true
		p.probeSent = false
	}
	p.next = max(p.next, n+1)
	return updated
}

// maybeDecrTo handles a rejection of the append at index rejected; hint is
// where the follower suggests we resume.
func (p *progress) maybeDecrTo(rejected, hint uint64) bool {
	if p.state == stateReplicate {
		if rejected <= p.match {
			return false // stale
		}
		p.next = p.match + 1
		return true
	}
	if p.next-1 != rejected {
		return false // stale
	}
	p.next = max(min(rejected, hint+1), p.match+1, 1)
	p.probeSent = false
	return true
}

func (p *progress) paused(maxInflight int) bool {
	switch p.state {
	case stateProbe:
		return p.probeSent
	case stateReplicate:
		return len(p.inflight) >= maxInflight
	}
	return true
}

func (p *progress) freeTo(i uint64) {
	k := 0
	for k < len(p.inflight) && p.inflight[k] <= i {
		k++
	}
	p.inflight = p.inflight[k:]
}

type readIndexStatus struct {
	req   Message
	index uint64
	acks  map[string]bool
}

type raft struct {
	id   string
	term uint64
	vote string
	lead string
	role Role
	log  *raftLog
	rng  *rand.Rand
	msgs []Message

	voters   map[string]bool
	learners map[string]bool
	prs      map[string]*progress
	votes    map[string]bool

	electionTick, heartbeatTick int
	electionElapsed             int
	heartbeatElapsed            int
	randomizedElectionTimeout   int
	preVote, checkQuorum        bool
	maxMsgSize                  uint64
	maxInflight                 int

	pendingConfIndex uint64
	leadTransferee   string

	readQueue      []string
	readPending    map[string]*readIndexStatus
	readStates     []ReadState
	pendingReadMsg []Message // ReadIndex requests waiting for a current-term commit
}

func newRaft(c *Config) (*raft, error) {
	if c.ID == "" {
		return nil, errors.New("raft: Config.ID is required")
	}
	if c.Storage == nil {
		return nil, errors.New("raft: Config.Storage is required")
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return nil, errors.New("raft: HeartbeatTick must be below ElectionTick")
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = 1 << 20
	}
	if c.MaxInflightMsgs == 0 {
		c.MaxInflightMsgs = 256
	}
	rng := c.Rand
	if rng == nil {
		var h uint64 = 1469598103934665603
		for i := 0; i < len(c.ID); i++ {
			h = (h ^ uint64(c.ID[i])) * 1099511628211
		}
		rng = rand.New(rand.NewPCG(h, 0x7af7))
	}
	l, err := newLog(c.Storage)
	if err != nil {
		return nil, err
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	r := &raft{
		id: c.ID, log: l, rng: rng,
		voters: map[string]bool{}, learners: map[string]bool{}, prs: map[string]*progress{},
		electionTick: c.ElectionTick, heartbeatTick: c.HeartbeatTick,
		preVote: c.PreVote, checkQuorum: c.CheckQuorum,
		maxMsgSize: c.MaxSizePerMsg, maxInflight: c.MaxInflightMsgs,
		readPending: map[string]*readIndexStatus{},
	}
	if l.lastIndex() == 0 && hs.IsEmpty() && len(cs.Voters) == 0 && len(c.Peers)+len(c.Learners) > 0 {
		// Bootstrap: write the initial membership into the log as committed
		// conf-change entries. Every founding peer writes byte-identical
		// entries, so their logs agree, and a node added later learns the
		// full membership by replicating the log -- it is never told only
		// about itself (which would let it elect itself with a quorum of one).
		var boot []Entry
		for _, p := range sortedCopy(c.Peers) {
			boot = append(boot, Entry{Term: 1, Index: uint64(len(boot) + 1), Type: EntryConfChange,
				Data: ConfChange{Type: AddVoter, Node: p}.encode()})
		}
		for _, p := range sortedCopy(c.Learners) {
			boot = append(boot, Entry{Term: 1, Index: uint64(len(boot) + 1), Type: EntryConfChange,
				Data: ConfChange{Type: AddLearner, Node: p}.encode()})
		}
		l.append(boot)
		l.committed = uint64(len(boot))
		hs = HardState{Term: 1, Commit: l.committed}
	}
	for _, v := range cs.Voters {
		r.voters[v] = true
	}
	for _, v := range cs.Learners {
		r.learners[v] = true
	}
	if !hs.IsEmpty() {
		if hs.Commit > l.lastIndex() {
			return nil, fmt.Errorf("raft: hard state commit %d outside [%d, %d]", hs.Commit, l.committed, l.lastIndex())
		}
		l.committed = hs.Commit
		r.term, r.vote = hs.Term, hs.Vote
	}
	// Membership is ConfState from the snapshot plus every committed
	// configuration change after it (including the bootstrap entries).
	if l.committed > l.firstIndex()-1 {
		ents, err := l.slice(l.firstIndex(), l.committed+1, noLimit)
		if err != nil {
			return nil, err
		}
		for _, e := range ents {
			if e.Type == EntryConfChange {
				if cc, err := DecodeConfChange(e.Data); err == nil {
					r.applyConf(cc)
				}
			}
		}
	}
	if c.Applied > 0 {
		if c.Applied > l.committed {
			return nil, fmt.Errorf("raft: Applied %d beyond commit %d", c.Applied, l.committed)
		}
		l.appliedTo(c.Applied)
	}
	r.becomeFollower(r.term, "")
	return r, nil
}

func (r *raft) quorum() int { return len(r.voters)/2 + 1 }

func sortedCopy(xs []string) []string {
	out := slices.Clone(xs)
	sort.Strings(out)
	return slices.Compact(out)
}

func (r *raft) promotable() bool { return r.voters[r.id] }

func (r *raft) sortedPeers() []string {
	var out []string
	for id := range r.voters {
		out = append(out, id)
	}
	for id := range r.learners {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (r *raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func (r *raft) softState() SoftState { return SoftState{Lead: r.lead, Role: r.role} }

func (r *raft) send(m Message) {
	m.From = r.id
	if m.Type == MsgVote || m.Type == MsgVoteResp || m.Type == MsgPreVote || m.Type == MsgPreVoteResp {
		if m.Term == 0 {
			panic("raft: vote message without a term")
		}
	} else if m.Type != MsgProp && m.Type != MsgReadIndex {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

// ---- role transitions ----

func (r *raft) reset(term uint64) {
	if r.term != term {
		r.term = term
		r.vote = ""
	}
	r.lead = ""
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	r.randomizedElectionTimeout = r.electionTick + r.rng.IntN(r.electionTick)
	r.votes = map[string]bool{}
	r.leadTransferee = ""
	r.prs = map[string]*progress{}
	for _, id := range r.sortedPeers() {
		p := &progress{next: r.log.lastIndex() + 1, learner: r.learners[id]}
		if id == r.id {
			p.match = r.log.lastIndex()
		}
		r.prs[id] = p
	}
	r.pendingConfIndex = 0
	r.readQueue, r.readPending = nil, map[string]*readIndexStatus{}
}

func (r *raft) becomeFollower(term uint64, lead string) {
	r.reset(term)
	r.role = Follower
	r.lead = lead
}

func (r *raft) becomePreCandidate() {
	// Pre-vote changes neither term nor vote.
	r.votes = map[string]bool{}
	r.lead = ""
	r.role = PreCandidate
}

func (r *raft) becomeCandidate() {
	r.reset(r.term + 1)
	r.vote = r.id
	r.role = Candidate
}

func (r *raft) becomeLeader() {
	r.reset(r.term)
	r.role = Leader
	r.lead = r.id
	for id, p := range r.prs {
		if id != r.id {
			p.becomeProbe()
		}
		p.recentActive = true
	}
	r.appendEntries(Entry{Type: EntryNormal})
	// Conf changes are blocked until this term's empty entry is applied: a
	// leader must commit an entry of its own term before changing the
	// configuration (the fix for the single-server change bug).
	r.pendingConfIndex = r.log.lastIndex()
}

// ---- elections ----

func (r *raft) hup(transfer bool) {
	if r.role == Leader || !r.promotable() {
		return
	}
	// A committed-but-unapplied conf change that removes us must take
	// effect before we may campaign.
	for _, e := range r.log.nextCommitted() {
		if e.Type != EntryConfChange {
			continue
		}
		if cc, err := DecodeConfChange(e.Data); err == nil && cc.Node == r.id && cc.Type != AddVoter {
			return
		}
	}
	if r.preVote && !transfer {
		r.campaign(true, false)
	} else {
		r.campaign(false, transfer)
	}
}

func (r *raft) campaign(pre, transfer bool) {
	var voteMsg MessageType
	var term uint64
	if pre {
		r.becomePreCandidate()
		voteMsg, term = MsgPreVote, r.term+1
	} else {
		r.becomeCandidate()
		voteMsg, term = MsgVote, r.term
	}
	r.votes[r.id] = true
	if r.tally() == voteWon {
		if pre {
			r.campaign(false, transfer)
		} else {
			r.becomeLeader()
		}
		return
	}
	var ctx []byte
	if transfer {
		ctx = []byte("transfer")
	}
	for _, id := range r.sortedPeers() {
		if id == r.id || !r.voters[id] {
			continue
		}
		r.send(Message{Type: voteMsg, To: id, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(), Context: ctx})
	}
}

type voteResult uint8

const (
	votePending voteResult = iota
	voteWon
	voteLost
)

func (r *raft) tally() voteResult {
	granted, rejected := 0, 0
	for id := range r.voters {
		if v, ok := r.votes[id]; ok {
			if v {
				granted++
			} else {
				rejected++
			}
		}
	}
	switch {
	case granted >= r.quorum():
		return voteWon
	case rejected > len(r.voters)-r.quorum():
		return voteLost
	}
	return votePending
}

// ---- ticking ----

func (r *raft) tick() {
	if r.role == Leader {
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.electionElapsed >= r.electionTick {
			r.electionElapsed = 0
			if r.checkQuorum {
				r.Step(Message{Type: MsgCheckQuorum, From: r.id})
			}
			if r.role == Leader && r.leadTransferee != "" {
				r.leadTransferee = "" // transfer timed out
			}
		}
		if r.role != Leader {
			return
		}
		if r.heartbeatElapsed >= r.heartbeatTick {
			r.heartbeatElapsed = 0
			r.Step(Message{Type: MsgBeat, From: r.id})
		}
		return
	}
	r.electionElapsed++
	if r.promotable() && r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(Message{Type: MsgHup, From: r.id})
	}
}

// ---- stepping ----

// Step advances the state machine with a message.
func (r *raft) Step(m Message) error {
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			force := bytes.Equal(m.Context, []byte("transfer"))
			inLease := r.checkQuorum && r.lead != "" && r.electionElapsed < r.electionTick
			if !force && inLease {
				// We heard from a live leader recently: ignore the
				// disruption without even adopting the term.
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A granted pre-vote is for a term we have not entered yet.
		default:
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, "")
			}
		}
	case m.Term < r.term:
		if (r.checkQuorum || r.preVote) && (m.Type == MsgHeartbeat || m.Type == MsgApp) {
			// A stale leader: tell it about the newer term so it steps down.
			r.send(Message{To: m.From, Type: MsgAppResp})
		} else if m.Type == MsgPreVote {
			r.send(Message{To: m.From, Term: r.term, Type: MsgPreVoteResp, Reject: true})
		}
		return nil
	}

	switch m.Type {
	case MsgHup:
		r.hup(false)
	case MsgVote, MsgPreVote:
		canVote := r.vote == m.From ||
			(r.vote == "" && r.lead == "") ||
			(m.Type == MsgPreVote && m.Term > r.term)
		resp := MsgVoteResp
		if m.Type == MsgPreVote {
			resp = MsgPreVoteResp
		}
		if canVote && r.log.isUpToDate(m.Index, m.LogTerm) {
			r.send(Message{To: m.From, Term: m.Term, Type: resp})
			if m.Type == MsgVote {
				r.electionElapsed = 0
				r.vote = m.From
			}
		} else {
			r.send(Message{To: m.From, Term: r.term, Type: resp, Reject: true})
		}
	default:
		switch r.role {
		case Leader:
			return r.stepLeader(m)
		case Candidate, PreCandidate:
			return r.stepCandidate(m)
		default:
			return r.stepFollower(m)
		}
	}
	return nil
}

func (r *raft) committedInTerm() bool { return r.log.zeroTerm(r.log.committed) == r.term }

func (r *raft) stepLeader(m Message) error {
	switch m.Type {
	case MsgBeat:
		r.bcastHeartbeat(nil)
		return nil
	case MsgCheckQuorum:
		active := 0
		for id, p := range r.prs {
			if !r.voters[id] {
				continue
			}
			if id == r.id || p.recentActive {
				active++
			}
			if id != r.id {
				p.recentActive = false
			}
		}
		if active < r.quorum() {
			r.becomeFollower(r.term, "")
		}
		return nil
	case MsgProp:
		if len(m.Entries) == 0 {
			return errors.New("raft: empty proposal")
		}
		if _, ok := r.prs[r.id]; !ok || r.leadTransferee != "" {
			return ErrProposalDropped
		}
		for i := range m.Entries {
			if m.Entries[i].Type == EntryConfChange {
				if r.pendingConfIndex > r.log.applied {
					return ErrProposalDropped
				}
				r.pendingConfIndex = r.log.lastIndex() + uint64(i) + 1
			}
		}
		r.appendEntries(m.Entries...)
		r.bcastAppend()
		return nil
	case MsgReadIndex:
		r.handleReadIndex(m)
		return nil
	case MsgTransferLeader:
		to := m.From
		if !r.voters[to] || to == r.id || r.leadTransferee == to {
			return nil
		}
		r.leadTransferee = to
		r.electionElapsed = 0
		if r.prs[to].match == r.log.lastIndex() {
			r.send(Message{To: to, Type: MsgTimeoutNow})
		} else {
			r.sendAppend(to)
		}
		return nil
	}

	p := r.prs[m.From]
	if p == nil {
		return nil
	}
	switch m.Type {
	case MsgAppResp:
		p.recentActive = true
		if m.Reject {
			hint := m.RejectHint
			if m.LogTerm > 0 {
				hint = r.log.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if p.maybeDecrTo(m.Index, hint) {
				if p.state == stateReplicate {
					p.becomeProbe()
				}
				r.sendAppend(m.From)
			}
			return nil
		}
		wasPaused := p.paused(r.maxInflight)
		if p.maybeUpdate(m.Index) {
			switch {
			case p.state == stateProbe:
				p.becomeReplicate()
			case p.state == stateSnapshot && p.match >= p.pendingSnapshot:
				p.becomeProbe()
				p.becomeReplicate()
			case p.state == stateReplicate:
				p.freeTo(m.Index)
			}
			if r.maybeCommit() {
				r.releasePendingReads()
				r.bcastAppend()
			} else if wasPaused {
				r.sendAppend(m.From)
			}
			// Keep the pipeline full.
			for p.state == stateReplicate && !p.paused(r.maxInflight) && p.next <= r.log.lastIndex() {
				if !r.sendAppend(m.From) {
					break
				}
			}
			if m.From == r.leadTransferee && p.match == r.log.lastIndex() {
				r.send(Message{To: m.From, Type: MsgTimeoutNow})
			}
		}
	case MsgHeartbeatResp:
		p.recentActive = true
		p.probeSent = false
		if p.state == stateSnapshot {
			// The follower is alive but has not caught up: if no snapshot
			// report arrives for an election timeout, assume it was lost and
			// fall back to probing (which will send a fresh one if needed).
			if p.snapWait++; p.snapWait > r.electionTick {
				p.becomeProbe()
			}
		}
		if p.state == stateReplicate && len(p.inflight) >= r.maxInflight {
			p.inflight = p.inflight[1:]
		}
		if p.match < r.log.lastIndex() {
			r.sendAppend(m.From)
		}
		if len(m.Context) > 0 {
			r.ackRead(m.From, m.Context)
		}
	case MsgSnapStatus:
		if p.state != stateSnapshot {
			return nil
		}
		if m.Reject {
			p.pendingSnapshot = 0
		}
		p.becomeProbe()
		p.probeSent = true // wait for a heartbeat response before probing
	case MsgUnreachable:
		if p.state == stateReplicate {
			p.becomeProbe()
		}
	}
	return nil
}

func (r *raft) stepCandidate(m Message) error {
	want := MsgVoteResp
	if r.role == PreCandidate {
		want = MsgPreVoteResp
	}
	switch m.Type {
	case MsgProp:
		return ErrProposalDropped
	case MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppend(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case want:
		r.votes[m.From] = !m.Reject
		switch r.tally() {
		case voteWon:
			if r.role == PreCandidate {
				r.campaign(false, false)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case voteLost:
			r.becomeFollower(r.term, "")
		}
	case MsgReadIndex:
		return ErrProposalDropped
	}
	return nil
}

func (r *raft) stepFollower(m Message) error {
	switch m.Type {
	case MsgProp:
		if r.lead == "" {
			return ErrProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgTransferLeader:
		if r.lead != "" {
			m.To = r.lead
			r.send(m)
		}
	case MsgTimeoutNow:
		if r.promotable() {
			r.hup(true)
		}
	case MsgReadIndex:
		if r.lead == "" {
			return ErrProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case MsgReadIndexResp:
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Context})
	}
	return nil
}

// ---- replication ----

func (r *raft) appendEntries(es ...Entry) {
	li := r.log.lastIndex()
	for i := range es {
		es[i].Term = r.term
		es[i].Index = li + 1 + uint64(i)
	}
	r.log.append(es)
	if p := r.prs[r.id]; p != nil {
		p.maybeUpdate(r.log.lastIndex())
	}
	r.maybeCommit()
}

// maybeCommit advances the commit index to the highest index replicated on a
// quorum of voters -- but only via an entry of the current term (Figure 8:
// an old-term entry is committed indirectly, by committing a newer one).
func (r *raft) maybeCommit() bool {
	var matches []uint64
	for id := range r.voters {
		if p := r.prs[id]; p != nil {
			matches = append(matches, p.match)
		} else {
			matches = append(matches, 0)
		}
	}
	if len(matches) == 0 {
		return false
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	return r.log.maybeCommit(matches[r.quorum()-1], r.term)
}

// sendAppend sends entries (or a snapshot) to a follower. It reports whether
// anything was sent.
func (r *raft) sendAppend(to string) bool {
	p := r.prs[to]
	if p == nil || p.paused(r.maxInflight) {
		return false
	}
	prevTerm, errT := r.log.term(p.next - 1)
	ents, errE := r.log.entries(p.next, r.maxMsgSize)
	if errT != nil || errE != nil {
		// The follower is behind our compacted log: ship a snapshot.
		if !p.recentActive {
			return false
		}
		snap, err := r.log.snapshot()
		if err != nil || snap.IsEmpty() {
			return false
		}
		r.send(Message{To: to, Type: MsgSnap, Snapshot: &snap})
		p.becomeSnapshot(snap.Index)
		return true
	}
	r.send(Message{To: to, Type: MsgApp, Index: p.next - 1, LogTerm: prevTerm, Entries: ents, Commit: r.log.committed})
	if n := len(ents); n > 0 {
		switch p.state {
		case stateReplicate:
			last := ents[n-1].Index
			p.next = last + 1
			p.inflight = append(p.inflight, last)
		case stateProbe:
			p.probeSent = true
		}
	}
	return true
}

func (r *raft) bcastAppend() {
	for _, id := range r.sortedPeers() {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *raft) bcastHeartbeat(ctx []byte) {
	for _, id := range r.sortedPeers() {
		if id == r.id {
			continue
		}
		p := r.prs[id]
		commit := min(p.match, r.log.committed)
		r.send(Message{To: id, Type: MsgHeartbeat, Commit: commit, Context: ctx})
	}
}

func (r *raft) handleAppend(m Message) {
	if m.Index < r.log.committed {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.log.committed})
		return
	}
	if last, ok := r.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: last})
		return
	}
	hint := min(m.Index, r.log.lastIndex())
	hint = r.log.findConflictByTerm(hint, m.LogTerm)
	r.send(Message{To: m.From, Type: MsgAppResp, Index: m.Index, Reject: true, RejectHint: hint, LogTerm: r.log.zeroTerm(hint)})
}

func (r *raft) handleHeartbeat(m Message) {
	if m.Commit <= r.log.lastIndex() {
		r.log.commitTo(m.Commit)
	}
	r.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m Message) {
	s := *m.Snapshot
	if r.restore(s) {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.log.lastIndex()})
	} else {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.log.committed})
	}
}

// restore installs a snapshot unless we already have its contents.
func (r *raft) restore(s Snapshot) bool {
	if s.Index <= r.log.committed {
		return false
	}
	if r.log.matchTerm(s.Index, s.Term) {
		r.log.commitTo(s.Index) // fast-forward: we already have the entries
		return false
	}
	r.log.restore(s)
	r.voters, r.learners = map[string]bool{}, map[string]bool{}
	for _, v := range s.Conf.Voters {
		r.voters[v] = true
	}
	for _, v := range s.Conf.Learners {
		r.learners[v] = true
	}
	r.prs = map[string]*progress{}
	for _, id := range r.sortedPeers() {
		r.prs[id] = &progress{next: r.log.lastIndex() + 1, learner: r.learners[id]}
	}
	return true
}

// ---- linearizable reads ----

func (r *raft) handleReadIndex(m Message) {
	if len(r.voters) == 1 && r.voters[r.id] {
		r.respondRead(m, r.log.committed)
		return
	}
	if !r.committedInTerm() {
		// We do not know the true commit index until an entry of our own
		// term commits; hold the request until then.
		r.pendingReadMsg = append(r.pendingReadMsg, m)
		return
	}
	key := string(m.Context)
	if _, dup := r.readPending[key]; dup {
		return
	}
	r.readPending[key] = &readIndexStatus{req: m, index: r.log.committed, acks: map[string]bool{r.id: true}}
	r.readQueue = append(r.readQueue, key)
	r.bcastHeartbeat(m.Context)
}

func (r *raft) releasePendingReads() {
	if !r.committedInTerm() || len(r.pendingReadMsg) == 0 {
		return
	}
	msgs := r.pendingReadMsg
	r.pendingReadMsg = nil
	for _, m := range msgs {
		r.handleReadIndex(m)
	}
}

func (r *raft) ackRead(from string, ctx []byte) {
	st, ok := r.readPending[string(ctx)]
	if !ok {
		return
	}
	st.acks[from] = true
	votes := 0
	for id := range st.acks {
		if r.voters[id] {
			votes++
		}
	}
	if votes < r.quorum() {
		return
	}
	// A quorum confirmed our leadership after this request arrived: it and
	// every request queued before it can be answered.
	i := 0
	for ; i < len(r.readQueue); i++ {
		key := r.readQueue[i]
		s := r.readPending[key]
		delete(r.readPending, key)
		r.respondRead(s.req, s.index)
		if key == string(ctx) {
			break
		}
	}
	r.readQueue = r.readQueue[i+1:]
}

func (r *raft) respondRead(req Message, index uint64) {
	if req.From == "" || req.From == r.id {
		r.readStates = append(r.readStates, ReadState{Index: index, RequestCtx: req.Context})
		return
	}
	r.send(Message{To: req.From, Type: MsgReadIndexResp, Index: index, Context: req.Context})
}

// ---- membership ----

func (r *raft) applyConf(cc ConfChange) {
	switch cc.Type {
	case AddVoter:
		delete(r.learners, cc.Node)
		r.voters[cc.Node] = true
	case AddLearner:
		if r.voters[cc.Node] {
			return // demotion is not supported
		}
		r.learners[cc.Node] = true
	case RemoveNode:
		delete(r.voters, cc.Node)
		delete(r.learners, cc.Node)
	}
}

func (r *raft) applyConfChange(cc ConfChange) {
	r.applyConf(cc)
	if r.role != Leader {
		return
	}
	switch cc.Type {
	case AddVoter, AddLearner:
		if _, ok := r.prs[cc.Node]; !ok {
			r.prs[cc.Node] = &progress{next: r.log.lastIndex() + 1, learner: cc.Type == AddLearner, recentActive: true}
		}
		r.prs[cc.Node].learner = cc.Type == AddLearner
	case RemoveNode:
		delete(r.prs, cc.Node)
		if cc.Node == r.id {
			r.becomeFollower(r.term, "")
			return
		}
		if r.leadTransferee == cc.Node {
			r.leadTransferee = ""
		}
	}
	// The quorum may have shrunk.
	if r.maybeCommit() {
		r.bcastAppend()
	}
}

func (r *raft) confState() ConfState {
	var cs ConfState
	for id := range r.voters {
		cs.Voters = append(cs.Voters, id)
	}
	for id := range r.learners {
		cs.Learners = append(cs.Learners, id)
	}
	sort.Strings(cs.Voters)
	sort.Strings(cs.Learners)
	return cs
}
