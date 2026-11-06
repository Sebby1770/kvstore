package raft

import (
	"errors"
	"slices"
)

// RawNode is the application's handle on a Raft node: a pure state machine
// (no goroutines, no clock -- Tick is the clock). See the package docs for
// the Ready contract.
type RawNode struct {
	r        *raft
	prevSoft SoftState
	prevHard HardState
}

// NewRawNode builds a node from cfg, restoring from cfg.Storage.
func NewRawNode(cfg *Config) (*RawNode, error) {
	c := *cfg
	r, err := newRaft(&c)
	if err != nil {
		return nil, err
	}
	return &RawNode{r: r, prevSoft: r.softState(), prevHard: r.hardState()}, nil
}

// Tick advances the logical clock by one tick.
func (rn *RawNode) Tick() { rn.r.tick() }

// Campaign starts an election now.
func (rn *RawNode) Campaign() error { return rn.r.Step(Message{Type: MsgHup}) }

// Propose submits data for replication. Followers forward it to the leader;
// ErrProposalDropped means nobody can take it right now (no leader, or a
// leadership transfer is in progress) -- retry later.
func (rn *RawNode) Propose(data []byte) error {
	return rn.r.Step(Message{Type: MsgProp, From: rn.r.id, Entries: []Entry{{Data: slices.Clone(data)}}})
}

// ProposeConfChange submits a single-server membership change. Only one may
// be in flight; the leader drops a second until the first is applied.
func (rn *RawNode) ProposeConfChange(cc ConfChange) error {
	return rn.r.Step(Message{Type: MsgProp, From: rn.r.id, Entries: []Entry{{Type: EntryConfChange, Data: cc.encode()}}})
}

// Step feeds a message from a peer.
func (rn *RawNode) Step(m Message) error {
	if m.Type.local() || m.Type >= msgTypeCount {
		return ErrStepLocal
	}
	if err := validate(m); err != nil {
		return err
	}
	return rn.r.Step(m)
}

// ErrMalformed rejects a message that no correct peer would send.
var ErrMalformed = errors.New("raft: malformed message")

// validate rejects structurally impossible messages before they reach the
// state machine (peers are authenticated by the transport, but a bug or a
// corrupted frame must not be able to crash a node).
func validate(m Message) error {
	if m.From == "" {
		return ErrMalformed
	}
	// Only forwarded proposals and read requests travel without a term;
	// anything else with term 0 would be mistaken for a local message.
	if m.Term == 0 && m.Type != MsgProp && m.Type != MsgReadIndex {
		return ErrMalformed
	}
	switch m.Type {
	case MsgSnap:
		if m.Snapshot == nil || m.Snapshot.IsEmpty() {
			return ErrMalformed
		}
	case MsgApp:
		prevTerm := m.LogTerm
		for i, e := range m.Entries {
			if e.Index != m.Index+1+uint64(i) || e.Term < prevTerm || e.Term > m.Term {
				return ErrMalformed
			}
			prevTerm = e.Term
		}
	}
	return nil
}

// ReadIndex requests a linearizable read barrier; the answer arrives as a
// ReadState carrying ctx (which must be unique per request).
func (rn *RawNode) ReadIndex(ctx []byte) error {
	return rn.r.Step(Message{Type: MsgReadIndex, Context: slices.Clone(ctx)})
}

// TransferLeadership asks the leader to hand over to transferee.
func (rn *RawNode) TransferLeadership(transferee string) {
	rn.r.Step(Message{Type: MsgTransferLeader, From: transferee})
}

// ReportUnreachable tells the leader a peer could not be reached.
func (rn *RawNode) ReportUnreachable(id string) {
	rn.r.Step(Message{Type: MsgUnreachable, From: id})
}

// ReportSnapshot tells the leader whether a snapshot reached a peer.
func (rn *RawNode) ReportSnapshot(id string, ok bool) {
	rn.r.Step(Message{Type: MsgSnapStatus, From: id, Reject: !ok})
}

// HasReady reports whether Ready would return anything.
func (rn *RawNode) HasReady() bool {
	r := rn.r
	if r.softState() != rn.prevSoft {
		return true
	}
	if hs := r.hardState(); !hs.IsEmpty() && hs != rn.prevHard {
		return true
	}
	return r.log.snap != nil || len(r.log.unstableEntries()) > 0 || len(r.msgs) > 0 ||
		len(r.readStates) > 0 || r.log.hasNextCommitted()
}

// Ready returns the pending work. Hand it back to Advance once done.
func (rn *RawNode) Ready() Ready {
	r := rn.r
	rd := Ready{
		Entries:          slices.Clone(r.log.unstableEntries()),
		CommittedEntries: slices.Clone(r.log.nextCommitted()),
		Messages:         r.msgs,
		ReadStates:       r.readStates,
	}
	if ss := r.softState(); ss != rn.prevSoft {
		rd.SoftState = &ss
	}
	if hs := r.hardState(); hs != rn.prevHard {
		rd.HardState = hs
	}
	if r.log.snap != nil {
		rd.Snapshot = *r.log.snap
	}
	r.msgs, r.readStates = nil, nil
	return rd
}

// Advance acknowledges that rd has been persisted, sent and applied.
// Committed membership changes take effect here.
func (rn *RawNode) Advance(rd Ready) {
	r := rn.r
	if rd.SoftState != nil {
		rn.prevSoft = *rd.SoftState
	}
	if !rd.HardState.IsEmpty() {
		rn.prevHard = rd.HardState
	}
	if !rd.Snapshot.IsEmpty() {
		r.log.stableSnapTo(rd.Snapshot.Index)
	}
	if n := len(rd.Entries); n > 0 {
		last := rd.Entries[n-1]
		r.log.stableTo(last.Index, last.Term)
	}
	if n := len(rd.CommittedEntries); n > 0 {
		for _, e := range rd.CommittedEntries {
			if e.Index <= r.log.applied {
				continue
			}
			r.log.appliedTo(e.Index)
			if e.Type == EntryConfChange {
				if cc, err := DecodeConfChange(e.Data); err == nil {
					r.applyConfChange(cc)
				}
			}
		}
	}
	// A conf change or snapshot may have released reads or commits.
	if r.role == Leader {
		r.releasePendingReads()
	}
}

// Status reports the node's state.
func (rn *RawNode) Status() Status {
	r := rn.r
	cs := r.confState()
	st := Status{ID: r.id, Term: r.term, Vote: r.vote, Lead: r.lead, Role: r.role,
		Commit: r.log.committed, Applied: r.log.applied, LastIndex: r.log.lastIndex(),
		Voters: cs.Voters, Learners: cs.Learners, Transferee: r.leadTransferee}
	if r.role == Leader {
		st.Progress = map[string]Progress{}
		for id, p := range r.prs {
			st.Progress[id] = Progress{Match: p.match, Next: p.next, State: p.state.String(),
				RecentActive: p.recentActive || id == r.id, Learner: p.learner}
		}
	}
	return st
}

// ConfState is the current membership.
func (rn *RawNode) ConfState() ConfState { return rn.r.confState() }
