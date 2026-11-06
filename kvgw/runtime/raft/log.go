package raft

import (
	"fmt"
	"slices"
)

// raftLog is the log as Raft sees it: the persisted prefix in Storage
// followed by an unstable suffix (entries, or a snapshot, not yet persisted).
type raftLog struct {
	storage Storage

	// unstable
	snap    *Snapshot
	ents    []Entry
	offset  uint64 // index of ents[0] (or where it would be)
	stabled uint64 // highest index persisted (for Ready bookkeeping)

	committed uint64
	applied   uint64
}

func newLog(s Storage) (*raftLog, error) {
	first, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := s.LastIndex()
	if err != nil {
		return nil, err
	}
	l := &raftLog{storage: s, offset: last + 1, stabled: last}
	l.committed = first - 1
	l.applied = first - 1
	return l, nil
}

func (l *raftLog) firstIndex() uint64 {
	if l.snap != nil {
		return l.snap.Index + 1
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastIndex() uint64 {
	if n := len(l.ents); n > 0 {
		return l.offset + uint64(n) - 1
	}
	if l.snap != nil {
		return l.snap.Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) term(i uint64) (uint64, error) {
	// The valid range is [firstIndex-1, lastIndex].
	if i+1 < l.firstIndex() {
		return 0, ErrCompacted
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	if i >= l.offset && i-l.offset < uint64(len(l.ents)) {
		return l.ents[i-l.offset].Term, nil
	}
	if l.snap != nil && i == l.snap.Index {
		return l.snap.Term, nil
	}
	t, err := l.storage.Term(i)
	if err == nil || err == ErrCompacted || err == ErrUnavailable {
		return t, err
	}
	panic(err)
}

func (l *raftLog) zeroTerm(i uint64) uint64 {
	t, err := l.term(i)
	if err != nil {
		return 0
	}
	return t
}

func (l *raftLog) lastTerm() uint64 { return l.zeroTerm(l.lastIndex()) }

func (l *raftLog) matchTerm(i, t uint64) bool {
	got, err := l.term(i)
	return err == nil && got == t
}

// isUpToDate implements the election restriction (§5.4.1).
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

// entries returns [lo, lastIndex] limited by maxSize.
func (l *raftLog) entries(lo, maxSize uint64) ([]Entry, error) {
	if lo > l.lastIndex() {
		return nil, nil
	}
	return l.slice(lo, l.lastIndex()+1, maxSize)
}

func (l *raftLog) slice(lo, hi, maxSize uint64) ([]Entry, error) {
	if lo < l.firstIndex() {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, fmt.Errorf("raft: slice [%d,%d) out of bounds (last %d)", lo, hi, l.lastIndex())
	}
	if lo == hi {
		return nil, nil
	}
	var out []Entry
	if lo < l.offset {
		stored, err := l.storage.Entries(lo, min(hi, l.offset), maxSize)
		if err != nil {
			return nil, err
		}
		if uint64(len(stored)) < min(hi, l.offset)-lo {
			return stored, nil // size-limited
		}
		out = stored
	}
	if hi > l.offset {
		u := l.ents[max(lo, l.offset)-l.offset : hi-l.offset]
		if len(out) > 0 {
			out = append(slices.Clone(out), u...)
		} else {
			out = u
		}
	}
	return limitSize(out, maxSize), nil
}

// findConflict returns the index of the first entry whose term disagrees
// with ours, the index of the first new entry if all overlapping ones agree,
// or 0 if everything is already present.
func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// findConflictByTerm returns the largest index <= index whose term is <=
// term. Leaders use it to skip back a whole term at a time when a follower
// rejects an append.
func (l *raftLog) findConflictByTerm(index, term uint64) uint64 {
	for ; index > 0; index-- {
		t, err := l.term(index)
		if err != nil {
			return index // compacted or unavailable: stop here
		}
		if t <= term {
			return index
		}
	}
	return 0
}

// maybeAppend appends ents after (index, logTerm) if that entry matches,
// truncating any conflicting suffix, and advances the commit index.
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents []Entry) (uint64, bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	lastNew := index + uint64(len(ents))
	if ci := l.findConflict(ents); ci != 0 {
		if ci <= l.committed {
			// Only a buggy or Byzantine peer sends this: refuse it rather
			// than overwrite (or crash on) a committed entry.
			return 0, false
		}
		l.append(ents[ci-(index+1):])
	}
	l.commitTo(min(committed, lastNew))
	return lastNew, true
}

// append adds entries to the unstable log, truncating conflicts.
func (l *raftLog) append(ents []Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	after := ents[0].Index - 1
	if after < l.committed {
		panic(fmt.Sprintf("raft: append after %d is below committed %d", after, l.committed))
	}
	switch {
	case after+1 == l.offset+uint64(len(l.ents)):
		l.ents = append(l.ents, ents...)
	case after+1 <= l.offset:
		// Replaces the whole unstable suffix (and possibly stored entries).
		l.offset = after + 1
		l.ents = slices.Clone(ents)
		if l.stabled > after {
			l.stabled = after
		}
	default:
		l.ents = append(slices.Clone(l.ents[:after+1-l.offset]), ents...)
		if l.stabled > after {
			l.stabled = after
		}
	}
	return l.lastIndex()
}

func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		if i > l.lastIndex() {
			panic(fmt.Sprintf("raft: commit %d beyond last index %d", i, l.lastIndex()))
		}
		l.committed = i
	}
}

func (l *raftLog) maybeCommit(i, term uint64) bool {
	if i > l.committed && l.zeroTerm(i) == term {
		l.commitTo(i)
		return true
	}
	return false
}

func (l *raftLog) unstableEntries() []Entry {
	if len(l.ents) == 0 {
		return nil
	}
	start := max(l.stabled+1, l.offset)
	if start > l.lastIndex() {
		return nil
	}
	return l.ents[start-l.offset:]
}

func (l *raftLog) nextCommitted() []Entry {
	lo := max(l.applied+1, l.firstIndex())
	if l.committed+1 <= lo {
		return nil
	}
	ents, err := l.slice(lo, l.committed+1, 1<<62)
	if err != nil {
		panic(err)
	}
	return ents
}

func (l *raftLog) hasNextCommitted() bool {
	return l.committed > max(l.applied, l.firstIndex()-1)
}

// stableTo records that entries up to i (with term t) are persisted.
func (l *raftLog) stableTo(i, t uint64) {
	if l.zeroTerm(i) != t || i > l.lastIndex() {
		return
	}
	l.stabled = max(l.stabled, i)
	// Drop persisted entries from the unstable buffer once storage has them.
	if i >= l.offset {
		cut := i + 1 - l.offset
		if cut >= uint64(len(l.ents)) {
			l.ents = nil
		} else {
			l.ents = slices.Clone(l.ents[cut:])
		}
		l.offset = i + 1
	}
}

func (l *raftLog) stableSnapTo(i uint64) {
	if l.snap != nil && l.snap.Index == i {
		l.snap = nil
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if i > l.committed || i < l.applied {
		panic(fmt.Sprintf("raft: applied %d out of range [%d, %d]", i, l.applied, l.committed))
	}
	l.applied = i
}

// restore replaces the log with a snapshot.
func (l *raftLog) restore(s Snapshot) {
	cp := s
	l.snap = &cp
	l.ents = nil
	l.offset = s.Index + 1
	l.stabled = s.Index
	l.committed = s.Index
	l.applied = s.Index
}

func (l *raftLog) snapshot() (Snapshot, error) {
	if l.snap != nil {
		return *l.snap, nil
	}
	return l.storage.Snapshot()
}
