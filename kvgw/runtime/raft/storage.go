package raft

import (
	"slices"
	"sync"
)

// Storage is the durable log as Raft reads it. Implementations hold entries
// that have been persisted (Ready.Entries after the application saved them);
// entries not yet persisted live in Raft's own unstable buffer.
type Storage interface {
	// InitialState returns the persisted HardState and ConfState.
	InitialState() (HardState, ConfState, error)
	// Entries returns entries [lo, hi), stopping early (but always returning
	// at least one) once maxSize bytes of data are collected.
	Entries(lo, hi, maxSize uint64) ([]Entry, error)
	// Term returns the term of entry i (FirstIndex-1 is allowed: it is the
	// snapshot's index).
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	Snapshot() (Snapshot, error)
}

// MemoryStorage is an in-memory Storage. ents[0] is a dummy entry carrying
// the snapshot's index and term.
type MemoryStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap Snapshot
	ents []Entry
}

// NewMemoryStorage returns empty storage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{ents: make([]Entry, 1)} }

// InitialState implements Storage.
func (ms *MemoryStorage) InitialState() (HardState, ConfState, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.hs, ms.snap.Conf, nil
}

// SetHardState records the latest HardState.
func (ms *MemoryStorage) SetHardState(hs HardState) error {
	ms.mu.Lock()
	ms.hs = hs
	ms.mu.Unlock()
	return nil
}

func (ms *MemoryStorage) offset() uint64 { return ms.ents[0].Index }
func (ms *MemoryStorage) last() uint64   { return ms.ents[0].Index + uint64(len(ms.ents)) - 1 }

// Entries implements Storage.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]Entry, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	off := ms.offset()
	if lo <= off {
		return nil, ErrCompacted
	}
	if hi > ms.last()+1 {
		return nil, ErrUnavailable
	}
	return limitSize(ms.ents[lo-off:hi-off], maxSize), nil
}

// Term implements Storage.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	off := ms.offset()
	if i < off {
		return 0, ErrCompacted
	}
	if i > ms.last() {
		return 0, ErrUnavailable
	}
	return ms.ents[i-off].Term, nil
}

// LastIndex implements Storage.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.last(), nil
}

// FirstIndex implements Storage.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.offset() + 1, nil
}

// Snapshot implements Storage.
func (ms *MemoryStorage) Snapshot() (Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.snap, nil
}

// ApplySnapshot replaces the storage contents with a snapshot.
func (ms *MemoryStorage) ApplySnapshot(s Snapshot) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if s.Index <= ms.snap.Index {
		return ErrSnapOutOfDate
	}
	ms.snap = s
	ms.ents = []Entry{{Term: s.Term, Index: s.Index}}
	return nil
}

// CreateSnapshot records a snapshot of the state machine as of applied index
// i (which must be in the log) with the given membership.
func (ms *MemoryStorage) CreateSnapshot(i uint64, conf ConfState, data []byte) (Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if i <= ms.snap.Index {
		return Snapshot{}, ErrSnapOutOfDate
	}
	if i > ms.last() {
		return Snapshot{}, ErrUnavailable
	}
	ms.snap = Snapshot{Index: i, Term: ms.ents[i-ms.offset()].Term, Conf: cloneConf(conf), Data: data}
	return ms.snap, nil
}

// Compact discards entries up to and including i (which must be at or below
// the latest snapshot's index for followers to catch up via snapshot).
func (ms *MemoryStorage) Compact(i uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	off := ms.offset()
	if i <= off {
		return ErrCompacted
	}
	if i > ms.last() {
		return ErrUnavailable
	}
	k := i - off
	ents := make([]Entry, 1, uint64(len(ms.ents))-k)
	ents[0] = Entry{Index: ms.ents[k].Index, Term: ms.ents[k].Term}
	ms.ents = append(ents, ms.ents[k+1:]...)
	return nil
}

// Append adds entries, replacing any conflicting suffix.
func (ms *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	first := ms.offset() + 1
	last := entries[len(entries)-1].Index
	if last < first {
		return nil // all compacted already
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	at := entries[0].Index - ms.offset()
	switch {
	case uint64(len(ms.ents)) > at:
		ms.ents = append(slices.Clone(ms.ents[:at]), entries...)
	case uint64(len(ms.ents)) == at:
		ms.ents = append(ms.ents, entries...)
	default:
		return ErrUnavailable // gap
	}
	return nil
}

func cloneConf(c ConfState) ConfState {
	return ConfState{Voters: slices.Clone(c.Voters), Learners: slices.Clone(c.Learners)}
}

func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 {
		return ents
	}
	size := uint64(len(ents[0].Data))
	n := 1
	for ; n < len(ents); n++ {
		size += uint64(len(ents[n].Data))
		if size > maxSize {
			break
		}
	}
	return ents[:n]
}
