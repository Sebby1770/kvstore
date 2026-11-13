package vtime

import (
	"container/heap"
	"sort"

	"github.com/Sebby1770/kvstore/kvgw/runtime/hlc"
)

// Sequencer turns events stamped with hybrid-logical-clock timestamps into
// one total order -- the same on every node -- with no coordinator and no
// waiting on wall clocks.
//
// Each member promises, by advancing its watermark, that it will never emit
// an event at or below it (events from one member arrive in order, so its
// latest event is itself such a promise). The frontier is the lowest
// watermark across the members; below it no new event can ever appear. The
// sequencer releases buffered events up to the frontier sorted by
// (timestamp, origin), so every replica applies the same sequence -- a
// serialisable history that respects causality (HLC order does), decided
// locally. External consistency on top (an event committed before another
// started is ordered first in real time too) costs a local commit-wait of
// the clock's ε -- microseconds with disciplined clocks -- see CommitWait.
type Sequencer struct {
	members    map[string]hlc.Timestamp // watermark per member
	pending    eventHeap
	released   hlc.Timestamp
	lastOrigin string
}

// Event is one stamped event.
type Event struct {
	TS      hlc.Timestamp
	Origin  string
	Payload []byte
}

func (a Event) before(b Event) bool {
	if a.TS != b.TS {
		return a.TS < b.TS
	}
	return a.Origin < b.Origin
}

type eventHeap []Event

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].before(h[j]) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)        { *h = append(*h, x.(Event)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// NewSequencer orders events among members.
func NewSequencer(members []string) *Sequencer {
	s := &Sequencer{members: map[string]hlc.Timestamp{}}
	s.SetMembers(members)
	return s
}

// SetMembers changes the membership (a joining member starts at the
// current release point: it cannot place events in the released past).
func (s *Sequencer) SetMembers(members []string) {
	next := map[string]hlc.Timestamp{}
	for _, m := range members {
		if w, ok := s.members[m]; ok {
			next[m] = w
		} else {
			next[m] = s.released
		}
	}
	s.members = next
}

// Watermark advances a member's promise.
func (s *Sequencer) Watermark(member string, ts hlc.Timestamp) {
	if w, ok := s.members[member]; ok && ts > w {
		s.members[member] = ts
	}
}

// Add buffers an event; it also advances its origin's watermark (events
// from one member arrive in order). Events at or below what has already
// been released are rejected: accepting them would rewrite history.
func (s *Sequencer) Add(e Event) bool {
	if _, ok := s.members[e.Origin]; !ok {
		return false
	}
	if e.TS < s.released || e.TS == s.released && e.Origin <= s.lastOrigin {
		return false
	}
	heap.Push(&s.pending, e)
	s.Watermark(e.Origin, e.TS)
	return true
}

// Frontier is the lowest watermark: no event at or below it can still
// arrive.
func (s *Sequencer) Frontier() hlc.Timestamp {
	first := true
	var lo hlc.Timestamp
	for _, w := range s.members {
		if first || w < lo {
			lo, first = w, false
		}
	}
	return lo
}

// Ready releases every buffered event up to the frontier, in order.
func (s *Sequencer) Ready() []Event {
	f := s.Frontier()
	var out []Event
	for len(s.pending) > 0 && s.pending[0].TS <= f {
		e := heap.Pop(&s.pending).(Event)
		s.released, s.lastOrigin = e.TS, e.Origin
		out = append(out, e)
	}
	return out
}

// Pending is how many events wait for the frontier.
func (s *Sequencer) Pending() int { return len(s.pending) }

// Watermarks snapshots the members' promises (sorted by member).
func (s *Sequencer) Watermarks() []struct {
	Member string
	TS     hlc.Timestamp
} {
	out := make([]struct {
		Member string
		TS     hlc.Timestamp
	}, 0, len(s.members))
	for m, w := range s.members {
		out = append(out, struct {
			Member string
			TS     hlc.Timestamp
		}{m, w})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Member < out[j].Member })
	return out
}

// CommitWait is how long to wait after choosing timestamp ts (nanoseconds)
// before making its effects visible, so that any event started afterwards,
// anywhere, gets a later timestamp: until the clock's earliest bound passes
// ts. It is a local wait of at most 2ε -- no messages, no consensus.
func CommitWait(c *Clock, ts int64) int64 {
	if w := ts - c.Interval().Earliest; w > 0 {
		return w
	}
	return 0
}
