package node

import (
	"sort"
	"sync"

	"github.com/Sebby1770/kvstore/kvgw/runtime/hlc"
)

// Event is one entry in a node's cluster event log: a membership change, a
// leader election, a ring reconfiguration, an actor activation. Each carries
// a hybrid-logical-clock timestamp, so logs pulled from different nodes can be
// merged into one causally consistent timeline (a node folds the HLC of every
// RPC it receives into its own clock, so an effect is always stamped after its
// cause, even across machines whose wall clocks disagree).
type Event struct {
	HLC    string `json:"hlc"`
	TS     uint64 `json:"ts"`
	Kind   string `json:"kind"` // dotted: swim.join, raft.leader, ring.epoch, actor.activate, ...
	Detail string `json:"detail"`
}

// eventLog is a bounded ring of recent events.
type eventLog struct {
	mu    sync.Mutex
	clock *hlc.Clock
	buf   []Event
	next  int
	full  bool
}

func newEventLog(clock *hlc.Clock, size int) *eventLog {
	if size <= 0 {
		size = 256
	}
	return &eventLog{clock: clock, buf: make([]Event, size)}
}

func (l *eventLog) add(kind, detail string) Event {
	ts := l.clock.Now()
	e := Event{HLC: ts.String(), TS: uint64(ts), Kind: kind, Detail: detail}
	l.mu.Lock()
	l.buf[l.next] = e
	l.next = (l.next + 1) % len(l.buf)
	if l.next == 0 {
		l.full = true
	}
	l.mu.Unlock()
	return e
}

// recent returns up to n events, oldest first, in HLC order.
func (l *eventLog) recent(n int) []Event {
	l.mu.Lock()
	var out []Event
	if l.full {
		out = append(out, l.buf[l.next:]...)
	}
	out = append(out, l.buf[:l.next]...)
	l.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool { return out[i].TS < out[j].TS })
	if n > 0 && len(out) > n {
		out = out[len(out)-n:]
	}
	return out
}
