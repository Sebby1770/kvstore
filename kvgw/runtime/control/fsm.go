package control

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

// FSM is the control-plane state machine. Construct it with NewFSM; feed it
// committed Raft entries in index order through Apply.
type FSM struct {
	mu       sync.RWMutex
	applied  uint64
	ring     RingConfig
	leases   map[string]*Lease
	registry map[string]*RegistryEntry
	queues   map[string]*queue
}

// NewFSM returns an empty state machine (no ring, no leases, no keys, no
// queues) at applied index 0.
func NewFSM() *FSM {
	return &FSM{
		leases:   make(map[string]*Lease),
		registry: make(map[string]*RegistryEntry),
		queues:   make(map[string]*queue),
	}
}

// Applied returns the index of the last applied entry.
func (f *FSM) Applied() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// Apply applies the committed entry at index. It never panics, whatever cmd
// contains: undecodable or invalid commands produce a CodeInvalid result and
// change nothing except the applied index. See the package doc for the
// determinism and index rules.
func (f *FSM) Apply(index uint64, cmd []byte) Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index <= f.applied {
		r := fail(CodeStaleIndex, "index %d already applied (last applied %d)", index, f.applied)
		r.Index = index
		return r
	}
	f.applied = index
	var r Result
	var op string
	switch {
	case len(cmd) == 0:
		op = OpNoop
	case len(cmd) > MaxCommandSize:
		r = fail(CodeInvalid, "command is %d bytes, limit %d", len(cmd), MaxCommandSize)
	default:
		var c Command
		if err := json.Unmarshal(cmd, &c); err != nil {
			r = fail(CodeInvalid, "decode: %v", err)
		} else {
			op = c.Op
			r = f.dispatch(index, &c)
		}
	}
	r.Op = op
	r.Index = index
	return r
}

func (f *FSM) dispatch(idx uint64, c *Command) Result {
	switch c.Op {
	case OpSetRing:
		return f.setRing(idx, c)
	case OpLeaseAcquire:
		return f.leaseAcquire(idx, c)
	case OpLeaseRenew:
		return f.leaseRenew(idx, c)
	case OpLeaseRelease:
		return f.leaseRelease(idx, c)
	case OpRegPut:
		return f.regPut(idx, c)
	case OpRegDelete:
		return f.regDelete(idx, c)
	case OpJobEnqueue:
		return f.jobEnqueue(idx, c)
	case OpJobClaim:
		return f.jobClaim(idx, c)
	case OpJobComplete:
		return f.jobComplete(idx, c)
	case OpJobFail:
		return f.jobFail(idx, c)
	case OpJobExtend:
		return f.jobExtend(idx, c)
	case OpJobRedrive:
		return f.jobRedrive(idx, c)
	case OpJobPurge:
		return f.jobPurge(idx, c)
	case OpNoop:
		return ok()
	}
	return fail(CodeInvalid, "unknown op %q", truncate(c.Op, 64))
}

// ---- read-only queries (linearizable only after a ReadIndex barrier) ----

// Ring returns the current ring config (Epoch 0 if none was ever set).
func (f *FSM) Ring() RingConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ring.clone()
}

// Lease returns the lease record for name. A record may be expired (judge it
// with Lease.ValidAt); released leases are deleted.
func (f *FSM) Lease(name string) (Lease, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	l, ok := f.leases[name]
	if !ok {
		return Lease{}, false
	}
	return *l, true
}

// Leases returns every lease record, sorted by name.
func (f *FSM) Leases() []Lease {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Lease, 0, len(f.leases))
	for _, l := range f.leases {
		out = append(out, *l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Get returns the registry entry for key.
func (f *FSM) Get(key string) (RegistryEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.registry[key]
	if !ok {
		return RegistryEntry{}, false
	}
	return e.clone(), true
}

// Keys returns the registry keys with the given prefix, sorted.
func (f *FSM) Keys(prefix string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []string
	for k := range f.registry {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Queue returns statistics for a queue (zero counts if it does not exist).
func (f *FSM) Queue(name string) QueueStats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	q, ok := f.queues[name]
	if !ok {
		return QueueStats{Name: name}
	}
	return q.stats()
}

// Queues returns the names of all queues, sorted.
func (f *FSM) Queues() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, 0, len(f.queues))
	for n := range f.queues {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Job returns one job by id.
func (f *FSM) Job(queueName, id string) (Job, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	q, ok := f.queues[queueName]
	if !ok {
		return Job{}, false
	}
	j, ok := q.jobs[id]
	if !ok {
		return Job{}, false
	}
	return j.clone(), true
}

// Jobs returns every retained job of a queue (all states), in enqueue order.
func (f *FSM) Jobs(queueName string) []Job {
	f.mu.RLock()
	defer f.mu.RUnlock()
	q, ok := f.queues[queueName]
	if !ok {
		return nil
	}
	return q.sorted(func(*Job) bool { return true })
}

// DeadLetters returns a queue's dead-lettered jobs in the order they died.
func (f *FSM) DeadLetters(queueName string) []Job {
	f.mu.RLock()
	defer f.mu.RUnlock()
	q, ok := f.queues[queueName]
	if !ok {
		return nil
	}
	out := q.sorted(func(j *Job) bool { return j.State == JobDead })
	sort.SliceStable(out, func(a, b int) bool { return out[a].FinishedIndex < out[b].FinishedIndex })
	return out
}

// ---- shared validation and arithmetic ----

func checkName(field, v string, max int) *Result {
	if v == "" {
		r := fail(CodeInvalid, "%s is required", field)
		return &r
	}
	if len(v) > max {
		r := fail(CodeInvalid, "%s is %d bytes, limit %d", field, len(v), max)
		return &r
	}
	return nil
}

func checkNow(now int64) *Result {
	if now <= 0 {
		r := fail(CodeInvalid, "now (proposer time, unix nanos) is required")
		return &r
	}
	return nil
}

// checkMs validates a millisecond duration field against [min, max].
func checkMs(field string, v int64, min int64, max time.Duration) *Result {
	if v < min || v > max.Milliseconds() {
		r := fail(CodeInvalid, "%s=%d out of range [%d, %d]", field, v, min, max.Milliseconds())
		return &r
	}
	return nil
}

// addMs returns t + ms milliseconds, saturating instead of overflowing so a
// command stamped near the end of time cannot wrap an expiry into the past.
func addMs(t int64, ms int64) int64 {
	if ms <= 0 {
		return t
	}
	if ms > math.MaxInt64/int64(time.Millisecond) {
		return math.MaxInt64
	}
	d := ms * int64(time.Millisecond)
	if t > math.MaxInt64-d {
		return math.MaxInt64
	}
	return t + d
}

// truncate shortens s to at most n bytes without splitting a UTF-8 sequence,
// so the result still round-trips through JSON byte-for-byte (encoding/json
// rewrites invalid UTF-8, which would make a restored replica differ).
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// checkInvariants verifies the internal consistency of the state. Tests and
// fuzzers call it after every Apply; Restore calls it on the decoded state.
// The caller must hold f.mu.
func (f *FSM) checkInvariants() error {
	if f.ring.Epoch > 0 {
		if err := validateRing(f.ring.Nodes, f.ring.RF, f.ring.R, f.ring.W); err != nil {
			return fmt.Errorf("ring: %s", err.Error)
		}
		if f.ring.Index > f.applied {
			return fmt.Errorf("ring index %d > applied %d", f.ring.Index, f.applied)
		}
	}
	if len(f.leases) > MaxLeases {
		return fmt.Errorf("%d leases exceeds limit", len(f.leases))
	}
	for name, l := range f.leases {
		if l == nil || l.Name != name || name == "" || l.Holder == "" {
			return fmt.Errorf("lease %q: bad record", name)
		}
		if l.Token == 0 || l.Token > f.applied {
			return fmt.Errorf("lease %q: token %d outside (0, %d]", name, l.Token, f.applied)
		}
		if l.TTLms <= 0 || l.TTLms > MaxLeaseTTL.Milliseconds() {
			return fmt.Errorf("lease %q: ttl %d", name, l.TTLms)
		}
	}
	if len(f.registry) > MaxRegistryKeys {
		return fmt.Errorf("%d registry keys exceeds limit", len(f.registry))
	}
	for k, e := range f.registry {
		if e == nil || e.Key != k || k == "" || len(k) > MaxKeyLen || len(e.Value) > MaxValueLen {
			return fmt.Errorf("registry %q: bad record", k)
		}
		if e.Version == 0 || e.Version > f.applied || e.CreateIndex == 0 || e.CreateIndex > e.Version {
			return fmt.Errorf("registry %q: version %d create %d applied %d", k, e.Version, e.CreateIndex, f.applied)
		}
	}
	if len(f.queues) > MaxQueues {
		return fmt.Errorf("%d queues exceeds limit", len(f.queues))
	}
	for name, q := range f.queues {
		if q == nil || q.name != name || name == "" {
			return fmt.Errorf("queue %q: bad record", name)
		}
		if err := q.check(f.applied); err != nil {
			return fmt.Errorf("queue %q: %w", name, err)
		}
	}
	return nil
}
