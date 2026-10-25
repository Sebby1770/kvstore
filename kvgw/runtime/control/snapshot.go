package control

import (
	"encoding/json"
	"fmt"
	"sort"
)

// snapshotV1 is the serialised FSM. Every collection is a sorted slice, so
// two replicas holding the same state produce byte-identical snapshots -- the
// determinism tests compare them directly.
type snapshotV1 struct {
	Version  int             `json:"version"`
	Applied  uint64          `json:"applied"`
	Ring     RingConfig      `json:"ring"`
	Leases   []Lease         `json:"leases"`
	Registry []RegistryEntry `json:"registry"`
	Queues   []queueSnap     `json:"queues"`
}

type queueSnap struct {
	Name         string `json:"name"`
	Seq          uint64 `json:"seq"`
	Jobs         []Job  `json:"jobs"`
	Enqueued     uint64 `json:"enqueued"`
	Claims       uint64 `json:"claims"`
	Reclaims     uint64 `json:"reclaims"`
	Completions  uint64 `json:"completions"`
	Failures     uint64 `json:"failures"`
	DeadLettered uint64 `json:"dead_lettered"`
}

// Snapshot serialises the whole state machine (including the applied index).
func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshotV1{Version: 1, Applied: f.applied, Ring: f.ring.clone()}
	for _, l := range f.leases {
		s.Leases = append(s.Leases, *l)
	}
	sort.Slice(s.Leases, func(i, j int) bool { return s.Leases[i].Name < s.Leases[j].Name })
	for _, e := range f.registry {
		s.Registry = append(s.Registry, e.clone())
	}
	sort.Slice(s.Registry, func(i, j int) bool { return s.Registry[i].Key < s.Registry[j].Key })
	for _, q := range f.queues {
		s.Queues = append(s.Queues, queueSnap{
			Name: q.name, Seq: q.seq, Jobs: q.sorted(func(*Job) bool { return true }),
			Enqueued: q.enqueued, Claims: q.claims, Reclaims: q.reclaims,
			Completions: q.completions, Failures: q.failures, DeadLettered: q.deadLettered,
		})
	}
	sort.Slice(s.Queues, func(i, j int) bool { return s.Queues[i].Name < s.Queues[j].Name })
	return json.Marshal(s)
}

// Restore replaces the state with a snapshot. On error the FSM is unchanged.
func (f *FSM) Restore(b []byte) error {
	var s snapshotV1
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("control: restore: %w", err)
	}
	if s.Version != 1 {
		return fmt.Errorf("control: restore: unsupported snapshot version %d", s.Version)
	}
	n := NewFSM()
	n.applied = s.Applied
	n.ring = s.Ring.clone()
	for i := range s.Leases {
		l := s.Leases[i]
		if _, dup := n.leases[l.Name]; dup {
			return fmt.Errorf("control: restore: lease %q twice", truncate(l.Name, 64))
		}
		n.leases[l.Name] = &l
	}
	for i := range s.Registry {
		e := s.Registry[i].clone()
		if _, dup := n.registry[e.Key]; dup {
			return fmt.Errorf("control: restore: key %q twice", truncate(e.Key, 64))
		}
		n.registry[e.Key] = &e
	}
	for _, qs := range s.Queues {
		if _, dup := n.queues[qs.Name]; dup {
			return fmt.Errorf("control: restore: queue %q twice", truncate(qs.Name, 64))
		}
		q := newQueue(qs.Name)
		q.seq = qs.Seq
		q.enqueued, q.claims, q.reclaims = qs.Enqueued, qs.Claims, qs.Reclaims
		q.completions, q.failures, q.deadLettered = qs.Completions, qs.Failures, qs.DeadLettered
		for i := range qs.Jobs {
			j := qs.Jobs[i].clone()
			if _, dup := q.jobs[j.ID]; dup {
				return fmt.Errorf("control: restore: job %q twice", truncate(j.ID, 64))
			}
			q.jobs[j.ID] = &j
		}
		n.queues[qs.Name] = q
	}
	if err := n.checkInvariants(); err != nil {
		return fmt.Errorf("control: restore: %w", err)
	}
	f.mu.Lock()
	f.applied, f.ring, f.leases, f.registry, f.queues = n.applied, n.ring, n.leases, n.registry, n.queues
	f.mu.Unlock()
	return nil
}
