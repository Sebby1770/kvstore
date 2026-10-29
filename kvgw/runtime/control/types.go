package control

import (
	"slices"
	"time"
)

// RingConfig is the replicated ring: which storage nodes exist and the RF/R/W
// every gateway must use. Epoch 0 means no ring has been set yet.
type RingConfig struct {
	Epoch     uint64   `json:"epoch"`
	Nodes     []string `json:"nodes"`
	RF        int      `json:"rf"`
	R         int      `json:"r"`
	W         int      `json:"w"`
	Index     uint64   `json:"index"`      // Raft index of the set_ring that produced it
	UpdatedAt int64    `json:"updated_at"` // proposer time of that set_ring (unix nanos)
}

func (r RingConfig) clone() RingConfig {
	r.Nodes = slices.Clone(r.Nodes)
	return r
}

// Lease is a granted lease. Token is the fencing token: the Raft index of the
// grant, strictly increasing per name across grants. Times are proposer
// clocks in Unix nanoseconds.
type Lease struct {
	Name       string `json:"name"`
	Holder     string `json:"holder"`
	Token      uint64 `json:"token"`
	TTLms      int64  `json:"ttl_ms"`
	AcquiredAt int64  `json:"acquired_at"`
	ExpiresAt  int64  `json:"expires_at"`
	Renewals   uint64 `json:"renewals"`
}

// ValidAt reports whether the lease is still held at t, as the FSM would judge
// a command stamped t.
func (l Lease) ValidAt(t time.Time) bool { return t.UnixNano() < l.ExpiresAt }

// Expiry returns ExpiresAt as a time.Time.
func (l Lease) Expiry() time.Time { return time.Unix(0, l.ExpiresAt) }

// RegistryEntry is one registry key. Version is the Raft index of the last
// write, so versions are unique and increase with every write; CreateIndex is
// the index of the write that created the key.
type RegistryEntry struct {
	Key         string `json:"key"`
	Value       []byte `json:"value"`
	Version     uint64 `json:"version"`
	CreateIndex uint64 `json:"create_index"`
	UpdatedAt   int64  `json:"updated_at"`
}

func (e RegistryEntry) clone() RegistryEntry {
	e.Value = slices.Clone(e.Value)
	return e
}

// JobState is where a job is in its lifecycle.
type JobState string

const (
	// JobPending: waiting to be claimed (ready once NotBefore has passed).
	JobPending JobState = "pending"
	// JobClaimed: handed to Worker under Token until VisibleUntil. After
	// VisibleUntil the job is claimable again, which invalidates Token.
	JobClaimed JobState = "claimed"
	// JobDead: exhausted MaxAttempts; parked in the dead-letter list.
	JobDead JobState = "dead"
	// JobCompleted: acknowledged; kept for CompletedRetention so a retried
	// enqueue of the same id is recognised as a duplicate.
	JobCompleted JobState = "completed"
)

// Job is one queued unit of work. Times are proposer clocks in Unix
// nanoseconds.
type Job struct {
	Queue       string   `json:"queue"`
	ID          string   `json:"id"`
	Payload     []byte   `json:"payload"`
	State       JobState `json:"state"`
	Seq         uint64   `json:"seq"` // enqueue order within the queue; claims prefer the lowest
	Attempts    int      `json:"attempts"`
	MaxAttempts int      `json:"max_attempts"`
	EnqueuedAt  int64    `json:"enqueued_at"`
	NotBefore   int64    `json:"not_before"` // pending: not claimable before this time

	Worker       string `json:"worker,omitempty"`
	Token        uint64 `json:"token,omitempty"` // Raft index of the latest claim
	ClaimedAt    int64  `json:"claimed_at,omitempty"`
	VisibleUntil int64  `json:"visible_until,omitempty"`

	LastError     string `json:"last_error,omitempty"`
	FinishedAt    int64  `json:"finished_at,omitempty"`    // completed or dead-lettered (proposer time)
	FinishedIndex uint64 `json:"finished_index,omitempty"` // Raft index of completion / dead-lettering
}

func (j Job) clone() Job {
	j.Payload = slices.Clone(j.Payload)
	return j
}

// Ready reports whether a claim stamped t could take this job.
func (j Job) Ready(t time.Time) bool {
	now := t.UnixNano()
	switch j.State {
	case JobPending:
		return j.NotBefore <= now
	case JobClaimed:
		return j.VisibleUntil <= now
	}
	return false
}

// QueueStats summarises one queue. Pending includes delayed jobs; Claimed
// includes claims whose visibility timeout has lapsed but that nobody has
// re-claimed yet.
type QueueStats struct {
	Name      string `json:"name"`
	Pending   int    `json:"pending"`
	Claimed   int    `json:"claimed"`
	Dead      int    `json:"dead"`
	Completed int    `json:"completed"` // retained for dedup

	Enqueued     uint64 `json:"enqueued"`
	Claims       uint64 `json:"claims"`
	Reclaims     uint64 `json:"reclaims"` // claims of a job whose previous claim lapsed
	Completions  uint64 `json:"completions"`
	Failures     uint64 `json:"failures"`
	DeadLettered uint64 `json:"dead_lettered"`
}
