package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Operation names carried in Command.Op.
const (
	OpNoop = "noop" // reported for empty entries; never needs to be proposed

	OpSetRing = "set_ring"

	OpLeaseAcquire = "lease_acquire"
	OpLeaseRenew   = "lease_renew"
	OpLeaseRelease = "lease_release"

	OpRegPut    = "reg_put"
	OpRegDelete = "reg_delete"

	OpJobEnqueue  = "job_enqueue"
	OpJobClaim    = "job_claim"
	OpJobComplete = "job_complete"
	OpJobFail     = "job_fail"
	OpJobExtend   = "job_extend"
	OpJobRedrive  = "job_redrive"
	OpJobPurge    = "job_purge"
)

// Limits. The control plane holds small, hot metadata; these bounds keep one
// misbehaving client from turning every replica's memory (and every snapshot)
// into a dumping ground. Exceeding a size limit is "invalid"; exceeding a
// count limit is "full".
const (
	MaxCommandSize  = 1 << 20 // bytes of encoded command
	MaxNameLen      = 512     // lease names, holders, queue names, job ids, workers, node names
	MaxKeyLen       = 1024    // registry keys
	MaxValueLen     = 64 << 10
	MaxPayloadLen   = 256 << 10
	MaxErrorLen     = 1024 // job_fail error text is truncated (not rejected) beyond this
	MaxRingNodes    = 4096
	MaxLeases       = 100_000
	MaxRegistryKeys = 100_000
	MaxQueues       = 10_000
	MaxQueueJobs    = 100_000 // pending + claimed jobs per queue
	MaxMaxAttempts  = 1000

	MaxLeaseTTL   = 24 * time.Hour
	MaxVisibility = 12 * time.Hour
	MaxDelay      = 7 * 24 * time.Hour

	// DefaultMaxAttempts applies when job_enqueue leaves max_attempts at 0.
	DefaultMaxAttempts = 5
	// RetryBackoffBase and RetryBackoffMax shape the default exponential
	// backoff used when job_fail leaves backoff_ms at 0:
	// base·2^(attempts-1), capped at max.
	RetryBackoffBase = time.Second
	RetryBackoffMax  = 10 * time.Minute

	// CompletedRetention is how long a completed job's id is remembered so
	// that a producer retrying an enqueue does not re-run finished work.
	// MaxCompletedPerQueue bounds that memory regardless of time.
	CompletedRetention   = time.Hour
	MaxCompletedPerQueue = 10_000
)

// Command is the JSON envelope of every control-plane operation. Only the
// fields relevant to Op are meaningful; the rest are omitted on the wire.
//
// Now is the proposer's wall clock in Unix nanoseconds. Every time-dependent
// decision the FSM makes for this command uses it (see the package doc).
type Command struct {
	Op  string `json:"op"`
	Now int64  `json:"now"`

	// set_ring
	Nodes       []string `json:"nodes,omitempty"`
	RF          int      `json:"rf,omitempty"`
	R           int      `json:"r,omitempty"`
	W           int      `json:"w,omitempty"`
	ExpectEpoch *uint64  `json:"expect_epoch,omitempty"` // optional CAS on the current epoch

	// leases
	Name   string `json:"name,omitempty"`
	Holder string `json:"holder,omitempty"`
	TTLms  int64  `json:"ttl_ms,omitempty"`
	Token  uint64 `json:"token,omitempty"` // also used by job_complete / job_fail / job_extend

	// registry
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
	// ExpectVersion is optional: absent means unconditional, 0 means "must not
	// exist", n means "current version must be n".
	ExpectVersion *uint64 `json:"expect_version,omitempty"`

	// jobs
	Queue        string `json:"queue,omitempty"`
	ID           string `json:"id,omitempty"`
	Payload      []byte `json:"payload,omitempty"`
	MaxAttempts  int    `json:"max_attempts,omitempty"`
	DelayMs      int64  `json:"delay_ms,omitempty"`
	Worker       string `json:"worker,omitempty"`
	VisibilityMs int64  `json:"visibility_ms,omitempty"`
	Error        string `json:"error,omitempty"`
	BackoffMs    int64  `json:"backoff_ms,omitempty"`
}

// Encode returns the JSON encoding of c. It cannot fail for a Command (all
// fields are plain data), so it panics only on a programming error.
func (c Command) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic("control: encoding Command: " + err.Error())
	}
	return b
}

// DecodeCommand parses an encoded command. It never panics on hostile input.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if len(b) > MaxCommandSize {
		return c, fmt.Errorf("control: command is %d bytes, limit %d", len(b), MaxCommandSize)
	}
	if len(b) == 0 {
		return c, errors.New("control: empty command")
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("control: decode command: %w", err)
	}
	return c, nil
}

// unixNanos converts a proposer timestamp. The zero time maps to 0, which the
// FSM rejects for time-dependent ops, so a forgotten timestamp fails loudly
// instead of making every lease look expired.
func unixNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// durMs converts a duration to whole milliseconds, rounding up so that a
// sub-millisecond positive duration does not silently become "zero" (which
// several ops treat as "use the default").
func durMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// SetRingCmd proposes a new ring: the node list and the replication factor
// and quorum sizes every gateway must use.
func SetRingCmd(nodes []string, rf, r, w int, now time.Time) []byte {
	return Command{Op: OpSetRing, Now: unixNanos(now), Nodes: nodes, RF: rf, R: r, W: w}.Encode()
}

// SetRingIfCmd is SetRingCmd guarded by the current epoch, so two operators
// racing to reconfigure cannot silently overwrite each other.
func SetRingIfCmd(nodes []string, rf, r, w int, expectEpoch uint64, now time.Time) []byte {
	e := expectEpoch
	return Command{Op: OpSetRing, Now: unixNanos(now), Nodes: nodes, RF: rf, R: r, W: w, ExpectEpoch: &e}.Encode()
}

// AcquireLeaseCmd asks for lease name on behalf of holder for ttl. Re-acquiring
// a lease you already hold extends it and keeps the token.
func AcquireLeaseCmd(name, holder string, ttl time.Duration, now time.Time) []byte {
	return Command{Op: OpLeaseAcquire, Now: unixNanos(now), Name: name, Holder: holder, TTLms: durMs(ttl)}.Encode()
}

// RenewLeaseCmd extends a held, unexpired lease. ttl 0 reuses the lease's
// previous TTL.
func RenewLeaseCmd(name, holder string, token uint64, ttl time.Duration, now time.Time) []byte {
	return Command{Op: OpLeaseRenew, Now: unixNanos(now), Name: name, Holder: holder, Token: token, TTLms: durMs(ttl)}.Encode()
}

// ReleaseLeaseCmd gives a lease up early.
func ReleaseLeaseCmd(name, holder string, token uint64, now time.Time) []byte {
	return Command{Op: OpLeaseRelease, Now: unixNanos(now), Name: name, Holder: holder, Token: token}.Encode()
}

// RegPutCmd unconditionally writes a registry key.
func RegPutCmd(key string, value []byte, now time.Time) []byte {
	return Command{Op: OpRegPut, Now: unixNanos(now), Key: key, Value: value}.Encode()
}

// RegCASCmd writes a registry key only if its current version is
// expectVersion (0 = the key must not exist).
func RegCASCmd(key string, value []byte, expectVersion uint64, now time.Time) []byte {
	v := expectVersion
	return Command{Op: OpRegPut, Now: unixNanos(now), Key: key, Value: value, ExpectVersion: &v}.Encode()
}

// RegDeleteCmd unconditionally deletes a registry key.
func RegDeleteCmd(key string, now time.Time) []byte {
	return Command{Op: OpRegDelete, Now: unixNanos(now), Key: key}.Encode()
}

// RegDeleteIfCmd deletes a registry key only if its version is expectVersion.
func RegDeleteIfCmd(key string, expectVersion uint64, now time.Time) []byte {
	v := expectVersion
	return Command{Op: OpRegDelete, Now: unixNanos(now), Key: key, ExpectVersion: &v}.Encode()
}

// EnqueueJobCmd adds job id to queue (idempotent by id). maxAttempts 0 means
// DefaultMaxAttempts; delay postpones the first claim.
func EnqueueJobCmd(queue, id string, payload []byte, maxAttempts int, delay time.Duration, now time.Time) []byte {
	return Command{Op: OpJobEnqueue, Now: unixNanos(now), Queue: queue, ID: id, Payload: payload,
		MaxAttempts: maxAttempts, DelayMs: durMs(delay)}.Encode()
}

// ClaimJobCmd claims the oldest ready job in queue for worker, hiding it from
// other claimers for visibility.
func ClaimJobCmd(queue, worker string, visibility time.Duration, now time.Time) []byte {
	return Command{Op: OpJobClaim, Now: unixNanos(now), Queue: queue, Worker: worker, VisibilityMs: durMs(visibility)}.Encode()
}

// CompleteJobCmd acknowledges a claimed job. Repeating it with the same token
// after success is answered OK (Duplicate) so workers can retry safely.
func CompleteJobCmd(queue, id string, token uint64, now time.Time) []byte {
	return Command{Op: OpJobComplete, Now: unixNanos(now), Queue: queue, ID: id, Token: token}.Encode()
}

// FailJobCmd reports a failed attempt. The job is retried after backoff (0 =
// default exponential backoff) or dead-lettered once it has used its attempts.
func FailJobCmd(queue, id string, token uint64, errMsg string, backoff time.Duration, now time.Time) []byte {
	return Command{Op: OpJobFail, Now: unixNanos(now), Queue: queue, ID: id, Token: token, Error: errMsg,
		BackoffMs: durMs(backoff)}.Encode()
}

// ExtendJobCmd pushes a claimed job's visibility timeout to now+visibility.
func ExtendJobCmd(queue, id string, token uint64, visibility time.Duration, now time.Time) []byte {
	return Command{Op: OpJobExtend, Now: unixNanos(now), Queue: queue, ID: id, Token: token, VisibilityMs: durMs(visibility)}.Encode()
}

// RedriveJobCmd moves a dead-lettered job back to the queue with a fresh
// attempt budget.
func RedriveJobCmd(queue, id string, now time.Time) []byte {
	return Command{Op: OpJobRedrive, Now: unixNanos(now), Queue: queue, ID: id}.Encode()
}

// PurgeJobCmd forgets a dead-lettered or completed job (freeing its id).
func PurgeJobCmd(queue, id string, now time.Time) []byte {
	return Command{Op: OpJobPurge, Now: unixNanos(now), Queue: queue, ID: id}.Encode()
}
