// Package control is the replicated control plane of a KVStore 2 mesh: the
// small amount of state that every node must agree on, kept in a deterministic
// state machine that Raft replicates.
//
// What lives here, and why it needs consensus:
//
//   - Ring config (set_ring). Every gateway must serve the same ring — the
//     same node list and the same RF/R/W — or two gateways could pick
//     preference lists whose R and W quorums do not overlap, silently breaking
//     read-your-writes. Each change bumps an epoch so stale configs are easy to
//     spot.
//   - Leases and locks (lease_acquire / lease_renew / lease_release). A grant
//     carries a fencing token equal to the Raft index of the grant. Indices
//     only grow, so tokens are strictly increasing per name, and a resource
//     that remembers the highest token it has seen can reject a delayed write
//     from a holder that lost its lease while paused (the classic GC-pause
//     problem). The token, not the clock, is the safety net.
//   - Registry (reg_put / reg_delete). Small linearizable metadata keys with
//     compare-and-set on a version (the Raft index of the last write).
//   - Job queues (job_enqueue / job_claim / job_complete / job_fail /
//     job_extend, plus job_redrive / job_purge for operators). Enqueue is
//     idempotent by job id; a claim hands out a token (again the Raft index)
//     and a visibility timeout; a worker that stalls past the timeout loses
//     the job to the next claimer and its token goes stale, so its late
//     complete or fail is rejected. Delivery is at-least-once; the token
//     makes "exactly one accepted completion per job" hold.
//
// # Determinism
//
// FSM.Apply must produce the same state and the same Result on every replica
// for the same (index, command) sequence, otherwise replicas diverge. So Apply
// never reads the clock, never draws randomness, and never lets map iteration
// order leak into outputs (snapshots and listings are sorted). Time enters
// only through the command: every command carries "now", the proposer's wall
// clock in Unix nanoseconds, and every expiry — lease TTLs, visibility
// timeouts, retry backoff — is judged against the "now" of the command being
// applied. Two replicas applying the same log therefore agree on which lease
// expired when, even if their own clocks disagree wildly.
//
// The price is that expiry is only as accurate as the proposer's clock. A
// proposer whose clock runs fast can make a lease look expired early (to that
// one command). That is why leases come with fencing tokens: clocks decide
// liveness, tokens decide safety. The FSM does not clamp "now" to be
// monotonic; each command is judged at its own time, so one bad clock affects
// only its own commands rather than dragging the whole state machine forward.
//
// Indices must be strictly increasing. Apply rejects an index at or below the
// last applied one with code "stale_index" and leaves the state untouched,
// which makes re-applying an already-applied suffix (e.g. after restoring a
// snapshot) harmless. An empty command is a no-op (Raft leaders append one on
// election) and still advances the applied index.
//
// # Wire format
//
// Commands are JSON objects {"op": "...", "now": <unix nanos>, ...fields};
// see Command for the fields and the typed constructors (AcquireLeaseCmd,
// EnqueueJobCmd, …) so callers never hand-write JSON. Apply returns a Result,
// itself JSON-encodable so a Service can hand it back to a remote proposer;
// Result.Err maps the result code onto sentinel errors (ErrConflict,
// ErrNotHolder, ErrExpired, ErrStaleToken, …) that work with errors.Is.
//
// # Reads
//
// The query methods (Ring, Lease, Leases, Get, Keys, Queue, Queues, Jobs,
// DeadLetters) read the local replica. They are linearizable only when the
// caller first confirms, via Raft ReadIndex, that this replica has applied
// everything committed before the read began; that is the Service's job.
// The FSM is safe for concurrent use: Apply takes a write lock, queries a
// read lock, and everything returned is a copy.
//
// # Snapshots
//
// Snapshot serialises the whole state (sorted, versioned JSON) together with
// the applied index; Restore replaces the state with a snapshot after fully
// validating it, because snapshots arrive from the network (InstallSnapshot)
// and must not be trusted to be well-formed. A failed Restore leaves the FSM
// unchanged.
package control
