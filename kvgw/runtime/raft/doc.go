// Package raft is Raft consensus (Ongaro & Ousterhout) as a sans-IO library,
// in the shape of etcd's RawNode: the application owns the clock (Tick), the
// network (Step and Ready.Messages) and the disk (Storage and
// Ready.Entries); this package owns only the algorithm. Nothing here starts
// a goroutine or reads the time, so a cluster of nodes can be driven
// deterministically from a seed -- which is exactly how it is tested.
//
// # The Ready contract
//
// Loop: when HasReady, take rd := Ready() and, in this order,
//
//  1. persist rd.HardState, rd.Entries and rd.Snapshot (FileStorage.Save
//     does all three with one fsync);
//  2. send rd.Messages (only now: a vote or an append acknowledgement must
//     never be visible to a peer before the state it promises is durable);
//  3. apply rd.Snapshot, then rd.CommittedEntries, to the state machine;
//  4. call Advance(rd).
//
// # What is implemented
//
//   - Leader election with randomised timeouts, plus PreVote (a node that was
//     partitioned away runs a non-binding election first, so it cannot
//     inflate terms and depose a healthy leader when it returns) and
//     CheckQuorum (a leader that cannot reach a quorum for an election
//     timeout steps down; followers with a live leader ignore vote requests
//     -- a leader lease).
//   - Log replication with probe / replicate / snapshot progress states,
//     pipelining bounded by MaxInflightMsgs, and conflict back-off a term at
//     a time using the follower's hint.
//   - Commitment only through entries of the leader's own term (Figure 8),
//     with an empty entry appended at the start of every term.
//   - ReadIndex linearizable reads: the leader records its commit index,
//     confirms it is still leader with a heartbeat round tagged with the
//     request, and only then answers; requests arriving before the leader's
//     first commit in its term wait for it.
//   - Snapshots: InstallSnapshot for followers behind the compacted log; a
//     leader that hears nothing about a snapshot for an election timeout
//     falls back to probing.
//   - Single-server membership changes (voters and learners), one at a time,
//     only after the leader has committed an entry of its own term (the fix
//     for the single-server change bug). Initial membership is bootstrapped
//     as committed conf-change entries, identical on every founding peer, so
//     a node added later learns the full membership from the log.
//   - Leadership transfer via TimeoutNow.
//   - FileStorage: a CRC32C-framed write-ahead log plus an atomically
//     replaced snapshot file. A torn or corrupt tail is truncated on open,
//     never replayed.
//
// Inbound messages are validated (terms present, append entries contiguous,
// snapshots non-empty) and an append that would overwrite a committed entry
// is refused rather than trusted: FuzzUnmarshalMessage feeds arbitrary bytes
// through the codec into a live node.
//
// # Testing
//
// A simulated cluster drops, duplicates and reorders messages, partitions
// the network, and crashes and restarts nodes from their durable storage
// while proposals flow; after every Ready it checks election safety (one
// leader per term), state-machine safety (every node applies the same entry
// at every index), leader completeness (a new leader holds every committed
// entry) and commit monotonicity, and after healing that every acknowledged
// proposal was applied everywhere in the same order. 120 seeds run in about
// a second and replay exactly.
package raft
