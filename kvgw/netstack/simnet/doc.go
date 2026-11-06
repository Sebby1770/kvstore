// Package simnet is an in-memory packet network for testing networked code
// without sockets.
//
// A Network hands out PacketConns that implement net.PacketConn with string
// addresses ("node-a:7000", or anything else without a '*'). Code written
// against net.PacketConn — the KTP endpoint, a SWIM driver — runs unchanged on
// top of it, but now the test controls the wire:
//
//   - Per-direction Link models: Latency + Jitter (one-way delay is uniform in
//     [Latency, Latency+Jitter]), Loss, Duplicate, Reorder (a held-back packet
//     is overtaken by later ones), Corrupt (one flipped bit), Bandwidth (packets
//     are serialised onto the link one after another, so a burst queues) and MTU
//     (oversize packets vanish, as they would with DF set). Links are looked up
//     most-specific first: (from,to), (from,"*"), ("*",to), then the default.
//   - Topology faults: Partition into groups, one-way Block, Isolate a node, and
//     Heal. Faults are checked both when a packet is sent and when it would be
//     delivered, so cutting the network also drops what is already in flight —
//     tests get a crisp "nothing crosses after this line".
//   - Each conn has a bounded receive queue that drops on overflow, like a
//     socket buffer, so a slow reader loses packets instead of growing memory.
//   - Stats count every packet's fate per link and globally.
//
// # Time
//
// Delivery is scheduled with time.AfterFunc and readers block on channels and
// time.Timer; there is no private clock and no polling loop. Inside a
// testing/synctest bubble the whole network therefore runs on the bubble's
// virtual clock: a test of a 30-second protocol timeout over a lossy 50 ms link
// finishes in milliseconds of real time, and timing assertions are exact.
// Outside a bubble it runs in real time.
//
// Each direction of each address pair is a FIFO path with one timer, so packets
// that should arrive at the same instant arrive in send order; only Jitter and
// Reorder change the order.
//
// # Determinism
//
// All randomness comes from one seeded RNG behind the network's mutex. With a
// deterministic sender schedule (a single goroutine, or a synctest bubble whose
// goroutines take turns) the same seed reproduces the same losses, delays and
// corruptions. Link.Fate is the pure fault model on its own — given an RNG and
// a packet size it returns the delays of the copies that survive — so the
// discrete-event harness in the KTP tests applies exactly the same faults
// without goroutines or timers.
package simnet
