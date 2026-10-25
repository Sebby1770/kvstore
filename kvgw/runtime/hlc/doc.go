// Package hlc implements hybrid logical clocks (Kulkarni, Demirbas et al.,
// "Logical Physical Clocks", 2014).
//
// A hybrid logical clock gives every event a timestamp that
//
//   - respects causality like a Lamport clock: if a happened-before b, then
//     ts(a) < ts(b), because receivers fold remote timestamps in with Update;
//   - stays close to physical time: the wall component is always the largest
//     physical time any causally-preceding event has observed, so it never runs
//     ahead of the fastest clock in the cluster by more than the accepted skew;
//   - fits in a single comparable integer.
//
// # Encoding
//
// A Timestamp is a uint64 laid out as
//
//	63                               16 15            0
//	+----------------------------------+---------------+
//	|  wall clock, Unix milliseconds   | logical count |
//	+----------------------------------+---------------+
//
// Because the wall part occupies the high bits, plain integer comparison is the
// HLC order (wall first, then logical), so timestamps can be used directly as
// map keys, sort keys, or version numbers. 48 bits of milliseconds lasts until
// the year 10889.
//
// The logical counter breaks ties between events that share a millisecond. If
// more than 65535 events happen within one millisecond the counter carries into
// the wall part: the clock borrows a millisecond from the future rather than
// wrapping or blocking. That keeps the clock strictly monotonic at the cost of
// the wall part running marginally ahead of physical time under extreme load;
// it catches up as soon as physical time passes it.
//
// # Skew protection
//
// Update rejects a remote timestamp whose wall part is more than MaxOffset
// ahead of the local physical clock. Without that check one node with a clock
// set years into the future would drag every clock it talks to along with it,
// permanently. Rejected timestamps leave the clock untouched.
package hlc
