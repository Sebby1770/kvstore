// Package swim is cluster membership and failure detection: SWIM (Das,
// Gupta & Motivala, 2002) with HashiCorp's Lifeguard refinements, written as a
// sans-IO state machine. Handle feeds it datagrams, Tick runs its timers,
// Outbox drains what it wants to send, Events reports membership changes. It
// reads no clock and draws randomness only from Config.Rand, so a cluster of
// nodes can be simulated deterministically (see the tests).
//
// # Failure detection
//
// Once per protocol period a node pings one member, chosen round-robin from a
// list reshuffled every pass (so every member is probed within a bounded time,
// unlike uniform random choice). No ack within the probe timeout, and it asks
// k other members to ping the target on its behalf (ping-req); they relay the
// ack, or a nack if they also heard nothing. If the period ends without an
// ack, the target becomes a suspect -- not dead: suspicion gives a slow or
// briefly partitioned node a chance to refute.
//
// # Precedence (incarnation numbers)
//
// Every member owns an incarnation number that only it may increase. Rumours
// carry (state, incarnation) and are ordered:
//
//   - alive(i) overrides alive/suspect at a lower incarnation (and revives a
//     dead member that rejoined with a higher one);
//   - suspect(i) overrides alive(i) and anything at a lower incarnation;
//   - dead(i) / left(i) override alive and suspect at the same or lower
//     incarnation.
//
// A member that hears a rumour of its own suspicion or death refutes it by
// gossiping alive at a higher incarnation. That is also how a restarted node
// shakes off its old death: the first push-pull tells it what the cluster
// believes, and it out-bids it.
//
// # Lifeguard
//
//   - Local health awareness: missed acks, missing nacks and rumours of our
//     own suspicion raise a score that stretches our probe interval and
//     timeout, so a node that is itself overloaded stops accusing healthy
//     peers. Successful probes lower it.
//   - Dynamic suspicion: a suspicion starts at its maximum timeout and shrinks
//     toward the minimum (SuspicionMult × log10(n) × interval) as independent
//     members confirm it, so true failures are declared fast and lonely
//     accusations slowly.
//   - Nacks let the prober tell "the target is down" from "my helpers can't
//     hear me either".
//
// # Dissemination
//
// Rumours piggyback on pings, acks and periodic gossip messages, least-sent
// first, each retransmitted RetransmitMult × ceil(log10(n+1)) times and packed
// to fit one datagram; a newer rumour about a member replaces the queued one.
// A periodic push-pull exchanges full state with a random member (chunked to
// fit datagrams), and every period also with a recently failed member and a
// seed -- the reconnect that lets two halves of a healed partition find each
// other again. Dead and left members are forgotten after DeadReclaim.
package swim
