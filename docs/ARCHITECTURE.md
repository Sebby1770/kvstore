# KVStore 2 — distributed systems runtime + networking stack

KVStore 1.x was a replicated key-value store: a C storage engine, a Rust node
daemon, and a single Go gateway that owned all distributed policy. 2.0 turns the
Go tier into two reusable layers and rebuilds the store on top of them:

```
                 applications:  KV gateway (quorum store)  ·  virtual actors  ·  leases / locks / job queues
   ┌───────────────────────────────────────────────────────────────────────────────────────────────┐
   │ runtime/  — distributed systems runtime                                                        │
   │   node     the daemon: wires everything below into one process (cmd/kvmesh)                    │
   │   actor    virtual actors: placement on the live ring, turn-based mailboxes, CAS-fenced state   │
   │   control  replicated control plane (FSM on Raft): ring config, leases + fencing, locks, jobs    │
   │   raft     consensus (sans-IO): pre-vote, check-quorum, ReadIndex, snapshots, conf changes       │
   │   swim     membership + failure detection (sans-IO): suspicion, incarnations, Lifeguard, gossip   │
   │   hlc      hybrid logical clocks                                                                │
   ├───────────────────────────────────────────────────────────────────────────────────────────────┤
   │ netstack/ — networking stack                                                                   │
   │   rpc      unary + server-streaming RPC over KTP streams: deadlines, cancellation, status codes  │
   │   endpoint goroutine driver: one UDP socket, many connections, Dial/Accept, net.Conn-like streams │
   │   ktp      KTP, the transport protocol (sans-IO): 1-RTT mutually-authenticated handshake,         │
   │            AEAD packets, multiplexed streams, flow control, ACK ranges, RFC 9002 loss recovery,   │
   │            NewReno / CUBIC / BBR congestion control, pacing, datagrams, keep-alive, close/drain   │
   │   wire     packet + frame codec, varints, CRC32C                                                │
   │   simnet   in-memory packet network: latency, jitter, loss, dup, reorder, corrupt, partitions    │
   └───────────────────────────────────────────────────────────────────────────────────────────────┘
                 UDP (or simnet)  ·  storage tier: kvnode (Rust) + kvengine (C) over the 1.x TCP protocol
```

Import paths are `github.com/Sebby1770/kvstore/kvgw/netstack/<pkg>` and
`github.com/Sebby1770/kvstore/kvgw/runtime/<pkg>`. Everything is **Go standard
library only** (no third-party modules), Go 1.25+.

## Design rules (apply to every package)

1. **Sans-IO protocol cores.** `ktp`, `swim`, and `raft` are pure state
   machines. They never read the clock (`now time.Time` is a parameter), never
   start goroutines, never touch sockets, and draw randomness only from an
   injected `*rand.Rand` / `io.Reader`. Input goes in through methods
   (`HandlePacket`, `Step`, `Handle`, `Tick`); output comes out by polling
   (`Poll`/`Outbox`/`Ready`). This is what makes deterministic simulation
   possible: the same seed replays the same execution, bit for bit.
2. **Drivers are thin.** Goroutines, timers, and sockets live in drivers
   (`endpoint`, `control.Service`, `node`). Drivers only use `time.Now`,
   `time.Timer`, `time.AfterFunc`, and channels, so they run unmodified inside a
   `testing/synctest` bubble (virtual time) over `simnet`.
3. **Hostile input never panics.** Every decoder bounds-checks and returns an
   error. Decoders that face the network have `Fuzz*` tests.
4. **Tests prove properties, not examples.** Each protocol package has
   randomized, seeded tests that check invariants (safety) and convergence
   (liveness after faults heal), reproducible from the printed seed. Heavy
   variants honour `testing.Short()`.
5. **House style.** Explanatory package docs and comments that say *why*
   (see `internal/cluster/cluster.go`), `gofmt`, `go vet` clean, `go test -race`
   clean.

---

## netstack/wire

Byte-level codec shared by the transport. Big-endian fixed fields, QUIC-style
varints (2-bit length prefix, up to 62 bits).

Packet (every datagram is exactly one packet):

```
 0  1B  type (high nibble) | reserved (low nibble)
        1 = Initial      (client hello; plaintext, CRC32C-protected, padded ≥ 1200 B)
        2 = Handshake    (server hello; plaintext, CRC32C-protected)
        3 = OneRTT       (application packets; AEAD-sealed, or CRC32C in insecure mode)
        4 = Raw          (connectionless sealed datagram, used by SWIM)
        5 = Reset        (stateless "I have no state for this connection")
 1  1B  version (1)
 2  8B  destination connection ID
10  8B  source connection ID
18  8B  packet number (strictly increasing per direction, never reused)
26  2B  payload length (bytes after the header, including AEAD tag / CRC)
28  …   payload
```

The 28-byte header is the AEAD associated data. Frames inside a payload are
`type varint` + fields:

| type | frame | fields |
|---|---|---|
| 0x00 | PADDING | — |
| 0x01 | PING | — (ack-eliciting) |
| 0x02 | ACK | largest, ack_delay_µs, range_count, first_range, [gap, range]… |
| 0x04 | RESET_STREAM | stream_id, error_code, final_size |
| 0x05 | STOP_SENDING | stream_id, error_code |
| 0x06 | HANDSHAKE | length, handshake message bytes |
| 0x08–0x0b | STREAM | flags: 0x02 = has-length, 0x01 = FIN; stream_id, offset, [length], data |
| 0x10 | MAX_DATA | max |
| 0x11 | MAX_STREAM_DATA | stream_id, max |
| 0x12 | MAX_STREAMS | max |
| 0x14 | DATA_BLOCKED | limit |
| 0x15 | STREAM_DATA_BLOCKED | stream_id, limit |
| 0x1c | CONNECTION_CLOSE | error_code, reason_len, reason |
| 0x1e | HANDSHAKE_DONE | — |
| 0x30 | DATAGRAM | length, data |

## netstack/ktp — the KTP transport (sans-IO)

**Identity and handshake.** Every node has an Ed25519 identity key. The
handshake is one round trip, mutually authenticated, forward secret, and bound
to an optional cluster pre-shared key:

```
C → S  Initial    HELLO        { version, client_eph_x25519, client_nonce }        (padded to ≥ 1200 B)
S → C  Handshake  SERVER_HELLO { server_eph_x25519, server_nonce, server_ed25519,
                                 sig_s = Sign(server_key, "ktp/1 server" ‖ H(transcript)) }
       both: shared = X25519(eph_c, eph_s)
             prk    = HKDF-Extract(SHA-256, salt = SHA-256("ktp/1 psk" ‖ cluster_key), ikm = shared)
             keys   = HKDF-Expand(prk, "ktp/1 c2s|s2c key|iv" ‖ H(transcript))
C → S  OneRTT     CLIENT_AUTH  { client_ed25519, sig_c = Sign(client_key, "ktp/1 client" ‖ H(transcript)) }
                  (application STREAM frames may ride in the same flight)
S → C  OneRTT     HANDSHAKE_DONE
```

* OneRTT packets: AES-256-GCM, nonce = iv XOR packet number, AAD = header.
* A wrong cluster key yields different keys, so the peer's first sealed packet
  fails to open and the handshake times out. `Config.VerifyPeer` can pin
  identities (allow-list). `Config.Insecure` replaces AEAD with CRC32C for
  debugging and benchmarks.
* Anti-amplification: until the client is validated, the server sends at most
  3× the bytes it has received.
* Raw datagrams (SWIM) are sealed with AES-256-GCM under
  `HKDF(cluster_key, "ktp/1 raw")` with a random 12-byte nonce carried in the
  packet, or CRC32C-only without a cluster key.

**Streams.** Bidirectional, ordered, reliable byte streams multiplexed on one
connection (client-initiated IDs 0, 2, 4…; server-initiated 1, 3, 5…), with
out-of-order reassembly, FIN, RESET_STREAM / STOP_SENDING, and a MAX_STREAMS
limit. Unreliable DATAGRAM frames ride alongside.

**Flow control.** Credit-based, per stream and per connection; the receiver
extends credit (MAX_STREAM_DATA / MAX_DATA) once the application has consumed
half the window. A sender never buffers past its credit plus a bounded send
buffer; `StreamWrite` returns a short count instead.

**Reliability (RFC 9002 style).** Packet numbers are never reused; lost
*frames* are retransmitted in new packets. ACK frames carry up to 32 ranges.
Receivers ACK every second ack-eliciting packet or after `max_ack_delay`
(25 ms). Loss detection: packet threshold 3, time threshold 9/8 × max(srtt,
latest_rtt). Probe timeout `PTO = srtt + max(4·rttvar, 1ms) + max_ack_delay`
with exponential backoff. RTT: latest / smoothed / variance / min, ack-delay
adjusted.

**Congestion control.** Pluggable `CongestionController`; NewReno (slow start,
congestion avoidance, one reduction per recovery period, persistent congestion
collapses to the minimum window), CUBIC (RFC 9438, with Reno-friendly region),
and BBR v1 with BBRv2's loss-based Startup exit. BBR is fed per-ACK
delivery-rate samples (draft-cheng-iccrg-delivery-rate-estimation, with
application-limited marking) and sets its own pacing rate; it was added after a
benchmark showed the loss-based controllers collapsing to ~1.7 Mbit/s at 2%
random loss on a 160 Mbit/s link (BBR: ~120 Mbit/s). A token-bucket pacer
spreads a window over the RTT at the controller's rate or 1.25 × cwnd / srtt.

**Lifecycle.** Keep-alive PINGs, idle timeout, CONNECTION_CLOSE with a
draining period of 3 × PTO, stateless Reset for unknown connection IDs.

**Observability.** `Stats()` exposes srtt, min_rtt, rttvar, cwnd, ssthresh,
bytes_in_flight, packets sent/received/lost, retransmitted frames, PTO count,
congestion events, bytes sent/received, open streams, handshake time.

## netstack/simnet

`Network` hands out `net.PacketConn`s with string addresses (`simnet.Addr`).
Per-direction `Link{Latency, Jitter, Loss, Duplicate, Reorder, Corrupt,
Bandwidth, MTU}` (with a default link and `"*"` wildcards), partitions
(`Partition(groups...)`, `Heal`, `Block(from,to)`, `Isolate(addr)`), bounded
receive queues (drop when full, like a socket buffer), seeded RNG, and delivery
via `time.AfterFunc` so it runs under `testing/synctest`. `Link.Fate(rng, size)`
is the pure fault model, reused by the KTP test harness.

## netstack/endpoint

One `net.PacketConn` (UDP or simnet), many KTP connections. An event loop owns
all `ktp.Conn` state; a reader goroutine feeds it packets; per-connection timers
drive `HandleTimeout`. Public surface:

* `Endpoint`: `New(pc, cfg, opts)`, `Listen("udp", addr, cfg, opts)`,
  `Dial(ctx, addr string)`, `Accept(ctx)`, `SendRaw(addr, payload)`,
  `RecvRaw(ctx) (from, payload)`, `Close`, `Stats`, `Conns`.
* `Conn`: `OpenStream(ctx)`, `AcceptStream(ctx)`, `SendDatagram`,
  `ReceiveDatagram(ctx)`, `CloseWithError`, `RemoteAddr`, `PeerIdentity`,
  `Stats`, `Done`, `Err`.
* `Stream`: `Read`, `Write`, `Close` (half-close: sends FIN), `CancelRead`,
  `Reset`, deadlines — usable as an `io.ReadWriteCloser` / mostly `net.Conn`.

`Endpoint.Conn(ctx, addr)` returns an established connection to an address --
one we dialed or one the peer dialed to us (accepted connections are
registered under the peer's address) -- dialing only if there is none, with
concurrent callers sharing a single dial. Peer-to-peer users set
`Options.AcceptDialed` so their accept loop also serves connections they
dialed: either side may then open streams on either connection, and each pair
of mesh nodes shares one encrypted connection.

## netstack/rpc

One RPC per KTP stream. Request: `method`, `deadline` (remaining µs, 0 = none),
metadata, payload. Response: zero or more DATA frames then END, or ERR
`{code, message}`. Status codes mirror gRPC (`OK, Canceled, Unknown,
InvalidArgument, DeadlineExceeded, NotFound, AlreadyExists, PermissionDenied,
ResourceExhausted, FailedPrecondition, Aborted, Unavailable, Internal,
Unimplemented`). Server: `Register(method, Handler)`, `RegisterStream`,
interceptors, panic recovery (→ `Internal`), per-method metrics. Client:
connection pool per address, `Call`, `CallStream`, retry of `Unavailable` on
idempotent calls only, context cancellation resets the stream. Typed helpers
`rpc.Unary[Req, Resp]` use a codec (JSON by default).

---

## runtime/hlc

Hybrid logical clock (Kulkarni et al.): 48-bit wall milliseconds + 16-bit
logical counter in a `uint64` that sorts correctly. `Now()` for local/send
events, `Update(remote)` for receives; rejects remote timestamps further than
`MaxOffset` ahead.

## runtime/swim — membership and failure detection (sans-IO)

SWIM (Das et al.) with the Lifeguard refinements (HashiCorp):

* Each protocol period, probe one member chosen round-robin from a shuffled
  list: `ping` → `ack`; on timeout, `ping-req` through k random members, which
  `ping` the target and relay the `ack` (or `nack`).
* No ack → the member becomes **suspect**; suspicion times out to **dead**
  unless refuted. Members refute suspicion of themselves by bumping their
  **incarnation**. Precedence: `alive(i)` overrides `suspect/alive(j<i)`;
  `suspect(i)` overrides `alive(i)` and anything older; `dead/left` override
  everything at the same or lower incarnation.
* Lifeguard: local health awareness (probe timeouts stretch when *we* are the
  slow one), suspicion timeouts that shrink as independent confirmations arrive
  (`min = mult·log10(n)·interval`, `max = maxMult·min`), and nacks.
* Updates piggyback on every message (retransmitted `mult·log(n+1)` times,
  newest/least-sent first, size-bounded to fit one datagram). Periodic
  push-pull full-state sync with a random member heals partitions quickly.
* Graceful leave broadcasts `left`. Member metadata (`map[string]string`, e.g.
  `rpc`, `http`, `role`) is gossiped with the incarnation.

API: `New(cfg, now)`, `Join(seeds, now)`, `Leave(now)`, `Tick(now)`,
`Handle(from, payload, now)`, `Outbox() []Message`, `Events() []Event`,
`NextDeadline()`, `Members()`, `Alive()`, `UpdateMeta(meta, now)`,
`LocalHealth()`.

## runtime/raft — consensus (sans-IO)

Raft (Ongaro & Ousterhout) in the etcd/raft `RawNode` shape:
`Tick()`, `Step(msg)`, `Propose(data)`, `ProposeConfChange(cc)`,
`ReadIndex(ctx)`, `TransferLeadership(to)`, `HasReady()`, `Ready()`,
`Advance(rd)`, `Status()`.

* Leader election with randomized timeouts, **PreVote** (a partitioned node
  can't inflate terms) and **CheckQuorum** (a leader that can't reach a quorum
  steps down).
* Log replication with optimistic pipelining, a bounded in-flight window, and
  fast conflict back-off (follower returns a term/index hint).
* A leader appends an empty entry on election and only counts replicas for
  entries of its own term (Figure 8).
* **ReadIndex** linearizable reads (leader confirms leadership with a heartbeat
  round before serving).
* Snapshots: `Storage.Snapshot()`, `InstallSnapshot` for lagging followers, log
  compaction.
* Membership: one voter/learner change at a time, only after the leader has
  committed an entry in its term and no earlier change is pending.
* Storage: `MemoryStorage` and a durable file storage (append-only WAL with
  per-record CRC32C, fsync, torn-tail truncation on recovery, snapshot file).

`Ready` contract: persist `HardState` + `Entries` (+ `Snapshot`) **before**
sending `Messages`, apply `CommittedEntries` in order, then `Advance`.

## runtime/control — the control plane

A deterministic state machine replicated by Raft (`FSM.Apply(index, cmd)`,
`Snapshot()`, `Restore()`); `Service` drives a `raft.RawNode`, persists,
forwards proposals to the leader, and serves linearizable reads via ReadIndex.

Commands (JSON, each carrying the proposer's wall time `now` so every replica
computes the same expiries):

* **Ring config** — `SetRing{nodes, rf, r, w}` → new epoch. Every gateway
  serves the same ring, so quorum overlap holds across gateways.
* **Leases / locks** — `Acquire{name, holder, ttl}`, `Renew`, `Release`. A
  grant returns a **fencing token** (the Raft index of the grant), strictly
  increasing per name; resources reject writes carrying an older token.
* **Registry** — small linearizable metadata keys with CAS.
* **Job queue** — `Enqueue{queue, id, payload, max_attempts}` (id makes it
  idempotent), `Claim{queue, worker, lease}` (visibility timeout),
  `Complete{id, token}`, `Fail{id, token, err}` with retry/backoff and a dead
  letter list. At-least-once; the token fences stale workers.

## runtime/actor — virtual actors

Orleans-style: an actor `ID{Type, Key}` always exists logically and is activated
on demand on its owner node (rendezvous hashing over SWIM-alive runtime nodes).

* `Host.Ask` / `Tell` route locally or forward to the owner over RPC (with a hop
  limit, re-routing if ownership moved).
* Turn-based concurrency: one mailbox goroutine per activation, one message at a
  time; bounded mailboxes push back.
* State: `ctx.State()` / `ctx.SetState()` / `ctx.Persist()` through a
  `StateStore` with **optimistic concurrency** (expected version). If two nodes
  briefly both activate an actor during membership churn, the stale one's next
  persist fails with a conflict and it deactivates — CAS fencing instead of a
  global directory.
* Timers (`ctx.SetTimer`), idle deactivation, `Rebalance()` on membership
  change, panic supervision (deactivate, error to caller, reactivate on next
  message with backoff), per-type metrics.

## runtime/node and cmd/kvmesh

`kvmesh` is one process per machine: KTP endpoint on UDP, SWIM over raw
datagrams, Raft + control plane over RPC, an actor host, and the KV gateway
(quorum coordinator) whose ring comes from the control plane. Any node serves
any request — no single gateway — and the HTTP API adds `/mesh/*`, `/lease/*`,
`/jobs/*`, `/actor/*` next to the 1.x KV routes.

## Deterministic simulation (where it lives)

The plan was a separate `runtime/sim` package; in the end each sans-IO core
carries its own seeded, virtual-time simulator next to its tests, which keeps
every harness in the package whose invariants it checks:

* `runtime/raft` -- a 5-node cluster with drop / duplicate / reorder,
  partitions and crash-restart from durable storage, checking election safety,
  log matching, leader completeness, state-machine safety and commit
  monotonicity after every Ready (120 seeds in the full run).
* `runtime/swim` -- clusters of up to 32 nodes: convergence, crash detection
  without false positives, refutation, rejoin, partitions healing, long lossy
  runs; the same seed reproduces the same event trace.
* `netstack/ktp` -- a client/server pair over the simnet fault model
  (`Link.Fate`) with bandwidth queues: byte-exact delivery under loss, dup,
  reorder and corruption for every congestion controller.
* `runtime/control` -- model-based lease checks; determinism and
  snapshot-equivalence over random command logs.

Drivers are tested on top: `netstack/endpoint`, `netstack/rpc`,
`runtime/control` (Service), `runtime/actor` and `runtime/node` run inside
`testing/synctest` bubbles over `simnet`, so whole meshes -- gossip, Raft,
actors, KV -- run on virtual time and a 40-second failover test takes well
under a second. Failures print their seed; the `make chaos` target runs the
full-size suites and CI runs them on every push.

## What changed from this plan

* Actor state lives in the control plane's registry (linearizable CAS) rather
  than the KV data plane: the data plane's CAS is per-coordinator, which is not
  a safe fence once there are many coordinators. Values are therefore bounded by
  the registry limit (64 KiB), and every persist is a Raft commit.
* Actor placement uses SWIM's *alive* members only (not suspects), so actors
  fail over as soon as their host is suspected; a false suspicion costs a brief
  double activation that the CAS fence makes harmless.
* Raft writes the founding membership into the log as committed conf-change
  entries (identical on every founder), so a node added later learns the full
  membership by replication instead of believing it is alone.
