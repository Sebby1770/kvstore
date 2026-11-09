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
                 UDP (or simnet)  ·  storage tier: kvnode (Rust, its own KTP server) + kvengine (C)
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

## The storage tier over KTP (kvnode/src/ktp)

kvnode serves the coordinators over KTP with a second, independent
implementation of the protocol in Rust: server side only (it accepts
connections and serves client-opened streams; it never dials, opens streams,
sends datagrams or gossips). The same rules apply: `ktp/conn.rs` is a sans-IO
state machine (handshake, AES-256-GCM, ACK ranges, RFC 9002 recovery with
NewReno, stream and connection flow control, anti-amplification, stateless
resets, idle timeout, close/drain), and `ktp/endpoint.rs` drives it with a
receive thread and a timer thread, handing out blocking `Read + Write` stream
handles. Each stream runs the unchanged 1.x request handler, exactly as a TCP
connection did. Coordinators address such nodes as `ktp://host:port`; a
`kvmesh` node uses its own endpoint, so storage links share the mesh's
identity and cluster key.

Two implementations stay compatible through tests, not hope:

* `kvgw/netstack/ktp/vectors_test.go` derives the codec, key schedule,
  handshake messages, packet protection and stateless resets from fixed keys
  into `kvnode/tests/ktp_vectors.txt`; the Go test fails if Go drifts from the
  file, `cargo test` if Rust does. The Rust tests also run the server through a
  handshake that must reproduce Go's SERVER_HELLO byte for byte.
* `kvgw/interop` (`make interop`) runs the real kvnode binary under the Go
  client: every op, 10% loss each way through a seeded relay, a wrong cluster
  key, kill -9 + restart healed by stateless reset, concurrency, identity
  pinning (`kvnode --allow-peer`).

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
* (2.1) The storage tier moved onto KTP (above). Porting the protocol found
  that Go's plaintext `open` could slice out of range on a header-only
  datagram whose bytes happened to form a valid CRC (fixed, regression-tested),
  and that quorum operations waited for every replica, so an unreachable
  one stalled each phase by its full timeout; they now return once the quorum
  is in and give stragglers a short grace period.

---

# 2.2 — the autonomous fabric

2.2 adds nine capabilities in three groups: kernel bypass and zero-copy
execution, self-healing topology, and network-aware runtime. Each one is a
package with its own tests, built to the same rules as the rest: sans-IO
cores, seeded and reproducible tests, hostile input fuzzed, standard library
only. `runtime/node` wires all of them into `kvmesh`.

```
                   HTTP adds: /replica · /spec/kv · /fabric · /mesh/{time,foresight,offload,pressure,spec,fabric}
   ┌──────────────────────────────────────────────────────────────────────────────────────────┐
   │ runtime/  foresight  LSTM + φ-accrual + fuzzy inference over link telemetry → failure     │
   │                      forecasts → pre-emptive leader handover, actor placement             │
   │           speculate  hedged execution, gossiped load digests, epidemic cancellation        │
   │           crdt       delta CRDTs (PN-counter, LWW, MV-register, add-wins OR-set)           │
   │           causal     causal broadcast over lossy datagrams, matrix-clock garbage collection │
   │           vtime      PTP-style clock discipline, TrueTime intervals, BMCA, sequencer        │
   ├──────────────────────────────────────────────────────────────────────────────────────────┤
   │ netstack/ offload    verified eBPF programs at an XDP/TC-style endpoint hook: rate limiter, │
   │                      circuit breaker, heartbeat responder, Maglev load balancer            │
   │           fabric     registered memory + one-sided verbs (READ/WRITE/CAS/FADD/CHASE)        │
   │           pmd        DPDK-style rings, mbuf pools, poll-mode lcores, RSS, memif              │
   │           multipath  MPTCP-style sessions over many KTP subflows                           │
   │           segroute   SRv6-style source routing through relays                              │
   │           rpc        + worker pool advertising APP_PRESSURE credits, Reactive Streams demand │
   │           ktp/wire   + APP_PRESSURE frame (0x31)                                           │
   └──────────────────────────────────────────────────────────────────────────────────────────┘
```

## What is real and what stands in for hardware

Some of these ideas assume hardware (RDMA NICs, CXL memory, SmartNICs and
DPUs, NICs bound to DPDK, PTP time cards) or kernel access (eBPF, XDP) that a
portable, standard-library Go program cannot have. In each case the
structures, algorithms, wire formats and failure semantics are the real
ones. The hardware's job is done by the closest software equivalent, and
the docs say which.

| Idea | What runs here | Stand-in for |
|---|---|---|
| RDMA one-sided verbs | fixed-layout verbs executed in the endpoint's packet path against registered memory; no application goroutine runs | the NIC executing verbs without the host CPU |
| CXL shared memory | `fabric.MapFile`: a MAP_SHARED mapping that co-located processes share with plain loads, stores and atomics | **real**, on one host |
| SmartNIC / eBPF | real eBPF bytecode (kernel encoding and helper numbers), verified and interpreted at the endpoint's ingress/egress hook | running on the card, or JIT-ed at XDP |
| DPDK | rte_ring, mbuf pools, poll-mode lcores and Toeplitz RSS in Go; the memif port is a **real** zero-copy, syscall-free path between processes | a physical NIC's queues polled from userspace |
| PTP time appliance | two-way time transfer with software timestamps; rigorous error intervals | hardware timestamps (smaller ε, same maths) |
| MPTCP / SRv6 | multipath sessions over KTP subflows; source routing through KTP-aware relays | kernel MPTCP; IPv6 segment routing headers |

## Group 1 — kernel bypass and zero-copy execution

### netstack/fabric — single-address-space memory

A `Region` is registered memory: a heap arena or a shared file mapping. A
`Ptr` is a global pointer (16-bit region, 48-bit offset). Peers use five
one-sided verbs, each a fixed-layout record that the owner reads in place:

```
request  tag | op | region | rkey | request id | offset | length/hops | chase read len
         | chase link off | chase key off | arg0 (expected / delta / key) | arg1 (new / flags) | data
response tag | op | status | flags | hops | request id | value (previous / end offset) | data
```

* READ, WRITE, CAS, FETCH_ADD, and CHASE. CHASE walks a linked structure on
  the owner (follow the word at `link_off`, stop on a matching key or after
  `max_hops`) and returns the node it ends on, so a remote pointer chase
  costs one round trip instead of one per hop.
* Execution: `endpoint.HandleDatagrams(tag, h)` runs `h` in the packet path,
  under the endpoint lock, as soon as the datagram is decrypted. The reply
  goes back on the same connection. No goroutine is scheduled and no handler
  is called.
* Safety: random 32-bit rkeys, per-region permissions (read, write,
  atomic), and every access is bounds- and alignment-checked.
* Reliability: verbs travel as unreliable datagrams and are retried with
  exponential backoff. Responses to WRITE, CAS and FETCH_ADD are cached per
  (connection, request id), so a retry is answered from the cache rather
  than executed twice. The test runs 200 FETCH_ADDs at 15% loss and 10%
  duplication and gets an exact count.
* `fabric.Index` is a lock-free hash index that lives entirely in one region,
  in the style of FaRM and Pilaf. Insert is FETCH_ADD (allocate), WRITE
  (entry pointing at the current bucket head), then CAS (publish), retried
  on conflict. Lookup is a READ of the bucket head plus a CHASE. After
  formatting the region, the owner runs no code for the index at all.

### netstack/offload — network functions in the data path

* **ISA.** eBPF's instruction encoding, registers r0-r10 (r10 read-only, a
  512-byte stack), the ALU/ALU64, JMP/JMP32, LDX/ST/STX and ATOMIC add
  classes, LD_IMM64 with pseudo-map-fd map references, and helpers by kernel
  number: lookup 1, update 2, delete 3, ktime 5, prandom 7, redirect_map 51.
  `HelperMAC64` (0x10001) is the one non-kernel helper, a keyed MAC
  primitive of the kind a SmartNIC provides. `Encode` produces the bytes the
  `bpf()` syscall takes; a test checks `mov r0,2; exit` against its known
  encoding.
* **Verifier.** It proves termination structurally: every jump goes
  forward, so an n-instruction program runs at most n instructions. It also
  validates every opcode, register, jump target, map reference, helper and
  argument count, and tracks which registers are initialised along every
  path (an intersection over predecessors).
* **Memory safety at run time.** Every pointer value carries a tag for the
  region it points into (context, packet, stack, a map value obtained in
  this run, or a map reference), and every load and store is bounds-checked
  against that region. Forging a tag can only reach memory the program
  already owns. A fault aborts the run (XDP_ABORTED).
* **Hook.** `endpoint.SetFilter` runs ingress programs on every datagram
  before connection lookup, and egress programs before every socket write.
  Verdicts work as in XDP: PASS, DROP, TX (rewrite and send back), and
  REDIRECT. Redirects to an address are sent encapsulated
  (`0xE2 'L' 'B' | origin | packet`); `TrustEncapsulation` lists the
  balancers allowed to send them. Redirects to a `QueueMap` hand the packet
  to Go code, like an AF_XDP socket.
* **Maps.** Array, hash (optionally LRU), `AddrMap` (DEVMAP-like redirect
  targets) and `QueueMap` (XSKMAP-like). A program run holds the locks of
  all its maps, taken in creation order, and Go-side access takes the same
  locks.
* **Programs.**
  * Rate limiter: per-peer token buckets in an LRU map, metering Initial
    and Raw packets (connection attempts and gossip). The node sets each
    member's share of the cluster budget whenever membership changes.
  * Circuit breaker: counts failures per peer, from stateless resets in the
    data path and from host-reported errors. When it trips, the egress
    program drops traffic to that peer; after a cooldown it lets one probe
    through (half-open), and any healthy packet closes the circuit again.
  * Heartbeat responder: answers MAC-authenticated 48-byte probes with
    XDP_TX and adds how long ago the host process last checked in. A node
    that answers but reports a stale host is hung, not dead.
  * Maglev: the paper's lookup-table construction (1021 slots,
    per-backend permutations), with flows steered by peer hash. Backends
    answer clients directly (direct server return); the test carries a real
    KTP handshake through it.

### netstack/pmd — userspace data plane

* `Ring` is DPDK's rte_ring: producer and consumer head/tail pairs, each on
  its own cache line, CAS reservation and in-order commit, multi-producer
  and multi-consumer, with burst enqueue and dequeue. It lives in a fabric
  region, so it works across processes.
* `Pool` is an mbuf pool: fixed buffers with 16-byte headers plus a ring of
  free indices, with no per-packet allocation. `Mbuf` is a two-word value.
* `Memif` is a shared-memory interface between two processes: one file
  holds a ring per direction and the shared pool. Sending a packet means
  enqueuing a buffer index, and the receiver reads it where the sender
  wrote it. The tests check zero copies (same buffer index on both sides),
  exchange packets with a child process through shared memory alone, and
  run the whole KTP stack over memif (a 2 MiB stream). In the in-process
  benchmark a packet costs about 7 ns.
* `Lcore` is a run-to-completion poll loop pinned with LockOSThread. Its
  `Idler` busy-polls while traffic flows, then yields, then sleeps in steps
  up to 1 ms.
* `Toeplitz` and `Steer` implement RSS. The hash is checked against
  Microsoft's published verification vectors.
* `SocketPort` drives the same Port API from a kernel socket. `PacketConn`
  adapts any Port to `net.PacketConn`.

## Group 2 — self-healing topology

### runtime/foresight — fuzzy failure prediction

* Each interval, per peer, it collects RTT inflation (srtt/min_rtt − 1),
  jitter (rttvar/srtt), loss (with a 40-packet prior so idle links cannot
  fake it), PTOs, congestion-window collapse (1 − cwnd/peak), heartbeat RTT
  and misses, and the peer's reported staleness.
* Features are normalised against the link's own slowly adapting baseline.
  The baseline stops adapting while the peer looks unwell, so failure is
  never learnt as normal.
* An online LSTM per link (12 units, truncated BPTT over 16 steps, Adam,
  global gradient clipping) is gradient-checked against central differences
  for every parameter. Its five-step autoregressive forecast is ensembled
  with a smoothed persistence-plus-trend extrapolation, taking the more
  pessimistic of the two. The LSTM covers learnt dynamics; the trend covers
  ramps it has never seen. Its one-step error is reported as "surprise".
* φ-accrual (Hayashibara) runs over heartbeat arrivals, with an asymptotic
  erfc tail so φ keeps rising during long silences.
* Mamdani fuzzy inference: trapezoid and shoulder membership functions,
  min/max rules, centroid defuzzification. The result is a likelihood in
  [0, 1] with reasons, a healthy/degrading/failing state with hysteresis,
  and a hung flag.
* Test results:
  * A grey failure (latency ×10, loss to 20%, cwnd collapse) is called
    failing 12–22 intervals before the node drops. φ alone fires 1–3
    intervals after it drops.
  * No failing calls across 20 seeds of jittery healthy links.
  * After recovery the peer returns to healthy.
* What the node does with it:
  * Actor placement avoids peers forecast to fail, before SWIM suspects
    them.
  * A leader that forecasts most of its voter links failing transfers
    leadership to the voter it reaches best, retrying every ten probe
    intervals. `TestForesightHandsLeadershipOver` degrades a leader's
    network and checks it steps down by itself.

### runtime/speculate — epidemic speculative execution

* Push gossip spreads load digests (load, queue, p50, p99). In the test,
  16 nodes converge in 10 rounds.
* The executor ranks candidates by expected latency, using observations,
  then digests, then live RPC pressure. It hedges to the next candidate once
  the primary passes its usual p95, up to `MaxCopies`, under a token-bucket
  budget. A failed copy is replaced at once (failover, outside the budget).
* The first success wins. The losers' streams are reset, and a cancel
  datagram goes to every node that got a copy. `Registry` stops a running
  copy, and refuses one that arrives late (tombstones last 30 s).
  Cancellation also chains through nested calls by context.
* With 10% random 200 ms stalls, p99 drops from 202 ms to 23 ms. `kvmesh`
  uses this for `GET /spec/kv/{key}` across coordinators.

### runtime/causal and runtime/crdt — coordination-free replication

* `causal`: messages carry their origin's vector of delivered sequence
  numbers. They are sent once, eagerly, and buffered until their causal
  predecessors arrive. Repair is by digest anti-entropy (push what the
  digest lacks; answer with our own digest if the sender knows more).
  Retransmission logs are dropped at causal stability, computed from a
  matrix clock. `FastForward` lets a snapshot-bootstrapped node skip
  history that has already been garbage collected.
* `crdt`: delta-state CRDTs with dotted causal contexts — a PN-counter, an
  LWW register (on HLC time), a multi-value register and an add-wins
  observed-remove set — in a named-object store. Each delta is a valid
  state and merging is a semilattice join.
* Tests:
  * Lattice laws hold for every type.
  * Add-wins and concurrent-write semantics behave as specified.
  * Causal delivery is exactly-once and in order across 20 seeds at 20%
    loss with duplication and reordering, and partitions heal.
  * Five stores updating concurrently over a lossy network converge
    byte-identically.
* The 1.x KV-backed CRDTs keep `/crdt/...`. The new in-memory replicated
  objects live at `/replica/...`.

## Group 3 — network-aware runtime

### netstack/multipath and netstack/segroute

* A `Session` is one byte stream over several subflows. Each subflow is its
  own KTP connection on its own path, with its own congestion control.
  Segments carry data sequence numbers; schedulers are ECF (default),
  MinRTT, round-robin and redundant.
* Data ACKs are cumulative KTP datagrams that acknowledge what the
  application has read, so the send buffer is also the flow-control window.
* A subflow is suspected only when it holds old unacknowledged data *and*
  its connection has heard nothing for as long. Its segments are then
  reinjected elsewhere. (Old data alone proves nothing: cumulative ACKs make
  fast paths wait on slow ones.)
* Joins need the session token and the same KTP peer identity.
* `segroute.Conn` prepends `0xE5 'S' 'R' | left | n | segments` for routed
  destinations. Each `Relay` pops itself off the list and forwards, and
  learns the way back per KTP connection ID, so replies need no header.
* Over fiber, cellular and satellite-like links, 16 MiB takes 6.8–7.7 s
  versus 11.4 s on the best single path (the run-to-run spread comes from
  goroutine interleavings, even on virtual time). A path blackholed mid-transfer is
  reinjected around, and a relay-routed subflow can join a session next to
  a direct one.

### runtime/vtime — deterministic virtual time

* Two-way time transfer (IEEE 1588 delay request-response) with a lucky
  packet filter and a PI servo: step above 5 ms, otherwise slew phase and
  frequency.
* `Interval()` provably contains grandmaster time: δ (mean one-way delay,
  which bounds any path asymmetry), plus the servo residual, plus
  (maxDrift + |rate|) times the time since the sample. 40 randomised
  scenarios check containment on every reading.
* The grandmaster is elected by IEEE 1588 dataset comparison (BMCA) over
  the live members. Every node's HLC reads its disciplined clock.
* `Matrix` is a matrix clock for causal stability. `Sequencer` releases
  HLC-stamped events up to the lowest member watermark, in (timestamp,
  origin) order, which gives the same total order on every node with no
  coordinator and no wall-clock wait. `CommitWait` adds external
  consistency for a local ε wait.

### APP_PRESSURE — application-aware backpressure (ktp, rpc)

* Frame `0x31`: `flags | [stream id] | seq | credits | load ‰ | queue`.
  Updates coalesce per scope (connection, or one stream). A lost frame is
  repaired only if it is still the latest, and receivers keep the highest
  sequence number seen.
* Pressure is opt-in per connection, and only sent to peers that have spoken
  it, so kvnode never receives one. Kvnode parses and ignores it anyway, and
  the Go↔Rust vectors pin the encoding.
* `rpc.Server.SetLimits` creates a bounded worker pool that advertises
  cumulative call credits: calls received plus free capacity. Unlike an
  absolute "N free", a cumulative limit is never wrong by the calls still in
  flight. The client blocks the *caller* at the limit, with a probe timer as
  a safety valve.
* `CallStream(..., Demand(n))` is Reactive Streams `request(n)`: the
  server's send blocks while the consumer has granted nothing.
* In the test, 40 callers hit a 4-slot, 2-queue server with zero rejections
  and never more than 4 handlers running.

## Node integration (runtime/node/autonomy.go)

| | where | default |
|---|---|---|
| heartbeat responder + prober | endpoint hook; probes every member each probe interval | on |
| rate limiter | endpoint hook; per-node share of `--rate-limit` | off |
| circuit breaker | endpoint hook | off (`--circuit-breaker`) |
| fabric index | 4 MiB region, handle gossiped in SWIM meta (`fabric`) | on (`--fabric-region`) |
| clock discipline | raw tag 0xA1, grandmaster = BMCA winner | on |
| replicated objects | raw tag 0xC5, bootstrap from a peer's snapshot | on |
| failure forecasts | probe interval; placement + leader handover | on |
| speculation | raw tag 0xD3 (digests, cancels); `/spec/kv` | on |
| RPC backpressure | 512 slots / 4096 queue | on (`--rpc-concurrency`) |

Raw-datagram tags below 0x20 belong to SWIM; the endpoint routes the rest
to handlers by their first byte.
