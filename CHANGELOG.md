# Changelog

All notable changes to KVStore are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/).

## [2.0.0] — 2026-10-29

KVStore becomes a distributed systems runtime and networking stack, with the
replicated KV store as its first application. Every Go package is standard
library only; protocol cores are sans-IO state machines tested by seeded
simulation.

### Added

**Networking stack (`kvgw/netstack`)**

- `wire` — KTP packet header, QUIC varints, all frame types, CRC32C.
- `ktp` — KTP transport: 1-RTT mutually authenticated handshake (X25519 +
  Ed25519 over a transcript hash, HKDF salted with the cluster key, confirm MAC
  so a wrong key fails immediately), AES-256-GCM packets, anti-amplification,
  multiplexed streams with reassembly / FIN / reset / stop-sending, stream and
  connection flow control, datagrams, ACK ranges, RFC 9002 loss detection and
  PTO, NewReno, CUBIC and BBR (delivery-rate sampling, BBRv2 Startup exit),
  pacing, keep-alive, idle/handshake timeouts, CONNECTION_CLOSE with draining,
  stateless resets authenticated by the cluster key, sealed Raw datagrams.
- `simnet` — in-memory `net.PacketConn` network with latency, jitter, loss,
  duplication, reordering, corruption, bandwidth queues, MTU and partitions;
  runs on virtual time inside `testing/synctest`.
- `endpoint` — KTP over one UDP socket: dial/accept, one shared connection
  per peer pair, goroutine-safe streams with deadlines, raw datagrams.
- `rpc` — one call per stream: unary and server-streaming, gRPC status codes,
  deadlines, cancellation that reaches the server, metadata, interceptors,
  panic recovery, per-method stats, typed JSON helpers.

**Distributed runtime (`kvgw/runtime`)**

- `hlc` — hybrid logical clocks with max-offset protection.
- `swim` — SWIM membership with Lifeguard (local health, dynamic suspicion,
  nacks), incarnation refutation, piggybacked gossip, push-pull, partition
  healing via reconnects to failed members and seeds.
- `raft` — pre-vote, check-quorum, pipelined replication, ReadIndex,
  snapshots, single-server membership changes, leadership transfer, message
  validation, and a CRC-framed WAL with torn-tail truncation.
- `control` — Raft-replicated control plane: the storage ring with epochs,
  leases with fencing tokens, job queues (visibility timeouts, token-fenced
  completion, retries with backoff, dead letters), a linearizable registry;
  `Service` runs it with request tracking and linearizable reads.
- `actor` — virtual actors: on-demand activation on the owner, turn-based
  mailboxes, timers, idle deactivation, rebalancing, panic supervision, and
  CAS-fenced state.
- `node` + `cmd/kvmesh` — the mesh daemon: every node serves the KV API, the
  ring comes from the control plane, founders bootstrap from a shared list,
  joiners are added as learners and promoted; `/mesh` control room,
  `/mesh/status`, `/mesh/metrics`, `/lease`, `/jobs`, `/registry`, `/actor`.
- `cmd/ktpcat` — KTP netcat, benchmark and simulated-link tracer.
- CLI: `kvstore mesh | lease … | jobs … | actor …`.
- `scripts/mesh-demo.sh` / `make mesh-demo`; `make test-race`, `make chaos`,
  `make fuzz-smoke`; CI runs all of them.

### Changed

- The coordinator's RF/R/W live in its atomic membership snapshot, and
  `Reconfigure(epoch, …)` adopts control-plane rings monotonically.
- The gateway's HTTP surface moved to `internal/httpapi`, shared with kvmesh;
  OR-Set tags are prefixed per coordinator so they cannot collide.
- Go 1.25 is required (for `testing/synctest` and `crypto/hkdf`).

### Fixed

- A write could mint a version from an incomplete peek of its replicas. A
  restarted gateway reaching only a lagging replica reissued a version already
  in use, and a write reported as failed could become the served value. Writes
  now need a peek quorum (peek + W > RF). Replica copies are also ordered by
  (version, state, value), so equal-version splits converge. These two fixes
  were stranded on a branch lost in the triglot-kv → kvstore rename.

## [1.2.0] — 2026-09-02

Prefix scan in the engine, prefix delete, operator fault injection, and
per-op get/put latency.

### Added

**Engine (C)**

- `kv_scan_prefix(db, prefix, plen, cb, ctx)` visits live keys whose key bytes
  start with `prefix` (`memcmp`). Empty prefix (`plen == 0`) is a full scan.

**Coordinator**

- `DeletePrefix(prefix) (n int, err error)` — scan, then delete each matching
  key with a new version (best-effort quorum). Metric `PrefixDeletes`.
- EWMA milliseconds for Get and Put: `LatencyGetMs`, `LatencyPutMs` on
  `Metrics` (`latency_get_ms`, `latency_put_ms` in JSON).
- `ForceDown(addr, down)` — operator fault inject. `ForceDown(true)` skips
  writes like heartbeat-down. Heartbeat must not mark a force-downed node up
  until `ForceDown(false)`. `forced` flag on `hbState`.

**HTTP / dashboard / CLI**

- `DELETE /scan?prefix=`
- `POST /admin/fault?addr=&down=1|0`
- `/health` metrics include `latency_get_ms`, `latency_put_ms`, `prefix_deletes`
- `/health` nodes include `forced` bool
- Control room: replica-map pills on inspect (`GET /route/{key}`), fault-inject
  toggle on each node card, prefix-delete (confirm) using the current filter,
  get/put latency metrics, pretty-print JSON values in inspect.
- CLI: `kvstore rm-prefix PREFIX` and `kvstore fault ADDR on|off`.

**JS client**

- `clients/kv.js` — no-deps fetch wrapper: `get` / `put` / `del` / `scan` /
  `health`. Exports `buildUrl`.

### Tests

- Engine: put `a`/`aa`/`b`, prefix `"a"` sees `a` and `aa` only.
- Coordinator: `ForceDown` is not healed by heartbeat; `DeletePrefix`.
- HTTP: prefix delete, fault inject, health JSON fields.

## [1.1.0] — 2026-09-02

Tombstone GC without grow, an operator-facing control room, and CLI watch.

### Added

**Engine compact (C)**

- `kv_compact(db)` rehashes at the **same** capacity so tombstones and expired
  keys are dropped (GC without grow). Returns `KV_OK` / `KV_ERROR`.

**Wire protocol**

- `COMPACT` (op 9): no payload. kvnode calls `kv_compact`. Go client
  `Compact() error`.

**Coordinator**

- `CompactAll()` best-effort compact of every live node. Metric `Compacts`.
- `HintsPending() map[string]int` — queued hints per node (not just lifetime
  counters). `/health` nodes include `hints_pending`.
- `/health` nodes include `capacity` and `load` (`count/capacity`) from STAT.

**HTTP / dashboard / CLI**

- `POST /admin/compact`.
- Control room: click a key to inspect (key, value, version, replicas, expiry;
  load into put/get; get/del/route); ops log of the last ~24 actions; CAS
  `If-Match` on put; prefix filter calls `GET /scan?prefix=`; UTF-8/hex value
  toggle; per-node heartbeat, keys, tombs, load bar, pending hints; Compact
  button; watch the selected key each refresh and flash on version change.
- CLI: `kvstore compact` and `kvstore watch KEY` (poll get every second until
  Ctrl-C; print version+value).

### Tests

- Engine: fill, delete many, compact, tombstones drop, remaining keys gettable.
- HTTP: compact + health JSON fields (extra fields tolerated).

## [1.0.0] — 2026-09-02

Rename to **KVStore** plus engine v3, native TTL, hinted handoff, and heartbeat
failure detection.

### Changed

- Product/repo: `kvstore` (display name **KVStore**). CLI binary is `kvstore`.
  Gateway stays `gateway`, node stays `kvnode`.
- Go module: `github.com/Sebby1770/kvstore/kvgw`.
- HTTP headers: `X-KV-Version`, `X-KV-Responders`, `X-KV-Repaired`, `X-KV-TTL`,
  `X-KV-CAS` (and matching `X-KV-Replicas` / `X-KV-Acks` / `X-KV-Expires-At`).
  Legacy `X-Triglot-*` request headers are still accepted for one release;
  responses set both.
- TTL envelope prefix for leftover blobs: `KVTTL1\n` (still decode `TGTTL1\n`).
- Engine magic `KVST0003`, format version 3. Demo DBs: `/tmp/kvstore-node-*.db`.
- Prometheus metrics renamed `kvstore_*`.

### Added

**Engine v3 (C)**

- Slot field `expires_at` (unix seconds, 0 = never).
- `kv_put_ttl`, `kv_set_time` (0 = `time(NULL)`), `kv_rehash` (copy live
  non-expired keys, drop tombstones), `kv_scan_all`, `kv_tombstones`.
- Get/scan lazily tombstone expired occupied slots.
- Auto-grow: an insert that would return `KV_FULL` doubles capacity via rehash
  and retries once.
- Deletes of never-present keys plant a tombstone so replicas can replicate
  deletes.

**Wire protocol**

- `PUTX` (op 7): key, version, expires_at, value. Old `PUT` (op 2) is
  `expires_at = 0`.
- GET/SCAN carry `expires_at` after version. `SCAN_ALL` (op 8) visits
  tombstones. STAT includes tombstone count.

**Coordinator (Go)**

- Hinted handoff: a failed replica write stores `{addr, key, value, version,
  expires, op}` and replays PUT/DEL when that addr is reachable again.
  Metrics: `HintsStored`, `HintsReplayed`.
- Heartbeat loop (`--heartbeat`, default 2s): last-ok / down per node without
  removing anyone from the ring. Writes skip known-down replicas (count as
  failed, generate hints) rather than waiting the full timeout. Health JSON
  includes `down` and `last_ok`.
- New writes use native `expires_at` instead of wrapping bytes. Old envelopes
  still decode.
- Anti-entropy sweep replicates tombstones via `scan_all`.

**Dashboard / CLI**

- Control room at `/dashboard`: charcoal `#0b0d10`, phosphor `#9ae6b4`, amber
  `#e6b35a`, steel `#8b9bb4`. Live heartbeat state, hint metrics, key browser,
  TTL put/get/del, add/remove node, CRDT widgets, ops sparkline.
- CLI: `kvstore --gw http://127.0.0.1:8090 put hello world`.

### Tests

- Engine: TTL expiry, rehash preserves live keys and reclaims tombstones, LWW,
  CRC/magic `KV_BAD_FILE`, auto-grow.
- `TestHintedHandoff`, `TestHeartbeatMarksDown`. Existing sim/chaos/membership
  /CRDT tests updated for the new module path.

## [0.3.0] — 2026-08-17

Coordinator-side upgrades on top of the 0.2 replicated store: compare-and-set,
gateway-coordinated TTL, export/import, prefix scan, a sharper dashboard, and
a four-node Docker compose. The C file format was still v2.

### Added

**Compare-and-set**

- `Cluster.CAS(key, expectedVersion, val)` — peek the winning version, succeed
  only if it matches (expected `0` means the key must be absent), then write a
  new version. Concurrent CAS/Mutate on the same key serialize on the
  coordinator. Returns `ErrConflict` otherwise.
- `PUT /kv/{key}` accepts `If-Match: <version>` or a CAS header and answers
  **409** on conflict.
- CLI: `cas KEY EXPECTED_VERSION VALUE`.

**TTL (gateway envelope, replica-safe)**

- Optional value prefix `<magic>\n<unix>\n` + raw bytes. Values without the
  prefix stay immortal. This was **not** a C-engine feature; superseded by
  native `expires_at` in 1.0.0.
- `PutTTL(key, val, ttl)` and a TTL seconds header on PUT.
- Get treats expired keys as not found and tombstones them (best-effort, via
  read-repair). Scan omits them. Anti-entropy sweep also deletes expired keys.
- CLI: `put KEY VALUE --ttl 10s` and `expire KEY 10s`.

**Export / import**

- `GET /export` → `{entries:[{key,value,version,expires_at?}]}` (decoded
  values, no envelope leaked). `POST /import` puts each row and skips expired.
- CLI: `export` prints JSON; `import` reads JSON from stdin.
- Dashboard: Export JSON + file import.

**Prefix scan**

- `GET /scan?prefix=foo` and CLI `scan [prefix]`.

**Dashboard**

- Key search/filter, TTL seconds on put, expiry column, export/import buttons,
  and a tiny sparkline of ops per poll from `/health` metrics.

**CLI**

- `bench [N]` — N put/get pairs, print ops/sec (default 200).
- `metrics` — pretty-print coordinator counters.

**Docker**

- `docker-compose.yml` plus `Dockerfile.kvnode` / `Dockerfile.gateway`: 4
  kvnodes + 1 gateway, published on **8090**. Aimed at Linux; on macOS prefer
  `make demo` if you would rather not deal with host cgo/mmap.

### Tests

- CAS success + conflict; TTL put then get-after-expiry (injected clock);
  export/import round-trip; prefix scan; envelope encode/decode does not
  break raw values. Existing chaos/sim/cluster tests remain.

## [0.2.0] — 2026-06-21

The release that turns a single-shard toy into a **replicated, fault-tolerant,
self-healing, resizable** key-value store — while keeping the three-language
split (C storage, Rust node, Go gateway). The original README was honest that
the toy was *not consistent, not resize-safe, not crash-safe*; this addresses
all three and then some.

### Added

**Replication & consistency**

- **Rendezvous (HRW) hashing** (`internal/ring`) replaces `hash(key) mod N`.
  Adding or removing a node moves only ~1/N of keys, and only to/from that node.
- **Replication factor RF** — every key lives on RF nodes.
- **Quorum reads/writes** with **last-writer-wins by version** — W acks to
  write, R responses to read; with R + W > RF a read always sees the latest
  write.
- **Read-repair** — a read pushes the winning version back to lagging replicas.
- **Background anti-entropy** (`--anti-entropy`) — a sweep proactively
  re-replicates keys without waiting for a read.

**CRDTs**

- `internal/crdt` — a PN-Counter and an observed-remove Set whose merge is a
  lattice join (associative, commutative, idempotent). A per-key-locked
  read-merge-write (`Cluster.Mutate`) means concurrent updates all survive
  (no lost updates). Endpoints: `/crdt/counter/{key}`, `/crdt/set/{key}`.

**Operability**

- A **live dashboard** served at `/dashboard` — nodes up/down with key counts,
  every key with its replica count, and put/get/delete/route forms.
- **Live cluster membership** — nodes join/leave a running cluster via
  `/admin/nodes`; an atomic membership snapshot keeps reads lock-free and
  effective RF/R/W clamp to the live node count.
- **Metrics** — atomic op counters (reads, writes, deletes, CRDT mutations,
  quorum failures, read-repairs, sweep-repairs) on the dashboard and at
  `GET /metrics` (Prometheus text format).

**Storage engine (C)**

- Versioned slots, last-writer-wins, **versioned tombstones**, a header
  **CRC32** validated on open, and ordered `MS_SYNC` (data before metadata).
  File format bumped to v2 (superseded by `KVST0003` in 1.0.0).

**Verification**

- A deterministic **simulation / linearizability checker** (thousands of ops
  under continuous node failure, checked against a model).
- A **chaos test** that fails/heals nodes *and* grows/shrinks the cluster at
  runtime, asserting no committed data is lost (race-clean).
- A machine-checked **TLA+ spec** of the R + W > N safety property
  (`spec/QuorumKV.tla`), verified with TLC, with a negative control proving the
  condition is necessary.
- Property tests for the CRDT merge laws, the rendezvous ring (distribution +
  minimal movement), and quorum behaviour.

**Tooling**

- GitHub Actions CI building all three languages and running the fault-tolerance
  demo. `make demo` now kills a live replica and shows quorum + read-repair.

### Changed

- The wire protocol is length-prefixed and carries an 8-byte version; new
  `STAT` op and `DELETED` status threaded through C ↔ Rust ↔ Go.
- The Rust node stays deliberately policy-free: it stores what it's told and
  reports versions back. All distributed policy lives in the Go coordinator.

### Still not (honest non-goals at the time)

- Membership is gateway-driven (no gossip / auto failure detection).
- Anti-entropy covers present keys, not tombstones; no hinted handoff.
- The gateway is a single process; tombstones aren't garbage-collected.

## [0.1.0]

- The original toy: a single shard per key (`hash(key) mod N`), no replication,
  no quorum, best-effort `msync`. Honest, small, and easy to lose data with.
