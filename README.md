# KVStore

A small **replicated, fault-tolerant** key-value store written in **C, Rust, and
Go** — each language in the layer it actually fits.

```
   ┌────────────────────────────────────────────────────────┐
   │  kvstore  (Go CLI / HTTP)                              │
   └───────────────────────────┬────────────────────────────┘
                               │ HTTP
   ┌───────────────────────────▼────────────────────────────┐
   │  gateway (Go) — rendezvous ring + quorum coordinator    │
   │  key -> top-RF replicas;  W acks to write,  R to read;  │
   │  last-writer-wins by version;  read-repair;  hinted      │
   │  handoff;  heartbeat failure detection;  native TTL      │
   └──┬──────────────┬──────────────┬──────────────┬─────────┘
      │ TCP          │ TCP          │ TCP          │ TCP
   ┌──▼─────┐    ┌───▼────┐     ┌───▼────┐     ┌───▼────┐
   │ kvnode │    │ kvnode │     │ kvnode │     │ kvnode │   (Rust, one per node)
   └──┬─────┘    └───┬────┘     └───┬────┘     └───┬────┘
      │ FFI          │ FFI          │ FFI          │ FFI
   ┌──▼──────────────────────────────────────────────────────┐
   │  libkvengine.a (C) — mmap'd open-addressed hash table,   │
   │  versioned slots, native expires_at, rehash/GC, CRC      │
   └──────────────────────────────────────────────────────────┘
```

- **C** owns the storage engine — a memory-mapped, open-addressed hash table
  (FNV-1a + linear probing). Every slot carries a **version** and optional
  **expires_at**. Writes are **last-writer-wins** and idempotent, deletes are
  **versioned tombstones**, expired keys are lazily tombstoned, **rehash**
  drops tombstones (GC) and auto-grows past ~70% load, and the header carries
  a **CRC32** so a torn page is caught on open.
- **Rust** owns the node daemon — a TCP server speaking a length-prefixed binary
  protocol, calling into the C engine over FFI. It's deliberately dumb: it stores
  whatever version and expiry it's told and reports them back. All distributed
  policy lives above it.
- **Go** owns the gateway and CLI — a **rendezvous-hashing ring**, a **quorum
  coordinator** (replication, read/write quorums, read-repair, hinted handoff,
  heartbeat), an HTTP front door, and `kvstore`.

## Build

Requires `cc` (clang or gcc), `cargo` (Rust 1.70+), and `go` (1.22+).

```sh
make all          # builds C lib, Rust node, Go gateway + CLI -> bin/
make test         # C engine smoke + cargo test + go test + go vet
make demo         # spins up a cluster and proves fault tolerance
```

## Demo

`make demo` starts four nodes and a gateway (`RF=3, R=2, W=2`), then **kills a
replica mid-flight** and shows reads and writes still succeed (a hint is stored
for the dead node). Bringing the node back shows **hinted handoff** plus
**read-repair**:

```
== FAULT TOLERANCE: killing replica 127.0.0.1:7071 (one of hello's three)
   heartbeat should mark it down; a read still succeeds from the survivors:
HTTP/1.1 200 OK
X-KV-Responders: 127.0.0.1:7073,127.0.0.1:7072
X-KV-Version: 1
   and so does a write — 2 of 3 replicas is still a W=2 quorum:
ok v5 acks=2/2 replicas=[127.0.0.1:7071,127.0.0.1:7073,127.0.0.1:7072]

== hinted handoff + read-repair: bring 127.0.0.1:7071 back
HTTP/1.1 200 OK
X-KV-Repaired: 127.0.0.1:7071
X-KV-Version: 5
```

## Use it manually

```sh
# 4 storage nodes
for i in 0 1 2 3; do
  bin/kvnode --listen 127.0.0.1:707$i --db /tmp/kvstore-node-$i.db --capacity 1024 &
done

# gateway: replicate each key to 3 nodes, 2 acks to write, 2 responses to read.
# --anti-entropy turns on a background repair sweep (heal without a read).
# --heartbeat (default 2s) pings nodes and replays hints.
bin/gateway --listen :8090 \
  --nodes 127.0.0.1:7070,127.0.0.1:7071,127.0.0.1:7072,127.0.0.1:7073 \
  --rf 3 --read 2 --write 2 --anti-entropy 5s --heartbeat 2s &

# drive it
bin/kvstore --gw http://127.0.0.1:8090 put hello world
bin/kvstore --gw http://127.0.0.1:8090 get hello       # -> world (v1 from [..])
bin/kvstore --gw http://127.0.0.1:8090 route hello     # -> the 3 replicas
bin/kvstore --gw http://127.0.0.1:8090 scan
bin/kvstore --gw http://127.0.0.1:8090 scan hel        # prefix scan
bin/kvstore --gw http://127.0.0.1:8090 health
```

Or with `curl` (response headers are `X-KV-*`; `X-Triglot-*` is still accepted
on requests for one release):

```sh
curl -i -X PUT --data-binary 'world' http://127.0.0.1:8090/kv/hello
curl -i http://127.0.0.1:8090/kv/hello
curl    http://127.0.0.1:8090/scan
curl    'http://127.0.0.1:8090/scan?prefix=hel'
curl    http://127.0.0.1:8090/route/hello
```

## Compare-and-set

Writes are last-writer-wins. When you need "write this only if nobody else has",
use CAS: the coordinator peeks the winning version and applies the put only if
it matches. Expected version `0` means the key must not exist (create-if-absent).
Otherwise you get a conflict and the stored value is unchanged.

```sh
# after a get (or put) the version is in X-KV-Version / the CLI note
bin/kvstore --gw http://127.0.0.1:8090 cas hello 1 world-v2
curl -i -X PUT --data-binary 'world-v3' \
  -H 'If-Match: 2' http://127.0.0.1:8090/kv/hello
# stale version → HTTP 409
curl -i -X PUT --data-binary 'nope' \
  -H 'X-KV-CAS: 1' http://127.0.0.1:8090/kv/hello
```

## TTL (native engine field)

TTL is stored as `expires_at` (unix seconds, 0 = never) on the C slot. New
writes use `PUTX` on the wire. Values previously wrapped as `KVTTL1\n` /
`TGTTL1\n` envelopes still decode on read.

```sh
bin/kvstore --gw http://127.0.0.1:8090 put session token --ttl 10s
curl -i -X PUT --data-binary 'token' -H 'X-KV-TTL: 10' \
  http://127.0.0.1:8090/kv/session
bin/kvstore --gw http://127.0.0.1:8090 expire session 30s
```

Get treats an expired key as not found and tombstones it (lazy in C, plus
read-repair). Scan omits expired keys. The optional `--anti-entropy` sweep
also deletes them.

## Hinted handoff and heartbeat

When a replica in the preference list misses a write, the coordinator stores a
**hint** `{addr, key, value, version, expires, op}` and still succeeds if the
remaining replicas make `W`. A **heartbeat** loop (default 2s) pings every
node, tracks `down` / `last_ok` **without removing anyone from the ring**
(preference lists stay stable), skips known-down replicas on the write path
instead of waiting out the full timeout, and **replays hints** when the node
answers again.

`GET /health` includes `down` and `last_ok` per node. Metrics expose
`hints_stored` / `hints_replayed`.

## Export / import

```sh
bin/kvstore --gw http://127.0.0.1:8090 export > snap.json
bin/kvstore --gw http://127.0.0.1:8090 import < snap.json

curl http://127.0.0.1:8090/export
curl -X POST --data-binary @snap.json http://127.0.0.1:8090/import
```

The document is `{ "entries": [{ "key", "value", "version", "expires_at"? }] }`.
Values are decoded (no envelope leaked). Import puts each live row and skips
already-expired ones. Versions are informational; import assigns new ones.

## Bench and metrics

```sh
bin/kvstore --gw http://127.0.0.1:8090 bench        # 200 put/get pairs
bin/kvstore --gw http://127.0.0.1:8090 bench 50
bin/kvstore --gw http://127.0.0.1:8090 metrics      # coordinator counters
```

## Docker

Four kvnodes + one gateway, dashboard published on **8090**:

```sh
docker compose up --build
curl http://127.0.0.1:8090/health
open http://127.0.0.1:8090/dashboard
```

Compose is written for Linux. On macOS the containers still run in a Linux VM
(Docker Desktop), so this usually works; if you hit awkward cgo/mmap issues
building **natively** on the host, use `make demo` instead.

## Consistency model

- **Placement**: rendezvous hashing scores every node for a key and takes the
  top `RF` as the preference list (replica set).
- **Writes**: the coordinator assigns a version greater than any it has seen
  (a Lamport-style clock, primed by a best-effort peek of the replicas), writes
  to all `RF` replicas, and needs `W` acks. Known-down replicas are skipped and
  receive a hint instead of a blocking RPC.
- **Reads**: query the replica set, need `R` responses, return the highest
  version. If `R + W > RF`, the read and write quorums always overlap, so a
  successful read observes the latest successful write.
- **Conflict resolution**: last-writer-wins by version, enforced in the C engine
  so writes are commutative and idempotent (safe to retry and replay).
- **Healing**: every read repairs replicas that are behind the winning version.
  Hinted handoff covers writes a replica missed while down. `--anti-entropy`
  sweeps live keys **and tombstones**.

## Growing and shrinking the cluster (live)

Nodes can join and leave a *running* cluster through admin endpoints — no
restart. Because placement is rendezvous hashing, adding a node only re-homes
~1/N of keys (onto the newcomer); the anti-entropy sweep (or the next read)
re-replicates them. Removing a node re-homes only its keys onto whoever the ring
picks next, and reads stay correct throughout.

```sh
curl :8090/admin/nodes                                   # current membership
curl -X POST ':8090/admin/nodes?addr=127.0.0.1:7073'     # join at runtime
curl -X DELETE :8090/admin/nodes/127.0.0.1:7070          # leave at runtime
```

The effective `RF/R/W` clamp to the live node count. Heartbeat marks a node
`down` but does **not** remove it from the ring.

## CRDT values (merge instead of overwrite)

Plain keys are last-writer-wins: two concurrent writes, one silently loses. For
counters and sets that's wrong — you want them to *merge*. The gateway exposes
**conflict-free replicated data types** whose merge is a lattice join
(associative, commutative, idempotent), coordinated by a per-key-locked
read-merge-write so concurrent updates all survive and replicas converge.

```sh
# PN-counter — fire 40 concurrent increments, get exactly 40 (no lost updates)
for i in $(seq 40); do curl -s -X POST :8090/crdt/counter/votes & done; wait
curl :8090/crdt/counter/votes                 # {"value":40}
curl -X POST ':8090/crdt/counter/votes?delta=-5'

# OR-set (observed-remove, add-wins)
curl -X POST ':8090/crdt/set/team?add=alice'
curl -X POST ':8090/crdt/set/team?add=bob'
curl -X POST ':8090/crdt/set/team?remove=bob'
curl :8090/crdt/set/team                       # {"members":["alice"]}
```

The merge laws are property-tested in [`internal/crdt`](kvgw/internal/crdt), and
a cluster test fires 50 concurrent increments through the real coordinator and
asserts none are lost.

## Live dashboard

The gateway serves a self-contained control room at `GET /dashboard` (no build
step, no dependencies). Charcoal / phosphor / amber / steel — live nodes with
heartbeat state, metrics including hints, a key browser, put/get/del with TTL,
add/remove node, CRDT counter/set widgets, and a sparkline of ops.

```sh
open http://127.0.0.1:8090/dashboard
```

Counters are also at `GET /metrics` in Prometheus text format.

## Proving it (not just demoing it)

`internal/cluster/cluster_sim_test.go` is a small deterministic simulation /
linearizability checker. For each of 25 seeds it drives the **real coordinator**
through 250 randomized `put`/`get`/`del` operations while **continuously failing
a node**, and checks every successful read against a single-source-of-truth
model — then brings the cluster back and asserts read-repair has converged every
replica. Failures are reproducible from the seed.

```sh
go test ./kvgw/internal/cluster/ -run TestSimulation -v
```

[`cluster_chaos_test.go`](kvgw/internal/cluster/cluster_chaos_test.go) goes
further: a malicious scheduler randomly fails/heals nodes *and* grows/shrinks the
cluster at runtime while reads and writes flow, then asserts no committed value
was lost.

And the *protocol* itself is model-checked: [`spec/QuorumKV.tla`](spec/QuorumKV.tla)
is a TLA+ model of KVStore's R + W > N safety property, verified with TLC.

## Wire protocol

Length-prefixed binary, big-endian, designed to be readable in `tcpdump -X`.
Values carry an 8-byte version and an 8-byte `expires_at` (0 = never).

```
Request   GET      : 1B op  4B klen  key
          PUT      : 1B op  4B klen  key  8B version  4B vlen  val     (expires=0)
          PUTX     : 1B op  4B klen  key  8B version  8B expires  4B vlen  val
          DEL      : 1B op  4B klen  key  8B version
          SCAN     : 1B op
          SCAN_ALL : 1B op   (includes tombstones)
          PING     : 1B op
          STAT     : 1B op
Response  1B status (0=ok 1=not_found 2=error 3=deleted)
GET ok       + 8B version  8B expires  4B vlen  val
GET deleted  + 8B version  8B expires
SCAN ok      + repeat(4B klen key 8B version 8B expires 4B vlen val), term klen=0xFFFFFFFF
SCAN_ALL ok  + repeat(4B klen key 8B version 8B expires 1B tomb 4B vlen val), term
STAT ok      + 4B count  4B capacity  8B max_version  4B tombstones
```

Ops: `1=GET 2=PUT 3=DEL 4=SCAN 5=PING 6=STAT 7=PUTX 8=SCAN_ALL`. See
[`kvnode/src/proto.rs`](kvnode/src/proto.rs) and
[`kvgw/internal/proto/proto.go`](kvgw/internal/proto/proto.go).

## Engine format v3

Magic `KVST0003`. Each slot is `{state, key, value, version, expires_at}`.
`kv_put` is immortal (`expires_at = 0`); `kv_put_ttl` sets expiry.
`kv_set_time` injects a clock for tests. `kv_rehash` copies live non-expired
keys and **drops tombstones**. Inserts that would return `KV_FULL` double
capacity and retry once.

## What it still is not

- **Not highly available end-to-end** — the gateway is a single process. You can
  run several behind a load balancer; they share no state, and the read-before-write
  version peek keeps versions monotonic across them.
- **Membership is still gateway-driven** — heartbeat detects unreachability for
  write-skipping and hint replay, but it does not auto-eject nodes. Join/leave
  goes through `/admin/nodes`.
- Fixed-size slots and `MS_SYNC` per write are honest, not fast.

## Layout

```
kvengine/           C — storage engine (libkvengine.a), v3 + CRC + TTL + rehash
kvnode/             Rust — TCP server + FFI
kvgw/
  internal/kv       shared value types + Node interface
  internal/ring     rendezvous hashing (+ tests)
  internal/cluster  quorum coordinator: replication, repair, CAS, TTL, hints
  internal/client   TCP client implementing kv.Node
  internal/proto    wire codec
  cmd/gateway       HTTP front door + dashboard
  cmd/kvstore       CLI
docker-compose.yml  4 kvnodes + gateway on :8090
scripts/            demo.sh
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT
