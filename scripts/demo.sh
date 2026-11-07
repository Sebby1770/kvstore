#!/usr/bin/env bash
# Spin up a replicated KVStore cluster and prove quorum, read-repair, and
# hinted handoff. The gateway reaches its storage nodes over KTP: encrypted,
# authenticated UDP, served by kvnode's own Rust implementation of the
# protocol. Self-cleaning.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
TMP="$(mktemp -d)"
PIDS=()

FAILED=0
cleanup() {
  set +e
  if [[ "$FAILED" -ne 0 ]]; then
    echo
    echo "=== logs ($TMP) ==="
    for f in "$TMP"/*.log; do
      [[ -f "$f" ]] || continue
      echo "--- $(basename "$f") ---"
      tail -n 20 "$f"
    done
  fi
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null
  rm -rf "$TMP"
}
trap 'FAILED=1; cleanup' ERR
trap cleanup EXIT

say()  { printf "\n\033[1;36m== %s\033[0m\n" "$*"; }
note() { printf "   \033[2m%s\033[0m\n" "$*"; }

if [[ ! -x "$BIN/kvnode" || ! -x "$BIN/gateway" || ! -x "$BIN/kvstore" ]]; then
  echo "binaries missing — run 'make all' first" >&2
  exit 1
fi

# Four nodes so a replica set of three can lose one and still hold a quorum.
NODES=(ktp://127.0.0.1:7170 ktp://127.0.0.1:7171 ktp://127.0.0.1:7172 ktp://127.0.0.1:7173)
KEY="demo-$(date +%s)"
GW_PORT=${KVSTORE_GW_PORT:-${TRIGLOT_GW_PORT:-8090}}
GW="127.0.0.1:$GW_PORT"

rm -f /tmp/kvstore-node-*.db

start_node() { # index
  local i="$1"
  "$BIN/kvnode" --ktp "${NODES[$i]#ktp://}" --db "/tmp/kvstore-node-$i.db" --capacity 1024 \
    --cluster-key "$KEY" >>"$TMP/node-$i.log" 2>&1 &
  PIDS[$i]=$!
}
idx_of() { echo $(( ${1##*:} - 7170 )); }  # ktp://127.0.0.1:7172 -> 2

say "starting 4 kvnodes (KTP: X25519 + Ed25519 handshake, AES-256-GCM, keyed to the cluster secret)"
for i in 0 1 2 3; do start_node "$i"; done
sleep 0.4
grep -h "ktp on" "$TMP"/node-*.log | sed 's/^/   /'

say "starting gateway on :$GW_PORT  (RF=3, R=2, W=2  ->  R+W>RF, strongly consistent)"
"$BIN/gateway" --listen ":$GW_PORT" --nodes "$(IFS=, ; echo "${NODES[*]}")" \
  --cluster-key "$KEY" --rf 3 --read 2 --write 2 --heartbeat 1s >"$TMP/gateway.log" 2>&1 &
PIDS+=($!)
sleep 0.5

CLI=("$BIN/kvstore" --gw "http://$GW")

say "health"
"${CLI[@]}" health
note "every storage link is an authenticated KTP connection (from the kvnode logs):"
grep -h "connected (peer" "$TMP"/node-*.log | sed 's/^/   /'

say "writing keys (each replicated to RF=3 nodes, needs W=2 acks)"
"${CLI[@]}" put hello      world
"${CLI[@]}" put language   rust+c+go
"${CLI[@]}" put colour     teal
"${CLI[@]}" put count      42

say "where does 'hello' live?  (rendezvous preference list)"
"${CLI[@]}" route hello
REPLICAS_LINE="$("${CLI[@]}" route hello 2>/dev/null)"
FIRST_REPLICA="$(echo "$REPLICAS_LINE" | sed 's/.*-> //; s/,.*//')"
VICTIM_IDX="$(idx_of "$FIRST_REPLICA")"

say "reading back (note the version and which replicas answered)"
"${CLI[@]}" get hello
"${CLI[@]}" get language

say "FAULT TOLERANCE: killing replica $FIRST_REPLICA (one of hello's three)"
kill "${PIDS[$VICTIM_IDX]}"
wait "${PIDS[$VICTIM_IDX]}" 2>/dev/null || true
sleep 1.2

note "heartbeat should mark it down; a read still succeeds from the survivors:"
curl -si "http://$GW/kv/hello" | grep -iE '^(HTTP|X-KV-(Version|Responders))' || true
echo
note "and so does a write — 2 of 3 replicas is still a W=2 quorum. It returns once the quorum"
note "is in; the dead replica's copy times out in the background and becomes a hint:"
"${CLI[@]}" put hello world-v2
sleep 2.3
curl -s "http://$GW/metrics" | grep -E '^kvstore_hints_(stored|replayed)_total' | sed 's/^/   /' || true

say "hinted handoff + read-repair: bring $FIRST_REPLICA back (it missed the world-v2 write)"
start_node "$VICTIM_IDX"
note "the restarted process has no state for the gateway's old connection: its stateless"
note "reset makes the gateway redial at once instead of waiting out an idle timeout"
sleep 1.5
note "heartbeat replays hints; a read returns the fresh value and may also read-repair:"
curl -si "http://$GW/kv/hello" | grep -iE '^(HTTP|X-KV-(Version|Responders|Repaired))' || true
echo
"${CLI[@]}" get hello
curl -s "http://$GW/metrics" | grep -E '^kvstore_hints_(stored|replayed)_total' | sed 's/^/   /' || true

say "scan — keys merged across nodes, with how many replicas hold each"
"${CLI[@]}" scan

say "delete a key (tombstoned across its replica set)"
"${CLI[@]}" del colour
"${CLI[@]}" scan

say "done — logs in $TMP (kept until process exit)"
sleep 0.3
