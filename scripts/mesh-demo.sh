#!/usr/bin/env bash
# KVStore 2 mesh demo: three storage nodes (Rust + C) and three kvmesh nodes
# (Go: KTP transport, SWIM, Raft control plane, virtual actors, KV
# coordinator) on real UDP sockets. Proves: any node serves any request,
# leases with fencing tokens, job queues, actors that migrate with their
# state, control-plane failover after kill -9, and a fourth node joining
# live. Self-cleaning.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
TMP="$(mktemp -d)"
PIDS=()
KEY="mesh-demo-$(date +%s)"

FAILED=0
cleanup() {
  set +e
  if [[ "$FAILED" -ne 0 ]]; then
    echo
    echo "=== logs ($TMP) ==="
    for f in "$TMP"/*.log; do
      [[ -f "$f" ]] || continue
      echo "--- $(basename "$f") ---"
      tail -n 25 "$f"
    done
  fi
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$TMP"
}
trap 'FAILED=1; cleanup; exit 1' ERR
trap cleanup EXIT

say()  { printf "\n\033[1;36m== %s\033[0m\n" "$*"; }
note() { printf "   \033[2m%s\033[0m\n" "$*"; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

for b in kvnode kvmesh kvstore; do
  [[ -x "$BIN/$b" ]] || { echo "bin/$b missing — run 'make all' first" >&2; exit 1; }
done

say "storage tier: 3 kvnodes (Rust daemon + C engine)"
KV=""
for i in 0 1 2; do
  "$BIN/kvnode" --listen "127.0.0.1:717$i" --db "$TMP/kv$i.db" --capacity 1024 >"$TMP/kvnode$i.log" 2>&1 &
  PIDS+=($!)
  KV="${KV:+$KV,}127.0.0.1:717$i"
done
note "$KV"

BOOT="n1=127.0.0.1:7201,n2=127.0.0.1:7202,n3=127.0.0.1:7203"
# PIDs live in files: macOS's bash 3.2 has no associative arrays.
start_mesh() { # name index [extra args...]
  local name=$1 i=$2; shift 2
  "$BIN/kvmesh" --name "$name" --listen "127.0.0.1:720$i" --http "127.0.0.1:820$i" \
    --data "$TMP/$name" --cluster-key "$KEY" --kvnodes "$KV" --anti-entropy 5s "$@" \
    >"$TMP/$name.log" 2>&1 &
  echo $! >"$TMP/$name.pid"
  PIDS+=($!)
}

say "mesh tier: 3 kvmesh nodes bootstrapping a Raft control plane over KTP/UDP"
for i in 1 2 3; do start_mesh "n$i" "$i" --bootstrap "$BOOT"; done

status() { curl -s "http://127.0.0.1:820$1/mesh/status"; }
ready=0
for _ in $(seq 1 80); do
  if s=$(status 1 2>/dev/null) && [[ -n "$s" ]]; then
    ok=$(echo "$s" | json "int(d['raft']['leader']!='' and d['control']['ring']['epoch']>0 and sum(m['state']=='alive' for m in d['members'])==3)")
    if [[ "$ok" == 1 ]]; then ready=1; break; fi
  fi
  sleep 0.25
done
[[ "$ready" == 1 ]] || { echo "mesh did not converge" >&2; false; }
status 1 | json "'   members: ' + ', '.join(m['name']+'='+m['state'] for m in d['members'])"
status 1 | json "'   raft: leader ' + d['raft']['leader'] + ', term ' + str(d['raft']['term']) + ', voters ' + ','.join(d['raft']['voters'])"
status 1 | json "'   storage ring (from the control plane): epoch ' + str(d['control']['ring']['epoch']) + ' ' + ','.join(d['control']['ring']['nodes'])"

say "KV: write through n1, read through n3 (no single gateway)"
curl -s -o /dev/null -w "   PUT /kv/hello via n1 -> %{http_code}\n" -X PUT --data-binary 'world' http://127.0.0.1:8201/kv/hello
printf "   GET /kv/hello via n3 -> %s\n" "$(curl -s http://127.0.0.1:8203/kv/hello)"

say "leases with fencing tokens (Raft-replicated)"
curl -s -X POST 'http://127.0.0.1:8202/lease/db-primary?holder=alice&ttl=60s' | json "'   alice: token ' + str(d['token']) + ' (holder ' + d['holder'] + ')'"
code=$(curl -s -o "$TMP/bob.json" -w "%{http_code}" -X POST 'http://127.0.0.1:8203/lease/db-primary?holder=bob&ttl=60s')
printf "   bob:   HTTP %s (%s)\n" "$code" "$(json "d.get('code','')" <"$TMP/bob.json")"

say "job queue: enqueue on n1, claim on n2, complete on n3"
curl -s -X POST --data-binary 'send welcome email' 'http://127.0.0.1:8201/jobs/emails?id=welcome-42' >/dev/null
claim=$(curl -s -X POST 'http://127.0.0.1:8202/jobs/emails/claim?worker=w1&visibility=30s')
tok=$(echo "$claim" | json "d['token']")
echo "$claim" | json "'   claimed ' + d['id'] + ' (\"' + d['payload'] + '\") token ' + str(d['token'])"
curl -s -X POST "http://127.0.0.1:8203/jobs/emails/welcome-42/complete?token=$tok" | json "'   completed, state ' + d['state']"

say "virtual actor: one counter, asked through every node"
for i in 1 2 3 1; do
  curl -s -X POST --data-binary 'inc' "http://127.0.0.1:820$i/actor/counter/visits" | json "'   via n$i: counter=' + d['reply'] + '  (activated on ' + d['node'] + ')'"
done
owner=$(curl -s -X POST --data-binary 'get' http://127.0.0.1:8201/actor/counter/visits | json "d['node']")

say "transport: KTP connections from n1"
status 1 | json "'\n'.join('   %s %-4s srtt %.2fms  cwnd %dKiB  sent %d  lost %d  (%s)' % ('->' if t['client'] else '<-', t['peer'], t['srtt_ms'], t['cwnd']//1024, t['packets_sent'], t['packets_lost'], t['congestion']) for t in d['transport'])"
note "one encrypted KTP connection per peer pair, reused in both directions"

leader=$(status 1 | json "d['raft']['leader']")
oldterm=$(status 1 | json "d['raft']['term']")
say "fault: kill -9 the actor's owner ($owner) and the control-plane leader ($leader)"
kill -9 "$(cat "$TMP/$owner.pid")" 2>/dev/null || true
if [[ "$leader" != "$owner" ]]; then kill -9 "$(cat "$TMP/$leader.pid")" 2>/dev/null || true; fi
survivors=()
for i in 1 2 3; do
  if [[ "n$i" != "$owner" && "n$i" != "$leader" ]]; then survivors+=("$i"); fi
done
s="${survivors[0]}"
note "survivor: n$s"
if [[ "$leader" != "$owner" ]]; then
  note "two of three Raft voters are down: the control plane has no quorum, so the"
  note "data plane carries on alone. Bringing $leader back..."
  start_mesh "$leader" "${leader#n}" --bootstrap "$BOOT"
fi
newlead=""
for _ in $(seq 1 120); do
  newlead=$(status "$s" | json "d['raft']['leader'] + ' (term ' + str(d['raft']['term']) + ')' if d['raft']['leader'] and d['raft']['term'] > $oldterm else ''" 2>/dev/null || true)
  [[ -n "$newlead" ]] && break
  sleep 0.25
done
note "control plane: re-elected, leader $newlead"
# The actor's owner is gone: as soon as SWIM stops believing it is alive,
# placement moves the actor and the next call re-activates it elsewhere
# from the state it last persisted.
start=$(date +%s)
reply=""
for _ in $(seq 1 120); do
  reply=$(curl -s -X POST --data-binary 'inc' "http://127.0.0.1:820$s/actor/counter/visits" | json "d.get('reply','') and d['reply'] + ' (re-activated on ' + d['node'] + ')'" 2>/dev/null || true)
  [[ -n "$reply" ]] && break
  sleep 0.25
done
[[ -n "$reply" ]] || { echo "actor never failed over" >&2; false; }
echo "   counter=$reply after $(( $(date +%s) - start ))s"
for _ in $(seq 1 120); do
  dead=$(status "$s" | json "int(all(m['state']!='alive' for m in d['members'] if m['name']=='$owner'))")
  [[ "$dead" == 1 ]] && break
  sleep 0.25
done
status "$s" | json "'   SWIM on n$s: ' + ', '.join(m['name']+'='+m['state'] for m in d['members'])"
printf "   GET /kv/hello via n%s -> %s\n" "$s" "$(curl -s "http://127.0.0.1:820$s/kv/hello")"
code=""
for _ in $(seq 1 60); do # needs a control-plane quorum again
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:820$s/lease/db-primary?holder=bob&ttl=60s")
  [[ "$code" == 409 ]] && break
  sleep 0.25
done
echo "   bob still cannot take alice's lease after failover: HTTP $code"

say "elastic: n4 joins via gossip, is added as a Raft learner, then promoted"
start_mesh n4 4 --join 127.0.0.1:720$s
for _ in $(seq 1 80); do
  v=$(status "$s" | json "int('n4' in d['raft']['voters'])" 2>/dev/null || echo 0)
  [[ "$v" == 1 ]] && break
  sleep 0.25
done
status "$s" | json "'   voters: ' + ','.join(d['raft']['voters']) + '  learners: ' + (','.join(d['raft']['learners']) or '-')"
printf "   GET /kv/hello via n4 -> %s\n" "$(curl -s http://127.0.0.1:8204/kv/hello)"

say "event log (hybrid-logical-clock ordered) from n$s"
status "$s" | json "'\n'.join('   %s  %-16s %s' % (e['hlc'], e['kind'], e['detail']) for e in d['events'][-12:])"

say "done — open http://127.0.0.1:820$s/mesh while it runs to see the control room"
