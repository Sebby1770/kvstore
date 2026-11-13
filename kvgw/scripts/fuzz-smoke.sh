#!/usr/bin/env bash
# Runs every Fuzz* target in the runtime and networking stack for a short,
# fixed time. Not a substitute for long fuzzing campaigns -- a tripwire that
# catches decoders which panic on the first few thousand hostile inputs.
#
#   scripts/fuzz-smoke.sh [duration]     (default 10s per target)
#
# One failure mode is the toolchain's, not ours: when a -fuzztime window ends,
# the fuzz coordinator can observe its child context before the cancellation
# has propagated to it and report a bare "context deadline exceeded" as the
# result (golang/go#75804; not fixed in Go 1.25 or 1.26). That run found
# nothing -- it fuzzed for the whole window -- so the target is run again
# rather than failed (up to five times: the race hits ~3% of runs, but in
# bursts when the machine is loaded). Only that exact signature is retried: a real crash
# writes a reproducer ("Failing input written to"), and a failing seed says
# "failure while testing seed corpus entry"; either one fails the script.
set -uo pipefail
cd "$(dirname "$0")/.."
T="${1:-10s}"
LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT

engine_race() {
  grep -qE '^[[:space:]]*context deadline exceeded[[:space:]]*$' "$LOG" &&
    ! grep -qE 'Failing input written to|failure while testing seed corpus entry|panic:|fuzzing process hung or terminated' "$LOG"
}

for pkg in $(go list ./netstack/... ./runtime/...); do
  for f in $(go test -list '^Fuzz' "$pkg" | grep '^Fuzz' || true); do
    attempt=1
    while :; do
      echo "== fuzz $pkg $f ($T)"
      if go test "$pkg" -run '^$' -fuzz "^${f}\$" -fuzztime "$T" 2>&1 | tee "$LOG"; then
        break
      fi
      if engine_race && [ "$attempt" -lt 5 ]; then
        attempt=$((attempt + 1))
        echo "== $f: the fuzz engine's end-of-fuzztime race (golang/go#75804), not a finding; retrying ($attempt/5)"
        continue
      fi
      echo "== FAILED: $pkg $f" >&2
      exit 1
    done
  done
done
