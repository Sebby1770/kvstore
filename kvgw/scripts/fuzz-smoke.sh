#!/usr/bin/env bash
# Runs every Fuzz* target in the runtime and networking stack for a short,
# fixed time. Not a substitute for long fuzzing campaigns -- a tripwire that
# catches decoders which panic on the first few thousand hostile inputs.
#
#   scripts/fuzz-smoke.sh [duration]     (default 10s per target)
set -euo pipefail
cd "$(dirname "$0")/.."
T="${1:-10s}"
for pkg in $(go list ./netstack/... ./runtime/...); do
  for f in $(go test -list '^Fuzz' "$pkg" | grep '^Fuzz' || true); do
    echo "== fuzz $pkg $f ($T)"
    go test "$pkg" -run '^$' -fuzz "^${f}\$" -fuzztime "$T"
  done
done
