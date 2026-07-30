#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <project-dir-visible-on-all-hosts> <host1> [host2 ...]" >&2
  exit 2
fi

PROJECT_DIR=$1
shift
HOSTS=("$@")
HOST_ARGS="${HOSTS[*]}"
mkdir -p "$PROJECT_DIR/logs"

pids=()
for i in "${!HOSTS[@]}"; do
  host=${HOSTS[$i]}
  one_based=$(printf '%02d' $((i + 1)))
  log="$PROJECT_DIR/logs/${#HOSTS[@]}p-${host}.log"
  echo "Launching rank $i on $host -> $log"
  ssh "$host" \
    "cd '$PROJECT_DIR' && timeout 180s ./build-rdma/rdma_test_runner -myindex '$one_based' -list $HOST_ARGS" \
    >"$log" 2>&1 &
  pids+=("$!")
done

failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    failed=1
  fi
done

for host in "${HOSTS[@]}"; do
  log="$PROJECT_DIR/logs/${#HOSTS[@]}p-${host}.log"
  echo "===== $host ====="
  cat "$log"
done

if grep -L 'TEST_RESULT PASS' "$PROJECT_DIR"/logs/${#HOSTS[@]}p-*.log >/dev/null; then
  failed=1
fi

if [[ $failed -ne 0 ]]; then
  echo "At least one rank failed. Inspect the logs above." >&2
  exit 1
fi

echo "All ${#HOSTS[@]} ranks passed."
