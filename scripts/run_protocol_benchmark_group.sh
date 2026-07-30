#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <project-dir-visible-on-all-hosts> <output.csv> <host1> <host2> [host3 ...]" >&2
  exit 2
fi

PROJECT_DIR=$1
OUTPUT_CSV=$2
shift 2
HOSTS=("$@")
WORLD_SIZE=${#HOSTS[@]}
HOST_ARGS="${HOSTS[*]}"

TRIALS=${BENCH_TRIALS:-2}
SIZES=${BENCH_SIZES:-64,256,1024,2048,4096,8192,16384,65536,262144,1048576,4194304,16777216}
WARMUP=${BENCH_WARMUP:-10}
REPETITIONS=${BENCH_REPETITIONS:-0}
PIECE_BYTES=${BENCH_PIECE_BYTES:-65536}
MAX_INFLIGHT=${BENCH_MAX_INFLIGHT:-4}
TIMEOUT_SECONDS=${BENCH_TIMEOUT_SECONDS:-900}

LOG_DIR="$PROJECT_DIR/logs/protocol-${WORLD_SIZE}p"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR" "$(dirname "$OUTPUT_CSV")"

all_logs=()

for ((trial = 1; trial <= TRIALS; ++trial)); do
  if (( trial % 2 == 1 )); then
    modes=(eager rendezvous)
  else
    # Alternate order to reduce systematic warm-machine/order bias.
    modes=(rendezvous eager)
  fi

  for mode in "${modes[@]}"; do
    echo "=== trial=$trial mode=$mode processes=$WORLD_SIZE ==="
    pids=()
    run_logs=()

    for i in "${!HOSTS[@]}"; do
      host=${HOSTS[$i]}
      one_based=$(printf '%02d' $((i + 1)))
      safe_host=${host//[^A-Za-z0-9_.-]/_}
      log="$LOG_DIR/trial-${trial}-${mode}-rank-${i}-${safe_host}.log"
      run_logs+=("$log")
      all_logs+=("$log")

      echo "Launching rank $i on $host -> $log"
      ssh "$host" \
        "cd '$PROJECT_DIR' && timeout '${TIMEOUT_SECONDS}s' stdbuf -oL -eL ./build-rdma/rdma_protocol_benchmark \
          -myindex '$one_based' \
          -list $HOST_ARGS \
          -mode '$mode' \
          -sizes '$SIZES' \
          -warmup '$WARMUP' \
          -repetitions '$REPETITIONS' \
          -piece-bytes '$PIECE_BYTES' \
          -max-inflight '$MAX_INFLIGHT' \
          -trial '$trial'" \
        >"$log" 2>&1 &
      pids+=("$!")
    done

    failed=0
    for pid in "${pids[@]}"; do
      if ! wait "$pid"; then
        failed=1
      fi
    done

    for log in "${run_logs[@]}"; do
      echo "===== $(basename "$log") ====="
      grep -E 'benchmark mode=|SIZE_RESULT|BENCHMARK_RESULT|FAIL|Error' "$log" || true
      if ! grep -q 'BENCHMARK_RESULT PASS' "$log"; then
        failed=1
      fi
    done

    if [[ $failed -ne 0 ]]; then
      echo "Benchmark failed. Full logs are under $LOG_DIR" >&2
      exit 1
    fi
  done
done

python3 "$PROJECT_DIR/scripts/extract_protocol_results.py" \
  "$OUTPUT_CSV" "${all_logs[@]}"

echo "Benchmark complete: $OUTPUT_CSV"
echo "Keep the CSV and send it for analysis. Raw logs remain in $LOG_DIR"
