#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR=${1:-"$(pwd)"}
OUTPUT=${2:-"$PROJECT_DIR/results/eager_vs_rendezvous_2p.csv"}
exec bash "$(dirname "$0")/run_protocol_benchmark_group.sh" \
  "$PROJECT_DIR" "$OUTPUT" \
  mlx-stud-01.cs.huji.ac.il \
  mlx-stud-02.cs.huji.ac.il
