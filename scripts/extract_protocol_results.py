#!/usr/bin/env python3
"""Extract CSV records emitted by rdma_protocol_benchmark from rank logs."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

HEADER = [
    "trial",
    "world_size",
    "rank",
    "host",
    "mode",
    "total_bytes",
    "count_int32",
    "pipeline_piece_bytes",
    "max_inflight",
    "warmup_iterations",
    "measured_iterations",
    "iteration",
    "latency_us",
]


def main() -> int:
    if len(sys.argv) < 3:
        print(
            f"Usage: {sys.argv[0]} <output.csv> <rank-log> [rank-log ...]",
            file=sys.stderr,
        )
        return 2

    output = Path(sys.argv[1])
    logs = [Path(argument) for argument in sys.argv[2:]]
    rows: list[list[str]] = []

    for log in logs:
        if not log.is_file():
            print(f"Missing log: {log}", file=sys.stderr)
            return 1
        text = log.read_text(encoding="utf-8", errors="replace")
        if "BENCHMARK_RESULT PASS" not in text:
            print(f"Benchmark did not pass: {log}", file=sys.stderr)
            return 1
        for line in text.splitlines():
            if not line.startswith("CSV,"):
                continue
            fields = line.split(",")[1:]
            if len(fields) != len(HEADER):
                print(f"Malformed CSV line in {log}: {line}", file=sys.stderr)
                return 1
            rows.append(fields)

    if not rows:
        print("No benchmark records were found.", file=sys.stderr)
        return 1

    rows.sort(
        key=lambda row: (
            int(row[0]),       # trial
            int(row[1]),       # world size
            row[4],            # mode
            int(row[5]),       # bytes
            int(row[11]),      # iteration
            int(row[2]),       # rank
        )
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)

    print(f"Wrote {len(rows)} raw measurements to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
