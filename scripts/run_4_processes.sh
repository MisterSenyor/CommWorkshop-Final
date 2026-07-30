#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR=${1:-"$(pwd)"}
exec "$(dirname "$0")/run_group.sh" "$PROJECT_DIR" mlx-stud-01.cs.huji.ac.il mlx-stud-02.cs.huji.ac.il mlx-stud-03.cs.huji.ac.il mlx-stud-04.cs.huji.ac.il
