#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR=${1:-"$(pwd)"}
exec "$(dirname "$0")/run_group.sh" "$PROJECT_DIR" mlxstud01 mlxstud02
