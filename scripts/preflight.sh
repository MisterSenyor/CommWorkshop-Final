#!/usr/bin/env bash
set -euo pipefail

echo "host: $(hostname -f 2>/dev/null || hostname)"
echo "kernel: $(uname -r)"
echo "compiler: $(g++ --version | head -1)"

echo "--- Verbs devices ---"
if command -v ibv_devices >/dev/null 2>&1; then
  ibv_devices
else
  echo "ibv_devices not found"
fi

if command -v ibv_devinfo >/dev/null 2>&1; then
  ibv_devinfo | sed -n '1,80p'
else
  echo "ibv_devinfo not found"
fi

echo "--- libibverbs ---"
ldconfig -p 2>/dev/null | grep ibverbs || true
