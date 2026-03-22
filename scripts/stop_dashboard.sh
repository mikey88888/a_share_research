#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${ROOT_DIR}/.dashboard.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "Dashboard PID file not found."
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  echo "Stopped dashboard PID $PID"
else
  echo "Dashboard process $PID is not running"
fi

rm -f "$PID_FILE"
