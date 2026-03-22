#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${ROOT_DIR}/.dashboard.pid"
LOG_FILE="${ROOT_DIR}/.dashboard.log"
HOST="${A_SHARE_DASHBOARD_HOST:-0.0.0.0}"
PORT="${A_SHARE_DASHBOARD_PORT:-8000}"

if [[ -z "${A_SHARE_PG_DSN:-}" ]]; then
  echo "A_SHARE_PG_DSN is not set"
  exit 1
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Dashboard is already running on PID $(cat "$PID_FILE")"
  exit 0
fi

cd "$ROOT_DIR"
nohup uv run python -m a_share_research.webapp --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
sleep 3

if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Dashboard failed to start. See $LOG_FILE"
  exit 1
fi

echo "Dashboard started on http://127.0.0.1:${PORT}"
echo "PID: $(cat "$PID_FILE")"
echo "Log: $LOG_FILE"
