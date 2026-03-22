#!/usr/bin/env bash
set -euo pipefail

PG_ROOT="${A_SHARE_PG_ROOT:-$HOME/.local/postgres16}"
PG_DATA="${A_SHARE_PG_DATA:-$HOME/.local/postgres16-data}"

BIN_DIR="$PG_ROOT/usr/lib/postgresql/16/bin"
LD_PATH="$PG_ROOT/usr/lib/x86_64-linux-gnu:$PG_ROOT/usr/lib/postgresql/16/lib"

if [[ ! -x "$BIN_DIR/pg_ctl" || ! -f "$PG_DATA/PG_VERSION" ]]; then
  echo "No local PostgreSQL cluster found."
  exit 0
fi

env LD_LIBRARY_PATH="$LD_PATH" \
  "$BIN_DIR/pg_ctl" -D "$PG_DATA" stop
