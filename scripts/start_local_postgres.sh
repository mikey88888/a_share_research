#!/usr/bin/env bash
set -euo pipefail

PG_ROOT="${A_SHARE_PG_ROOT:-$HOME/.local/postgres16}"
PG_PKG_DIR="${A_SHARE_PG_PKG_DIR:-$HOME/.local/postgres16-pkg}"
PG_DATA="${A_SHARE_PG_DATA:-$HOME/.local/postgres16-data}"
PG_RUN="${A_SHARE_PG_RUN:-$HOME/.local/postgres16-run}"
PG_PORT="${A_SHARE_PG_PORT:-5432}"
PG_USER="${A_SHARE_PG_USER:-$USER}"
PG_DB="${A_SHARE_PG_DB:-a_share_research}"

BIN_DIR="$PG_ROOT/usr/lib/postgresql/16/bin"
LD_PATH="$PG_ROOT/usr/lib/x86_64-linux-gnu:$PG_ROOT/usr/lib/postgresql/16/lib"

download_binaries() {
  mkdir -p "$PG_PKG_DIR" "$PG_ROOT"
  (
    cd "$PG_PKG_DIR"
    apt download \
      postgresql-16 \
      postgresql-client-16 \
      postgresql-common \
      postgresql-client-common \
      libpq5 >/dev/null
  )
  for deb in "$PG_PKG_DIR"/*.deb; do
    dpkg-deb -x "$deb" "$PG_ROOT"
  done
}

if [[ ! -x "$BIN_DIR/postgres" ]]; then
  download_binaries
fi

mkdir -p "$PG_RUN"

if [[ ! -f "$PG_DATA/PG_VERSION" ]]; then
  env LD_LIBRARY_PATH="$LD_PATH" \
    "$BIN_DIR/initdb" -D "$PG_DATA" -U "$PG_USER" -A trust >/dev/null
fi

if ! env LD_LIBRARY_PATH="$LD_PATH" \
  "$BIN_DIR/pg_isready" -h "$PG_RUN" -p "$PG_PORT" -U "$PG_USER" >/dev/null 2>&1; then
  nohup env LD_LIBRARY_PATH="$LD_PATH" \
    "$BIN_DIR/postgres" \
    -D "$PG_DATA" \
    -p "$PG_PORT" \
    -k "$PG_RUN" >"$PG_DATA/server.log" 2>&1 &
  sleep 3
fi

env LD_LIBRARY_PATH="$LD_PATH" \
  "$BIN_DIR/pg_isready" -h "$PG_RUN" -p "$PG_PORT" -U "$PG_USER"

if ! env LD_LIBRARY_PATH="$LD_PATH" \
  "$BIN_DIR/psql" \
  -h "$PG_RUN" \
  -p "$PG_PORT" \
  -U "$PG_USER" \
  -d postgres \
  -tAc "SELECT 1 FROM pg_database WHERE datname = '$PG_DB'" | grep -q 1; then
  env LD_LIBRARY_PATH="$LD_PATH" \
    "$BIN_DIR/createdb" -h "$PG_RUN" -p "$PG_PORT" -U "$PG_USER" "$PG_DB"
fi

printf 'A_SHARE_PG_DSN=postgresql://%s@127.0.0.1:%s/%s\n' "$PG_USER" "$PG_PORT" "$PG_DB"
