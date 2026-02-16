#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

COOKIE=bench
SHARDS=8

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shards) SHARDS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "==> Compiling..."
mix deps.get --check 2>/dev/null || mix deps.get
mix compile

echo "==> Starting replica1..."
elixir --name replica1@127.0.0.1 --cookie "$COOKIE" \
  -S mix run --no-halt -e "GroupBench.Replica.start()" &
REPLICA1_PID=$!

echo "==> Starting replica2..."
elixir --name replica2@127.0.0.1 --cookie "$COOKIE" \
  -S mix run --no-halt -e "GroupBench.Replica.start()" &
REPLICA2_PID=$!

cleanup() {
  echo "==> Stopping replicas..."
  kill "$REPLICA1_PID" "$REPLICA2_PID" 2>/dev/null || true
  wait "$REPLICA1_PID" "$REPLICA2_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Give replicas a moment to boot
sleep 2

echo "==> Starting coordinator (shards=$SHARDS)..."
elixir --name coordinator@127.0.0.1 --cookie "$COOKIE" \
  -S mix run -e "GroupBench.Distributed.run(shards: $SHARDS)"
