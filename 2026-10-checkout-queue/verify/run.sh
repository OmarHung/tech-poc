#!/usr/bin/env bash
# Starts a throwaway Redis and two instances of one implementation, then runs the
# flash-sale scenario (correctness) and the throughput scenario against them.
#
# Usage: verify/run.sh <dotnet>
# Needs Docker for Redis.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMPL_NAME="${1:?usage: verify/run.sh <dotnet>}"
IMPL_DIR="$ROOT/$IMPL_NAME"
[[ -x "$IMPL_DIR/run.sh" ]] || { echo "unknown implementation: $IMPL_NAME" >&2; exit 2; }

REDIS_IMAGE="redis:7.4-alpine"
REDIS_PORT="${REDIS_PORT:-6390}"
PORT_A="${PORT_A:-8091}"
PORT_B="${PORT_B:-8092}"
export APP_PORTS="$PORT_A,$PORT_B"
HEALTH_TIMEOUT_S=60

LOG_DIR="$ROOT/verify/logs"
mkdir -p "$LOG_DIR"
REDIS_CONTAINER="checkout-queue-redis-$$"
STARTED_PIDS=""

cleanup() {
  for pid in $STARTED_PIDS; do kill "$pid" 2>/dev/null || true; done
  docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_healthy() { # wait_healthy <port> <log>
  local deadline=$((SECONDS + HEALTH_TIMEOUT_S))
  until curl -fsS "http://127.0.0.1:$1/health" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then
      echo "instance on port $1 did not become healthy; see $2" >&2
      exit 1
    fi
    sleep 0.2
  done
}

start_instances() { # start_instances <phase>; a fresh SALE_ID per run, sale settings from the environment
  STARTED_PIDS=""
  local index=0
  for port in "$PORT_A" "$PORT_B"; do
    local log="$LOG_DIR/$IMPL_NAME-$1-instance-$index.log"
    env PORT="$port" INSTANCE_NAME="instance-$index" SALE_ID="$1-$RUN_ID" REDIS_URL="127.0.0.1:$REDIS_PORT" \
      "$IMPL_DIR/run.sh" serve >"$log" 2>&1 &
    STARTED_PIDS="$STARTED_PIDS $!"
    wait_healthy "$port" "$log"
    index=$((index + 1))
  done
}

stop_instances() {
  for pid in $STARTED_PIDS; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
  STARTED_PIDS=""
}

for port in "$REDIS_PORT" "$PORT_A" "$PORT_B"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $port is already in use; stop that process first" >&2
    exit 1
  fi
done

echo "### building $IMPL_NAME"
"$IMPL_DIR/run.sh" build

echo "### starting $REDIS_IMAGE on 127.0.0.1:$REDIS_PORT"
docker run -d --rm --name "$REDIS_CONTAINER" -p "127.0.0.1:$REDIS_PORT:6379" "$REDIS_IMAGE" >/dev/null
until docker exec "$REDIS_CONTAINER" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done

scenario() { python3 -I "$ROOT/verify/scenario.py" "$1" --lang "$IMPL_NAME"; }
FAILED=0
RUN_ID="$(date +%s)"

# Flash sale: short windows so that every timeout path happens within seconds.
export INITIAL_STOCK=200 MAX_PER_ORDER=2 MAX_ACTIVE=20 USERS=1000 \
  ADMISSION_WINDOW_MS=4000 PAYMENT_WINDOW_MS=3000 HEARTBEAT_TIMEOUT_MS=5000 POLL_INTERVAL_MS=1000 TICK_MS=100
start_instances flash-sale
scenario flash-sale || FAILED=1
stop_instances

# Throughput: plenty of stock and one checkout slot, so almost everyone stays in line and polls.
export INITIAL_STOCK=1000000 MAX_ACTIVE=1
start_instances throughput
scenario throughput || FAILED=1
stop_instances

exit "$FAILED"
