#!/usr/bin/env bash
# Builds one implementation, walks it through the three migration phases,
# and runs the black-box scenario after each phase.
#
# Usage: verify/run.sh <dotnet|python|rust>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMPL_NAME="${1:?usage: verify/run.sh <dotnet|python|rust>}"
IMPL_DIR="$ROOT/$IMPL_NAME"
[[ -x "$IMPL_DIR/run.sh" ]] || { echo "unknown implementation: $IMPL_NAME" >&2; exit 2; }

GATEWAY_PORT="${GATEWAY_PORT:-8080}"
MONOLITH_PORT="${MONOLITH_PORT:-8081}"
CATALOG_PORT="${CATALOG_PORT:-8082}"
export GATEWAY_PORT MONOLITH_PORT CATALOG_PORT
MONOLITH_URL="http://127.0.0.1:$MONOLITH_PORT"
CATALOG_URL="http://127.0.0.1:$CATALOG_PORT"
HEALTH_TIMEOUT_S=60

LOG_DIR="$ROOT/verify/logs"
mkdir -p "$LOG_DIR"
STARTED_PIDS=""
LAST_PID=""

cleanup() {
  for pid in $STARTED_PIDS; do stop "$pid"; done
}
trap cleanup EXIT

start() { # start <service> [VAR=value ...]
  local service="$1"; shift
  env "$@" "$IMPL_DIR/run.sh" "$service" >"$LOG_DIR/$IMPL_NAME-$service.log" 2>&1 &
  LAST_PID=$!
  STARTED_PIDS="$STARTED_PIDS $LAST_PID"
}

stop() { # stop <pid>
  kill "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

wait_healthy() { # wait_healthy <port> <service>
  local deadline=$((SECONDS + HEALTH_TIMEOUT_S))
  until curl -fsS "http://127.0.0.1:$1/health" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then
      echo "$2 did not become healthy on port $1; see $LOG_DIR/$IMPL_NAME-$2.log" >&2
      exit 1
    fi
    sleep 0.2
  done
}

for port in "$GATEWAY_PORT" "$MONOLITH_PORT" "$CATALOG_PORT"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $port is already in use; stop that process first" >&2
    exit 1
  fi
done

echo "### building $IMPL_NAME"
"$IMPL_DIR/run.sh" build

scenario() { python3 -I "$ROOT/verify/scenario.py" --lang "$IMPL_NAME" --phase "$1"; }
FAILED=0

# Phase 1: monolith owns the catalog. catalog-service is deployed but idle.
start catalog-service PORT="$CATALOG_PORT"; CATALOG_PID=$LAST_PID
start monolith PORT="$MONOLITH_PORT" CATALOG_MODE=inprocess CATALOG_URL="$CATALOG_URL"; MONOLITH_PID=$LAST_PID
start gateway PORT="$GATEWAY_PORT" MONOLITH_URL="$MONOLITH_URL" CATALOG_URL="$CATALOG_URL" CATALOG_ROUTE=monolith
wait_healthy "$CATALOG_PORT" catalog-service
wait_healthy "$MONOLITH_PORT" monolith
wait_healthy "$GATEWAY_PORT" gateway
scenario monolith || FAILED=1

# Phase 2: catalog-service becomes the source of truth (fresh data stands in for
# a data migration) and the monolith calls it over HTTP. The gateway keeps running.
stop "$MONOLITH_PID"
stop "$CATALOG_PID"
start catalog-service PORT="$CATALOG_PORT"; CATALOG_PID=$LAST_PID
wait_healthy "$CATALOG_PORT" catalog-service
start monolith PORT="$MONOLITH_PORT" CATALOG_MODE=remote CATALOG_URL="$CATALOG_URL"; MONOLITH_PID=$LAST_PID
wait_healthy "$MONOLITH_PORT" monolith
scenario extracted || FAILED=1

# Phase 3: catalog-service goes down. Shows what the new network dependency costs.
stop "$CATALOG_PID"
scenario catalog-down || FAILED=1

exit "$FAILED"
