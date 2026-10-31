#!/usr/bin/env bash
# Usage: ./run.sh build | test | serve
# serve reads PORT, REDIS_URL, INSTANCE_NAME, SALE_ID, INITIAL_STOCK, MAX_PER_ORDER, MAX_ACTIVE,
# ADMISSION_WINDOW_MS, PAYMENT_WINDOW_MS, HEARTBEAT_TIMEOUT_MS, POLL_INTERVAL_MS, TICK_MS.
# test needs Docker: Testcontainers starts a throwaway Redis.
set -euo pipefail
cd "$(dirname "$0")"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

case "${1:-}" in
  build) dotnet build -c Release --nologo -v quiet ;;
  test) dotnet test --nologo ;;
  serve) exec dotnet src/CheckoutQueue/bin/Release/net10.0/CheckoutQueue.dll ;;
  *) echo "usage: $0 build|test|serve" >&2; exit 2 ;;
esac
