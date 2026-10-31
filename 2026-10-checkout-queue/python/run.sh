#!/usr/bin/env bash
# Usage: ./run.sh build | test | serve
# serve reads PORT, REDIS_URL, INSTANCE_NAME, SALE_ID, INITIAL_STOCK, MAX_PER_ORDER, MAX_ACTIVE,
# ADMISSION_WINDOW_MS, PAYMENT_WINDOW_MS, HEARTBEAT_TIMEOUT_MS, POLL_INTERVAL_MS, TICK_MS.
# test needs Docker: Testcontainers starts a throwaway Redis.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  build) uv sync --locked ;;
  test) uv run pytest --cov=src --cov-report=term-missing ;;
  # One uvicorn process on 127.0.0.1:$PORT, no access log (see src/checkout_queue/__main__.py).
  serve) PYTHONPATH=src exec .venv/bin/python -m checkout_queue ;;
  *) echo "usage: $0 build|test|serve" >&2; exit 2 ;;
esac
