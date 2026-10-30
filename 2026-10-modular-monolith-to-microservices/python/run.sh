#!/usr/bin/env bash
# Usage: ./run.sh build | test | monolith | catalog-service | gateway
# Services read PORT, CATALOG_MODE, CATALOG_URL, MONOLITH_URL, CATALOG_ROUTE from the environment.
set -euo pipefail
cd "$(dirname "$0")"

serve() { # serve <app factory> <port>
  exec .venv/bin/uvicorn --factory "$1" --app-dir src --host 127.0.0.1 --port "$2" --no-access-log
}

case "${1:-}" in
  build) uv sync --locked ;;
  test) uv run pytest --cov=src --cov-report=term-missing ;;
  monolith) serve monolith.main:create_app "${PORT:-8081}" ;;
  catalog-service) serve catalog_service.main:create_app "${PORT:-8082}" ;;
  gateway) serve gateway.main:create_app "${PORT:-8080}" ;;
  *) echo "usage: $0 build|test|monolith|catalog-service|gateway" >&2; exit 2 ;;
esac
