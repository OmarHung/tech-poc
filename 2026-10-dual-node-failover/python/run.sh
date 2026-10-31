#!/usr/bin/env bash
# Usage: ./run.sh build | test | node | witness | gateway
# node reads PORT, NODE_ID, PEER_URL, WITNESS_URL, DATA_DIR, ARBITER, REPLICATION,
#   LEASE_TTL_MS, RENEW_INTERVAL_MS, REPLICATION_TIMEOUT_MS, ELECTION_DELAY_MS.
# witness reads PORT, DATA_DIR, NODES, LEASE_TTL_MS. gateway reads PORT, NODE_URLS, POLL_INTERVAL_MS.
set -euo pipefail
cd "$(dirname "$0")"

# exec: the verifier holds this PID and SIGKILLs it, so it must be the server itself.
# One process, no reload, no extra workers; --lifespan on makes a failing startup fatal instead of ignored.
serve() { # serve <package> <default port>
  exec .venv/bin/uvicorn --factory "$1.main:create_app" --app-dir src --host 127.0.0.1 --port "${PORT:-$2}" \
    --loop uvloop --http httptools --lifespan on --no-access-log --timeout-graceful-shutdown 5
}

case "${1:-}" in
  build) uv sync --locked ;;
  test) uv run pytest --cov=src --cov-report=term-missing ;;
  node) serve node 8101 ;;
  witness) serve witness 8103 ;;
  gateway) serve gateway 8100 ;;
  *) echo "usage: $0 build|test|node|witness|gateway" >&2; exit 2 ;;
esac
