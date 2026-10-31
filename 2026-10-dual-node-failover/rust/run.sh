#!/usr/bin/env bash
# Usage: ./run.sh build | test | node | witness | gateway
# node reads PORT, NODE_ID, PEER_URL, WITNESS_URL, DATA_DIR, ARBITER, REPLICATION,
#   LEASE_TTL_MS, RENEW_INTERVAL_MS, REPLICATION_TIMEOUT_MS, ELECTION_DELAY_MS.
# witness reads PORT, DATA_DIR, NODES, LEASE_TTL_MS. gateway reads PORT, NODE_URLS, POLL_INTERVAL_MS.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  build) cargo build --release ;;
  test) cargo test ;;
  node | witness | gateway) exec "target/release/$1" ;;
  *) echo "usage: $0 build|test|node|witness|gateway" >&2; exit 2 ;;
esac
