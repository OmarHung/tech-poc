#!/usr/bin/env bash
# Usage: ./run.sh build | test | node | witness | gateway | upstream | worker | sink
# Primary-standby with replication:
#   node reads PORT, NODE_ID, PEER_URL, WITNESS_URL, DATA_DIR, ARBITER, REPLICATION,
#     LEASE_TTL_MS, RENEW_INTERVAL_MS, REPLICATION_TIMEOUT_MS, ELECTION_DELAY_MS.
#   witness reads PORT, DATA_DIR, NODES, LEASE_TTL_MS. gateway reads PORT, NODE_URLS, POLL_INTERVAL_MS.
# Workers that never talk to each other (hot-hot, or dispatched by the upstream):
#   upstream reads PORT, TASK_LEASE_TTL_MS. sink reads PORT.
#   worker reads PORT, WORKER_ID, MODE, UPSTREAM_URL, SINK_URL, DATA_DIR, START_FROM, DETERMINISTIC,
#     LEASE_CHECK, WORK_MS, RPC_TIMEOUT_MS.
set -euo pipefail
cd "$(dirname "$0")"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

serve() { # serve <project>
  exec dotnet "src/$1/bin/Release/net10.0/$1.dll"
}

case "${1:-}" in
  build) dotnet build -c Release --nologo -v quiet ;;
  test) dotnet test --nologo ;;
  node) serve Node ;;
  witness) serve Witness ;;
  gateway) serve Gateway ;;
  upstream) serve Upstream ;;
  worker) serve Worker ;;
  sink) serve Sink ;;
  *) echo "usage: $0 build|test|node|witness|gateway|upstream|worker|sink" >&2; exit 2 ;;
esac
