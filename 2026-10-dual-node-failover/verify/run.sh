#!/usr/bin/env bash
# Builds one implementation and runs the failover scenarios against it.
#
# Usage: verify/run.sh <dotnet|python|rust> [scenario ...]   (all scenarios when none are named)
# VERIFY_PORT_BASE (default 8100) shifts every port, to run two implementations side by side.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMPL_NAME="${1:?usage: verify/run.sh <implementation> [scenario ...]}"
shift
[[ -x "$ROOT/$IMPL_NAME/run.sh" ]] || { echo "unknown implementation: $IMPL_NAME" >&2; exit 2; }

echo "### building $IMPL_NAME"
"$ROOT/$IMPL_NAME/run.sh" build
exec python3 "$ROOT/verify/scenario.py" --impl "$IMPL_NAME" "$@"
