#!/usr/bin/env bash
# Usage: ./run.sh demo | test
set -euo pipefail
cd "$(dirname "$0")"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

case "${1:-}" in
  demo) dotnet run --project src/SolidPrinciples -c Release ;;
  test) dotnet test --nologo ;;
  *) echo "usage: $0 demo|test" >&2; exit 2 ;;
esac
