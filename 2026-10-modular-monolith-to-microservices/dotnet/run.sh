#!/usr/bin/env bash
# Usage: ./run.sh build | test | monolith | catalog-service | gateway
# Services read PORT, CATALOG_MODE, CATALOG_URL, MONOLITH_URL, CATALOG_ROUTE from the environment.
set -euo pipefail
cd "$(dirname "$0")"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

serve() { # serve <project>
  exec dotnet "src/$1/bin/Release/net10.0/$1.dll"
}

case "${1:-}" in
  build) dotnet build -c Release --nologo -v quiet ;;
  test) dotnet test --nologo ;;
  monolith) serve Monolith ;;
  catalog-service) serve CatalogService ;;
  gateway) serve Gateway ;;
  *) echo "usage: $0 build|test|monolith|catalog-service|gateway" >&2; exit 2 ;;
esac
