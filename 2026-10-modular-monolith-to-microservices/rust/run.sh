#!/usr/bin/env bash
# Usage: ./run.sh build | test | monolith | catalog-service | gateway
# Services read PORT, CATALOG_MODE, CATALOG_URL, MONOLITH_URL, CATALOG_ROUTE from the environment.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  build) cargo build --release ;;
  test) cargo test ;;
  monolith | catalog-service | gateway) exec "target/release/$1" ;;
  *) echo "usage: $0 build|test|monolith|catalog-service|gateway" >&2; exit 2 ;;
esac
