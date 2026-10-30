#!/usr/bin/env bash
# Usage: ./run.sh demo | test
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  demo) cargo run --quiet ;;
  test) cargo test ;;
  *) echo "usage: $0 demo|test" >&2; exit 2 ;;
esac
