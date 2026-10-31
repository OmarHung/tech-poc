#!/usr/bin/env bash
# Usage: ./run.sh demo | test
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  demo) python3 demo.py ;;
  test) uv run pytest --cov=srp --cov=ocp --cov=lsp --cov=isp --cov=dip --cov-report=term-missing ;;
  *) echo "usage: $0 demo|test" >&2; exit 2 ;;
esac
