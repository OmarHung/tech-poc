#!/usr/bin/env bash
# Usage: ./run.sh demo | test | metrics
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  demo) python3 demo.py ;;
  test) uv run pytest --cov=shipping --cov=characterization --cov-report=term-missing ;;
  metrics)
    # 每個函式的行數（NLOC）、循環複雜度（CCN）、參數數量（PARAM）、認知複雜度（CogC，巢狀越深扣越多）
    uvx lizard -Ecognitive legacy.py shipping.py
    # 比較式裡的魔術數字（ruff PLR2004；只抓比較式，算出來的是下限）
    uvx ruff check --select PLR2004 --exit-zero --statistics legacy.py shipping.py
    ;;
  *) echo "usage: $0 demo|test|metrics" >&2; exit 2 ;;
esac
