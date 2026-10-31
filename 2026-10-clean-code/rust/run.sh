#!/usr/bin/env bash
# Usage: ./run.sh demo | test | metrics
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-}" in
  demo) cargo run --quiet ;;
  test) cargo test ;;
  metrics)
    # 每個函式的行數（NLOC）、循環複雜度（CCN）、參數數量（PARAM）、認知複雜度（CogC，巢狀越深扣越多）
    uvx lizard -Ecognitive src/legacy.rs src/shipping.rs
    ;;
  *) echo "usage: $0 demo|test|metrics" >&2; exit 2 ;;
esac
