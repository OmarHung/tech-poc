#!/usr/bin/env bash
# Usage: ./run.sh demo | test | metrics
set -euo pipefail
cd "$(dirname "$0")"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

case "${1:-}" in
  demo) dotnet run --project src/CleanCode -c Release ;;
  test) dotnet test --nologo ;;
  metrics)
    # 每個函式的行數（NLOC）、循環複雜度（CCN）、參數數量（PARAM）、認知複雜度（CogC，巢狀越深扣越多）
    # 注意：lizard 1.24.3 對 C# 三元運算子（?:）的 CogC 時算時不算（CCN 都有算），
    # 所以 RegionSurcharge 的 CogC 是 0，Python 版同一個函式是 1
    # Python 版另外用 ruff PLR2004 數魔術數字，那是 Python 專用的規則，這裡沒有對應
    uvx lizard -Ecognitive src/CleanCode/Legacy.cs src/CleanCode/Shipping.cs
    ;;
  *) echo "usage: $0 demo|test|metrics" >&2; exit 2 ;;
esac
