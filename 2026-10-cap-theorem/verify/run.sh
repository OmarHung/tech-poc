#!/usr/bin/env bash
# 用 Docker 起一個 3 台的 etcd 或 Cassandra 叢集，跑 scenario.py，跑完把叢集關掉。
#
# Usage: verify/run.sh etcd [follower|leader] | cassandra
set -euo pipefail
cd "$(dirname "$0")"

SYSTEM="${1:-}"
shift || true
case "$SYSTEM" in
  etcd | cassandra) ;;
  *) echo "usage: $0 etcd [follower|leader] | cassandra" >&2; exit 2 ;;
esac

COMPOSE=(docker compose --progress quiet -f "$SYSTEM/compose.yaml")
cleanup() { "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "啟動 3 台 ${SYSTEM}……"
[[ "$SYSTEM" == cassandra ]] && echo "（Cassandra 要一台一台加入叢集，約需 1～2 分鐘）"
"${COMPOSE[@]}" up --detach --wait --wait-timeout 900
python3 -I scenario.py "$SYSTEM" "$@"
