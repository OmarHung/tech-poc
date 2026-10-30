"""CAP 定理的最小模擬：同一組節點切換 CP / AP 兩種模式，看網路分區時的行為差異。

網路分區用「節點分組」表示：同一組內可以互相連線，跨組連不到。
寫入會立刻複製到同一組的所有節點，另一組收不到。
整個模擬跑在單一程序內，沒有真的網路，所以每次結果都一樣。
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import Enum


class Mode(Enum):
    CP = "CP"  # 少數派那側拒絕服務，換取一致性
    AP = "AP"  # 每個節點都照常服務，換取可用性


@dataclass(frozen=True)
class Version:
    value: str
    timestamp: int  # 全域邏輯時鐘；分區恢復時用它決定 last-write-wins 誰贏
    origin: str  # 接受這筆寫入的節點


@dataclass(frozen=True)
class Result:
    ok: bool
    value: str | None = None  # 讀到的值；寫入時為 None
    reason: str = ""  # 被拒絕的原因


@dataclass(frozen=True)
class Cluster:
    mode: Mode
    nodes: tuple[str, ...]
    stores: Mapping[str, Mapping[str, Version]]  # 節點 -> (key -> 版本)
    groups: tuple[frozenset[str], ...]  # 目前連得到彼此的節點分組
    clock: int = 0
    partitioned_at: int | None = None  # 分區發生時的時鐘；沒有分區時為 None


def new_cluster(mode: Mode, nodes: tuple[str, ...] = ("A", "B", "C")) -> Cluster:
    return Cluster(
        mode=mode,
        nodes=nodes,
        stores={node: {} for node in nodes},
        groups=(frozenset(nodes),),
    )


def partition(cluster: Cluster, *groups: Iterable[str]) -> Cluster:
    """把網路切成幾組。每個節點必須剛好出現在一組裡。"""
    frozen = tuple(frozenset(group) for group in groups)
    members = sorted(node for group in frozen for node in group)
    if members != sorted(cluster.nodes):
        raise ValueError(
            f"分組必須剛好涵蓋每個節點一次：{cluster.nodes}，收到 {groups}"
        )
    return replace(cluster, groups=frozen, partitioned_at=cluster.clock)


def write(cluster: Cluster, node: str, key: str, value: str) -> tuple[Cluster, Result]:
    peers = _reachable(cluster, node)
    rejection = _reject_without_quorum(cluster, peers)
    if rejection:
        return cluster, rejection

    clock = cluster.clock + 1
    version = Version(value=value, timestamp=clock, origin=node)
    stores = {
        name: {**store, key: version} if name in peers else store
        for name, store in cluster.stores.items()
    }
    return replace(cluster, stores=stores, clock=clock), Result(ok=True)


def read(cluster: Cluster, node: str, key: str) -> Result:
    rejection = _reject_without_quorum(cluster, _reachable(cluster, node))
    if rejection:
        return rejection

    # 同一組的節點資料永遠一致，所以讀本地就等於讀到這一組的最新值。
    # AP 少數派讀到的「最新」其實是分區前的舊資料。
    version = cluster.stores[node].get(key)
    return Result(ok=True, value=version.value if version else None)


def heal(cluster: Cluster) -> tuple[Cluster, tuple[Version, ...]]:
    """恢復連線，用 last-write-wins 合併各節點的資料。

    回傳合併後的叢集，以及「分區期間被確認過、合併時卻被丟掉」的寫入。
    """
    keys = sorted({key for store in cluster.stores.values() for key in store})
    candidates = {
        key: {store[key] for store in cluster.stores.values() if key in store}
        for key in keys
    }
    merged = {key: max(versions, key=_by_time) for key, versions in candidates.items()}

    # 分區前就存在的舊版本輸掉只是「落後」，分區期間寫入的版本輸掉才是「遺失」。
    since = cluster.clock if cluster.partitioned_at is None else cluster.partitioned_at
    lost = tuple(
        version
        for key, versions in candidates.items()
        for version in sorted(versions, key=_by_time)
        if version != merged[key] and version.timestamp > since
    )
    healed = replace(
        cluster,
        stores={node: dict(merged) for node in cluster.nodes},
        groups=(frozenset(cluster.nodes),),
        partitioned_at=None,
    )
    return healed, lost


def quorum(cluster: Cluster) -> int:
    return len(cluster.nodes) // 2 + 1


def _reachable(cluster: Cluster, node: str) -> frozenset[str]:
    group = next((group for group in cluster.groups if node in group), None)
    if group is None:
        raise ValueError(f"不存在的節點：{node}")
    return group


def _reject_without_quorum(cluster: Cluster, peers: frozenset[str]) -> Result | None:
    if cluster.mode is Mode.AP or len(peers) >= quorum(cluster):
        return None
    return Result(
        ok=False, reason=f"只連得到 {len(peers)} 台，需要 {quorum(cluster)} 台才過半"
    )


def _by_time(version: Version) -> int:
    return version.timestamp
