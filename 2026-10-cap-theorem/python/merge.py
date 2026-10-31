"""AP 不丟資料的兩種合併方式，對照 cap.py 的 last-write-wins。

1. version vector：每個版本記下「各節點寫過幾次」。恢復時比較時鐘，
   已經被別的版本看過的舊版本才丟掉；互相沒看過的並行寫入全部保留
   （兄弟版本），交給應用層決定怎麼合併。
2. CRDT 計數器（G-Counter）：每個節點只累加自己那一格，合併時每格取最大值，
   總數是各格相加。合併的順序和次數都不影響結果，所以可以自動合併。
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum

VectorClock = Mapping[str, int]  # 節點 -> 這個節點寫過幾次
GCounter = Mapping[str, int]  # 節點 -> 這個節點累加的量


class Order(Enum):
    BEFORE = "之前"  # a 發生在 b 之前：b 已經看過 a
    AFTER = "之後"  # b 發生在 a 之前
    EQUAL = "相同"
    CONCURRENT = "並行"  # 互相沒看過對方，誰都不該蓋掉誰


@dataclass(frozen=True)
class Sibling:
    value: str
    clock: VectorClock


def compare(a: VectorClock, b: VectorClock) -> Order:
    nodes = a.keys() | b.keys()
    a_ahead = any(a.get(node, 0) > b.get(node, 0) for node in nodes)
    b_ahead = any(b.get(node, 0) > a.get(node, 0) for node in nodes)
    if a_ahead and b_ahead:
        return Order.CONCURRENT
    if a_ahead:
        return Order.AFTER
    if b_ahead:
        return Order.BEFORE
    return Order.EQUAL


def next_version(siblings: Iterable[Sibling], node: str, value: str) -> Sibling:
    """在 node 寫入 value。新時鐘涵蓋目前所有兄弟版本，所以會取代它們。"""
    seen = _max_per_node(sibling.clock for sibling in siblings)
    return Sibling(value=value, clock={**seen, node: seen.get(node, 0) + 1})


def reconcile(versions: Iterable[Sibling]) -> tuple[Sibling, ...]:
    """合併各節點手上的版本：已經被別的版本看過的丟掉，並行的全部保留。"""
    candidates = tuple(versions)
    distinct = [v for i, v in enumerate(candidates) if v not in candidates[:i]]
    return tuple(
        version
        for version in distinct
        if not any(
            compare(version.clock, other.clock) is Order.BEFORE for other in distinct
        )
    )


def add(counter: GCounter, node: str, amount: int) -> GCounter:
    """在 node 那一格累加。G-Counter 只能增加；要能減少，就用兩個組成 PN-Counter。"""
    if amount <= 0:
        raise ValueError(f"G-Counter 只能加正數，收到 {amount}")
    return {**counter, node: counter.get(node, 0) + amount}


def merge_counters(*counters: GCounter) -> GCounter:
    """每一格取最大值。"""
    return _max_per_node(counters)


def total(counter: GCounter) -> int:
    return sum(counter.values())


def _max_per_node(tables: Iterable[Mapping[str, int]]) -> dict[str, int]:
    tables = tuple(tables)
    nodes = sorted({node for table in tables for node in table})
    return {node: max(table.get(node, 0) for table in tables) for node in nodes}
