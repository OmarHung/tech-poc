"""同一個情境用 CP、AP 各跑一次，把每一步的結果印出來對照。

情境：3 台節點 A、B、C 都有 x=1。網路斷成 {A, B} | {C}，
client1 連 A 寫 x=2，接著 client2 連 C 寫 x=3，最後網路恢復。
"""

from collections.abc import Mapping

from cap import Cluster, Mode, Result, heal, new_cluster, partition, read, write
from merge import add, compare, merge_counters, next_version, reconcile, total


def show_write(label: str, result: Result) -> None:
    print(f"  {label} → {'成功' if result.ok else '拒絕：' + result.reason}")


def show_read(label: str, result: Result) -> None:
    print(f"  {label} → {result.value if result.ok else '拒絕：' + result.reason}")


def write_and_show(cluster: Cluster, node: str, value: str, stage: str) -> Cluster:
    cluster, result = write(cluster, node, "x", value)
    show_write(f"[{stage}] {node} 寫 x={value}", result)
    return cluster


def run_three_nodes(mode: Mode) -> None:
    print(f"\n=== {mode.value} 模式（3 台）===")
    cluster = write_and_show(new_cluster(mode), "A", "1", "正常")

    cluster = partition(cluster, {"A", "B"}, {"C"})
    print("  --- 網路分區：{A, B} | {C} ---")
    cluster = write_and_show(cluster, "A", "2", "分區")
    cluster = write_and_show(cluster, "C", "3", "分區")
    for node in ("A", "C"):
        show_read(f"[分區] {node} 讀 x", read(cluster, node, "x"))

    cluster, lost = heal(cluster)
    print("  --- 網路恢復 ---")
    for node in cluster.nodes:
        show_read(f"[恢復] {node} 讀 x", read(cluster, node, "x"))
    lost_text = "、".join(f"x={v.value}（在 {v.origin} 寫入）" for v in lost) or "無"
    print(f"  已確認卻被丟掉的寫入：{lost_text}")


def run_two_nodes() -> None:
    print("\n=== 延伸：只有 2 台的 CP（對應雙機備援）===")
    cluster = partition(new_cluster(Mode.CP, nodes=("A", "B")), {"A"}, {"B"})
    print("  --- 網路分區：{A} | {B} ---")
    write_and_show(cluster, "A", "1", "分區")
    write_and_show(cluster, "B", "1", "分區")
    print("  兩邊都湊不到過半，整個服務停擺 → 需要第三方仲裁（witness）才能撐過分區")


def format_clock(clock: Mapping[str, int]) -> str:
    return (
        "{"
        + ", ".join(f"{node}:{count}" for node, count in sorted(clock.items()))
        + "}"
    )


def run_version_vectors() -> None:
    print("  --- 方案一：version vector，並行寫入兩個都留下 ---")
    x1 = next_version([], "A", "1")
    print(f"  [正常] A 寫 x=1 → 時鐘 {format_clock(x1.clock)}，複製到 B、C")
    x2 = next_version([x1], "A", "2")
    print(f"  [分區] A 寫 x=2 → 時鐘 {format_clock(x2.clock)}")
    x3 = next_version([x1], "C", "3")
    print(f"  [分區] C 寫 x=3 → 時鐘 {format_clock(x3.clock)}")
    pair = f"{format_clock(x2.clock)} 和 {format_clock(x3.clock)}"
    print(f"  [恢復] 比較 {pair} → {compare(x2.clock, x3.clock).value}")
    siblings = reconcile([x2, x2, x3])  # A、B、C 三台手上的版本
    values = "、".join(sibling.value for sibling in siblings)
    print(f"  [恢復] 讀 x → {values}（兄弟版本都留著，交給應用層決定）")
    resolved = format_clock(next_version(siblings, "A", "5").clock)
    print(f"  [解決] 應用層合併成 x=5 寫回 A → 時鐘 {resolved}，取代兄弟版本")


def last_write_wins_total() -> str | None:
    """把計數器的總數當一般值，用 cap.py 的 AP 模式跑一次：A 那側 +2、C 那側 +3。"""
    cluster, _ = write(new_cluster(Mode.AP), "A", "x", "0")
    cluster = partition(cluster, {"A", "B"}, {"C"})
    cluster, _ = write(cluster, "A", "x", "2")
    cluster, _ = write(cluster, "C", "x", "3")
    cluster, _ = heal(cluster)
    return read(cluster, "A", "x").value


def run_counter() -> None:
    print("  --- 方案二：CRDT 計數器（G-Counter），自動合併 ---")
    print("  [分區] A 那側 +2、C 那側 +3")
    lww = last_write_wins_total()
    print(f"  [恢復] 把總數當一般值用 last-write-wins → {lww}（少了 A 的 +2）")
    merged = merge_counters(add({}, "A", 2), add({}, "C", 3))
    cells, sum_ = format_clock(merged), total(merged)
    print(f"  [恢復] G-Counter 每格取最大再相加 → {cells}，總數 {sum_}")


def run_lossless_merges() -> None:
    print("\n=== 延伸：AP 不丟資料的兩種合併方式 ===")
    run_version_vectors()
    run_counter()


if __name__ == "__main__":
    run_three_nodes(Mode.CP)
    run_three_nodes(Mode.AP)
    run_two_nodes()
    run_lossless_merges()
