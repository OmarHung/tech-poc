"""同一個情境用 CP、AP 各跑一次，把每一步的結果印出來對照。

情境：3 台節點 A、B、C 都有 x=1。網路斷成 {A, B} | {C}，
client1 連 A 寫 x=2，接著 client2 連 C 寫 x=3，最後網路恢復。
"""

from cap import Cluster, Mode, Result, heal, new_cluster, partition, read, write


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


if __name__ == "__main__":
    run_three_nodes(Mode.CP)
    run_three_nodes(Mode.AP)
    run_two_nodes()
