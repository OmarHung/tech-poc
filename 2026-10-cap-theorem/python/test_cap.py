import pytest

from cap import Cluster, Mode, heal, new_cluster, partition, read, write


def partitioned(mode: Mode) -> Cluster:
    """共用情境：3 台都有 x=1 之後，網路斷成 {A, B} | {C}。"""
    cluster, _ = write(new_cluster(mode), "A", "x", "1")
    return partition(cluster, {"A", "B"}, {"C"})


def written_on_both_sides(mode: Mode) -> Cluster:
    """分區期間，A 寫 x=2、C 寫 x=3（C 比較晚）。"""
    cluster, _ = write(partitioned(mode), "A", "x", "2")
    cluster, _ = write(cluster, "C", "x", "3")
    return cluster


def values_on_every_node(cluster: Cluster, key: str) -> set[str | None]:
    return {read(cluster, node, key).value for node in cluster.nodes}


# --- CP ---


def test_cp_minority_side_rejects_writes_and_reads_during_partition():
    cluster = partitioned(Mode.CP)

    _, write_result = write(cluster, "C", "x", "3")
    read_result = read(cluster, "C", "x")

    assert not write_result.ok
    assert not read_result.ok


def test_cp_majority_side_keeps_serving_during_partition():
    cluster, write_result = write(partitioned(Mode.CP), "A", "x", "2")

    assert write_result.ok
    assert read(cluster, "B", "x").value == "2"


def test_cp_heal_loses_no_acknowledged_write():
    cluster, lost = heal(written_on_both_sides(Mode.CP))

    assert lost == ()
    assert values_on_every_node(cluster, "x") == {"2"}


def test_cp_with_two_nodes_cannot_serve_on_either_side_of_a_partition():
    cluster = partition(new_cluster(Mode.CP, nodes=("A", "B")), {"A"}, {"B"})

    _, on_a = write(cluster, "A", "x", "1")
    _, on_b = write(cluster, "B", "x", "1")

    assert not on_a.ok
    assert not on_b.ok


# --- AP ---


def test_ap_both_sides_accept_writes_during_partition():
    cluster, on_a = write(partitioned(Mode.AP), "A", "x", "2")
    _, on_c = write(cluster, "C", "x", "3")

    assert on_a.ok
    assert on_c.ok


def test_ap_reads_diverge_during_partition():
    cluster = written_on_both_sides(Mode.AP)

    assert read(cluster, "A", "x").value == "2"
    assert read(cluster, "C", "x").value == "3"


def test_ap_minority_side_reads_stale_value_during_partition():
    cluster, _ = write(partitioned(Mode.AP), "A", "x", "2")

    assert read(cluster, "C", "x").value == "1"


def test_ap_heal_with_last_write_wins_drops_an_acknowledged_write():
    cluster, lost = heal(written_on_both_sides(Mode.AP))

    assert [(v.value, v.origin) for v in lost] == [("2", "A")]
    assert values_on_every_node(cluster, "x") == {"3"}


# --- 共通行為 ---


def test_write_returns_new_cluster_without_changing_the_original():
    original = new_cluster(Mode.AP)

    updated, _ = write(original, "A", "x", "1")

    assert read(original, "A", "x").value is None
    assert read(updated, "A", "x").value == "1"


def test_heal_without_partition_loses_nothing():
    cluster, _ = write(new_cluster(Mode.AP), "A", "x", "1")

    _, lost = heal(cluster)

    assert lost == ()


@pytest.mark.parametrize(
    "groups",
    [
        ({"A"}, {"C"}),  # 漏掉 B
        ({"A", "B"}, {"B", "C"}),  # B 重複
        ({"A", "B"}, {"C", "Z"}),  # Z 不存在
    ],
)
def test_partition_requires_every_node_exactly_once(groups):
    with pytest.raises(ValueError):
        partition(new_cluster(Mode.CP), *groups)


def test_unknown_node_is_rejected():
    with pytest.raises(ValueError):
        read(new_cluster(Mode.CP), "Z", "x")
