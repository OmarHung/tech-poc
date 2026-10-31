import pytest

from cap import Mode, heal, new_cluster, partition, read, write
from merge import (
    Order,
    Sibling,
    add,
    compare,
    merge_counters,
    next_version,
    reconcile,
    total,
)

# 跟 demo 一樣的情境：A 寫 x=1 後分區，A 寫 x=2、C 寫 x=3
X1 = Sibling(value="1", clock={"A": 1})
X2_ON_A = next_version([X1], "A", "2")
X3_ON_C = next_version([X1], "C", "3")


# --- version vector ---


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ({"A": 1}, {"A": 2}, Order.BEFORE),
        ({"A": 2}, {"A": 1}, Order.AFTER),
        ({"A": 1, "C": 1}, {"A": 1, "C": 1}, Order.EQUAL),
        ({"A": 2}, {"A": 1, "C": 1}, Order.CONCURRENT),
        ({}, {"B": 1}, Order.BEFORE),  # 沒出現的節點當成 0
    ],
)
def test_compare_orders_clocks_by_happened_before(a, b, expected):
    assert compare(a, b) is expected


def test_next_version_supersedes_every_current_sibling():
    resolved = next_version([X2_ON_A, X3_ON_C], "A", "5")

    assert resolved.clock == {"A": 3, "C": 1}
    assert compare(X2_ON_A.clock, resolved.clock) is Order.BEFORE
    assert compare(X3_ON_C.clock, resolved.clock) is Order.BEFORE


def test_reconcile_drops_versions_that_a_newer_one_has_seen():
    # 等同 CP 恢復：C 只是落後，不是衝突
    assert reconcile([X2_ON_A, X1]) == (X2_ON_A,)


def test_reconcile_keeps_both_concurrent_writes():
    siblings = reconcile([X2_ON_A, X2_ON_A, X3_ON_C])  # A、B、C 三台的版本

    assert [s.value for s in siblings] == ["2", "3"]


def test_resolved_version_replaces_the_siblings_after_another_reconcile():
    resolved = next_version([X2_ON_A, X3_ON_C], "A", "5")

    assert reconcile([X2_ON_A, X3_ON_C, resolved]) == (resolved,)


def test_next_version_does_not_change_the_siblings_it_was_given():
    next_version([X1], "A", "2")

    assert X1.clock == {"A": 1}


# --- CRDT：G-Counter ---


def test_counter_increments_on_both_sides_merge_without_loss():
    on_a = add({}, "A", 2)
    on_c = add({}, "C", 3)

    assert total(merge_counters(on_a, on_c)) == 5


def test_counter_merge_is_commutative_and_idempotent():
    on_a = add(add({}, "A", 2), "B", 1)
    on_c = add({}, "C", 3)

    assert merge_counters(on_a, on_c) == merge_counters(on_c, on_a)
    assert merge_counters(on_a, on_a) == on_a


def test_counter_only_accepts_positive_amounts():
    with pytest.raises(ValueError):
        add({}, "A", 0)


def test_last_write_wins_on_a_counter_total_loses_an_increment():
    cluster, _ = write(new_cluster(Mode.AP), "A", "x", "0")
    cluster = partition(cluster, {"A", "B"}, {"C"})
    cluster, _ = write(cluster, "A", "x", "2")  # A 那側 +2
    cluster, _ = write(cluster, "C", "x", "3")  # C 那側 +3

    cluster, _ = heal(cluster)

    assert read(cluster, "A", "x").value == "3"
