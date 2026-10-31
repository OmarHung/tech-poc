use cap_theorem::merge::{
    GCounter, MergeError, Order, Sibling, VectorClock, add, compare, merge_counters, next_version,
    reconcile, total,
};
use cap_theorem::{Cluster, Mode};

fn clock(entries: &[(&str, u64)]) -> VectorClock {
    entries
        .iter()
        .map(|(node, count)| ((*node).to_owned(), *count))
        .collect()
}

// 跟 demo 一樣的情境：A 寫 x=1 後分區，A 寫 x=2、C 寫 x=3
fn x1() -> Sibling {
    Sibling {
        value: "1".to_owned(),
        clock: clock(&[("A", 1)]),
    }
}

fn x2_on_a() -> Sibling {
    next_version(&[x1()], "A", "2")
}

fn x3_on_c() -> Sibling {
    next_version(&[x1()], "C", "3")
}

// --- version vector ---

#[test]
fn compare_orders_clocks_by_happened_before() {
    let cases = [
        (clock(&[("A", 1)]), clock(&[("A", 2)]), Order::Before),
        (clock(&[("A", 2)]), clock(&[("A", 1)]), Order::After),
        (
            clock(&[("A", 1), ("C", 1)]),
            clock(&[("A", 1), ("C", 1)]),
            Order::Equal,
        ),
        (
            clock(&[("A", 2)]),
            clock(&[("A", 1), ("C", 1)]),
            Order::Concurrent,
        ),
        (clock(&[]), clock(&[("B", 1)]), Order::Before), // 沒出現的節點當成 0
    ];

    for (a, b, expected) in cases {
        assert_eq!(compare(&a, &b), expected, "{a:?} vs {b:?}");
    }
}

#[test]
fn next_version_supersedes_every_current_sibling() {
    let resolved = next_version(&[x2_on_a(), x3_on_c()], "A", "5");

    assert_eq!(resolved.clock, clock(&[("A", 3), ("C", 1)]));
    assert_eq!(compare(&x2_on_a().clock, &resolved.clock), Order::Before);
    assert_eq!(compare(&x3_on_c().clock, &resolved.clock), Order::Before);
}

#[test]
fn reconcile_drops_versions_that_a_newer_one_has_seen() {
    // 等同 CP 恢復：C 只是落後，不是衝突
    assert_eq!(reconcile(&[x2_on_a(), x1()]), vec![x2_on_a()]);
}

#[test]
fn reconcile_keeps_both_concurrent_writes() {
    let siblings = reconcile(&[x2_on_a(), x2_on_a(), x3_on_c()]); // A、B、C 三台的版本

    let values: Vec<&str> = siblings.iter().map(|s| s.value.as_str()).collect();
    assert_eq!(values, vec!["2", "3"]);
}

#[test]
fn resolved_version_replaces_the_siblings_after_another_reconcile() {
    let resolved = next_version(&[x2_on_a(), x3_on_c()], "A", "5");

    assert_eq!(
        reconcile(&[x2_on_a(), x3_on_c(), resolved.clone()]),
        vec![resolved]
    );
}

#[test]
fn next_version_does_not_change_the_siblings_it_was_given() {
    let original = x1();

    next_version(std::slice::from_ref(&original), "A", "2");

    assert_eq!(original.clock, clock(&[("A", 1)]));
}

// --- CRDT：G-Counter ---

#[test]
fn counter_increments_on_both_sides_merge_without_loss() {
    let on_a = add(&GCounter::new(), "A", 2).unwrap();
    let on_c = add(&GCounter::new(), "C", 3).unwrap();

    assert_eq!(total(&merge_counters(&[&on_a, &on_c])), 5);
}

#[test]
fn counter_merge_is_commutative_and_idempotent() {
    let on_a = add(&add(&GCounter::new(), "A", 2).unwrap(), "B", 1).unwrap();
    let on_c = add(&GCounter::new(), "C", 3).unwrap();

    assert_eq!(
        merge_counters(&[&on_a, &on_c]),
        merge_counters(&[&on_c, &on_a])
    );
    assert_eq!(merge_counters(&[&on_a, &on_a]), on_a);
}

#[test]
fn counter_only_accepts_positive_amounts() {
    assert_eq!(add(&GCounter::new(), "A", 0), Err(MergeError::ZeroAmount));
}

#[test]
fn last_write_wins_on_a_counter_total_loses_an_increment() {
    let cluster = Cluster::new(Mode::Ap, &["A", "B", "C"])
        .write("A", "x", "0")
        .unwrap()
        .partition(&[&["A", "B"], &["C"]])
        .unwrap()
        .write("A", "x", "2") // A 那側 +2
        .unwrap()
        .write("C", "x", "3") // C 那側 +3
        .unwrap();

    let (cluster, _) = cluster.heal();

    assert_eq!(cluster.read("A", "x"), Ok(Some("3")));
}
