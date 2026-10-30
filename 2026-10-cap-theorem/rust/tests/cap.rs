use cap_theorem::{CapError, Cluster, Mode};

const THREE_NODES: [&str; 3] = ["A", "B", "C"];

/// 共用情境：3 台都有 x=1 之後，網路斷成 {A, B} | {C}。
fn partitioned(mode: Mode) -> Cluster {
    Cluster::new(mode, &THREE_NODES)
        .write("A", "x", "1")
        .unwrap()
        .partition(&[&["A", "B"], &["C"]])
        .unwrap()
}

/// 分區期間，A 寫 x=2、C 寫 x=3（C 比較晚）。CP 模式下 C 的寫入會被拒絕。
fn written_on_both_sides(mode: Mode) -> Cluster {
    let cluster = partitioned(mode).write("A", "x", "2").unwrap();
    cluster.write("C", "x", "3").unwrap_or(cluster)
}

fn values_on_every_node(cluster: &Cluster, key: &str) -> Vec<Option<String>> {
    let mut values: Vec<_> = cluster
        .nodes()
        .iter()
        .map(|node| cluster.read(node, key).unwrap().map(str::to_owned))
        .collect();
    values.dedup();
    values
}

fn some(value: &str) -> Option<String> {
    Some(value.to_owned())
}

// --- CP ---

#[test]
fn cp_minority_side_rejects_writes_and_reads_during_partition() {
    let cluster = partitioned(Mode::Cp);

    let write_result = cluster.write("C", "x", "3");
    let read_result = cluster.read("C", "x");

    let no_quorum = CapError::NoQuorum {
        reachable: 1,
        quorum: 2,
    };
    assert_eq!(write_result.unwrap_err(), no_quorum);
    assert_eq!(read_result.unwrap_err(), no_quorum);
}

#[test]
fn cp_majority_side_keeps_serving_during_partition() {
    let cluster = partitioned(Mode::Cp).write("A", "x", "2").unwrap();

    assert_eq!(cluster.read("B", "x"), Ok(Some("2")));
}

#[test]
fn cp_heal_loses_no_acknowledged_write() {
    let (cluster, lost) = written_on_both_sides(Mode::Cp).heal();

    assert!(lost.is_empty());
    assert_eq!(values_on_every_node(&cluster, "x"), vec![some("2")]);
}

#[test]
fn cp_with_two_nodes_cannot_serve_on_either_side_of_a_partition() {
    let cluster = Cluster::new(Mode::Cp, &["A", "B"])
        .partition(&[&["A"], &["B"]])
        .unwrap();

    assert!(cluster.write("A", "x", "1").is_err());
    assert!(cluster.write("B", "x", "1").is_err());
}

// --- AP ---

#[test]
fn ap_both_sides_accept_writes_during_partition() {
    let cluster = partitioned(Mode::Ap).write("A", "x", "2").unwrap();

    assert!(cluster.write("C", "x", "3").is_ok());
}

#[test]
fn ap_reads_diverge_during_partition() {
    let cluster = written_on_both_sides(Mode::Ap);

    assert_eq!(cluster.read("A", "x"), Ok(Some("2")));
    assert_eq!(cluster.read("C", "x"), Ok(Some("3")));
}

#[test]
fn ap_minority_side_reads_stale_value_during_partition() {
    let cluster = partitioned(Mode::Ap).write("A", "x", "2").unwrap();

    assert_eq!(cluster.read("C", "x"), Ok(Some("1")));
}

#[test]
fn ap_heal_with_last_write_wins_drops_an_acknowledged_write() {
    let (cluster, lost) = written_on_both_sides(Mode::Ap).heal();

    let lost: Vec<_> = lost
        .iter()
        .map(|v| (v.value.as_str(), v.origin.as_str()))
        .collect();
    assert_eq!(lost, vec![("2", "A")]);
    assert_eq!(values_on_every_node(&cluster, "x"), vec![some("3")]);
}

// --- 共通行為 ---

#[test]
fn write_returns_new_cluster_without_changing_the_original() {
    let original = Cluster::new(Mode::Ap, &THREE_NODES);

    let updated = original.write("A", "x", "1").unwrap();

    assert_eq!(original.read("A", "x"), Ok(None));
    assert_eq!(updated.read("A", "x"), Ok(Some("1")));
}

#[test]
fn heal_without_partition_loses_nothing() {
    let cluster = Cluster::new(Mode::Ap, &THREE_NODES)
        .write("A", "x", "1")
        .unwrap();

    let (_, lost) = cluster.heal();

    assert!(lost.is_empty());
}

#[test]
fn partition_requires_every_node_exactly_once() {
    let cluster = Cluster::new(Mode::Cp, &THREE_NODES);

    for groups in [
        &[&["A"][..], &["C"]][..],   // 漏掉 B
        &[&["A", "B"], &["B", "C"]], // B 重複
        &[&["A", "B"], &["C", "Z"]], // Z 不存在
    ] {
        assert_eq!(
            cluster.partition(groups).unwrap_err(),
            CapError::InvalidPartition
        );
    }
}

#[test]
fn unknown_node_is_rejected() {
    let cluster = Cluster::new(Mode::Cp, &THREE_NODES);

    assert_eq!(
        cluster.read("Z", "x"),
        Err(CapError::UnknownNode("Z".to_owned()))
    );
}
