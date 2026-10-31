//! AP 不丟資料的兩種合併方式，對照 `Cluster::heal()` 的 last-write-wins。
//!
//! 1. version vector：每個版本記下「各節點寫過幾次」。恢復時比較時鐘，
//!    已經被別的版本看過的舊版本才丟掉；互相沒看過的並行寫入全部保留
//!    （兄弟版本），交給應用層決定怎麼合併。
//! 2. CRDT 計數器（G-Counter）：每個節點只累加自己那一格，合併時每格取最大值，
//!    總數是各格相加。合併的順序和次數都不影響結果，所以可以自動合併。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// 節點 -> 這個節點寫過幾次
pub type VectorClock = BTreeMap<String, u64>;

/// 節點 -> 這個節點累加的量
pub type GCounter = BTreeMap<String, u64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// a 發生在 b 之前：b 已經看過 a
    Before,
    /// b 發生在 a 之前
    After,
    Equal,
    /// 互相沒看過對方，誰都不該蓋掉誰
    Concurrent,
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Before => "之前",
            Self::After => "之後",
            Self::Equal => "相同",
            Self::Concurrent => "並行",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sibling {
    pub value: String,
    pub clock: VectorClock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeError {
    /// G-Counter 只能增加，累加 0 沒有意義（u64 不會是負數）
    ZeroAmount,
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAmount => f.write_str("G-Counter 只能加正數，收到 0"),
        }
    }
}

impl std::error::Error for MergeError {}

pub fn compare(a: &VectorClock, b: &VectorClock) -> Order {
    let nodes: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    let count = |clock: &VectorClock, node: &String| clock.get(node).copied().unwrap_or(0);
    let a_ahead = nodes.iter().any(|node| count(a, node) > count(b, node));
    let b_ahead = nodes.iter().any(|node| count(b, node) > count(a, node));
    match (a_ahead, b_ahead) {
        (true, true) => Order::Concurrent,
        (true, false) => Order::After,
        (false, true) => Order::Before,
        (false, false) => Order::Equal,
    }
}

/// 在 node 寫入 value。新時鐘涵蓋目前所有兄弟版本，所以會取代它們。
pub fn next_version(siblings: &[Sibling], node: &str, value: &str) -> Sibling {
    let mut clock = max_per_node(siblings.iter().map(|sibling| &sibling.clock));
    let count = clock.get(node).copied().unwrap_or(0) + 1;
    clock.insert(node.to_owned(), count);
    Sibling {
        value: value.to_owned(),
        clock,
    }
}

/// 合併各節點手上的版本：已經被別的版本看過的丟掉，並行的全部保留。
pub fn reconcile(versions: &[Sibling]) -> Vec<Sibling> {
    let distinct: Vec<&Sibling> = versions
        .iter()
        .enumerate()
        .filter(|&(i, version)| !versions[..i].contains(version))
        .map(|(_, version)| version)
        .collect();
    distinct
        .iter()
        .filter(|version| {
            !distinct
                .iter()
                .any(|other| compare(&version.clock, &other.clock) == Order::Before)
        })
        .map(|&version| version.clone())
        .collect()
}

/// 在 node 那一格累加。G-Counter 只能增加；要能減少，就用兩個組成 PN-Counter。
pub fn add(counter: &GCounter, node: &str, amount: u64) -> Result<GCounter, MergeError> {
    if amount == 0 {
        return Err(MergeError::ZeroAmount);
    }
    let mut updated = counter.clone();
    *updated.entry(node.to_owned()).or_insert(0) += amount;
    Ok(updated)
}

/// 每一格取最大值。
pub fn merge_counters(counters: &[&GCounter]) -> GCounter {
    max_per_node(counters.iter().copied())
}

pub fn total(counter: &GCounter) -> u64 {
    counter.values().sum()
}

/// 回傳新的表，每個節點取各表裡的最大值；傳進來的表都不變
fn max_per_node<'a>(
    tables: impl IntoIterator<Item = &'a BTreeMap<String, u64>>,
) -> BTreeMap<String, u64> {
    let mut merged = BTreeMap::new();
    for (node, count) in tables.into_iter().flatten() {
        let current: &mut u64 = merged.entry(node.clone()).or_insert(0);
        *current = (*current).max(*count);
    }
    merged
}
