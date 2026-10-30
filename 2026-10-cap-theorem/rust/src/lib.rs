//! CAP 定理的最小模擬：同一組節點切換 CP / AP 兩種模式，看網路分區時的行為差異。
//!
//! 網路分區用「節點分組」表示：同一組內可以互相連線，跨組連不到。
//! 寫入會立刻複製到同一組的所有節點，另一組收不到。
//! 整個模擬跑在單一程序內，沒有真的網路，所以每次結果都一樣。
//! 叢集是不可變的：每個操作都回傳新的叢集，原本的不會變。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// 少數派那側拒絕服務，換取一致性
    Cp,
    /// 每個節點都照常服務，換取可用性
    Ap,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cp => "CP",
            Self::Ap => "AP",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub value: String,
    /// 全域邏輯時鐘；分區恢復時用它決定 last-write-wins 誰贏
    pub timestamp: u64,
    /// 接受這筆寫入的節點
    pub origin: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapError {
    /// 連得到的節點沒過半（只有 CP 模式會發生）
    NoQuorum {
        reachable: usize,
        quorum: usize,
    },
    UnknownNode(String),
    /// 分組沒有剛好涵蓋每個節點一次
    InvalidPartition,
}

impl fmt::Display for CapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoQuorum { reachable, quorum } => {
                write!(f, "只連得到 {reachable} 台，需要 {quorum} 台才過半")
            }
            Self::UnknownNode(node) => write!(f, "不存在的節點：{node}"),
            Self::InvalidPartition => f.write_str("分組必須剛好涵蓋每個節點一次"),
        }
    }
}

impl std::error::Error for CapError {}

/// 一台節點上的資料：key -> 版本
type Store = BTreeMap<String, Version>;

#[derive(Debug, Clone)]
pub struct Cluster {
    mode: Mode,
    nodes: Vec<String>,
    stores: BTreeMap<String, Store>,
    /// 目前連得到彼此的節點分組
    groups: Vec<BTreeSet<String>>,
    clock: u64,
    /// 分區發生時的時鐘；沒有分區時為 None
    partitioned_at: Option<u64>,
}

impl Cluster {
    pub fn new(mode: Mode, nodes: &[&str]) -> Self {
        let nodes: Vec<String> = nodes.iter().map(|node| (*node).to_owned()).collect();
        Self {
            mode,
            stores: nodes
                .iter()
                .map(|node| (node.clone(), Store::new()))
                .collect(),
            groups: vec![nodes.iter().cloned().collect()],
            nodes,
            clock: 0,
            partitioned_at: None,
        }
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub fn quorum(&self) -> usize {
        self.nodes.len() / 2 + 1
    }

    /// 把網路切成幾組。每個節點必須剛好出現在一組裡。
    pub fn partition(&self, groups: &[&[&str]]) -> Result<Self, CapError> {
        let members = sorted(groups.iter().flat_map(|group| group.iter().copied()));
        if members != sorted(self.nodes.iter().map(String::as_str)) {
            return Err(CapError::InvalidPartition);
        }
        Ok(Self {
            groups: groups
                .iter()
                .map(|group| group.iter().map(|node| (*node).to_owned()).collect())
                .collect(),
            partitioned_at: Some(self.clock),
            ..self.clone()
        })
    }

    pub fn write(&self, node: &str, key: &str, value: &str) -> Result<Self, CapError> {
        let peers = self.reachable(node)?;
        self.require_quorum(peers)?;

        let clock = self.clock + 1;
        let version = Version {
            value: value.to_owned(),
            timestamp: clock,
            origin: node.to_owned(),
        };
        let stores = self
            .stores
            .iter()
            .map(|(name, store)| {
                let store = if peers.contains(name) {
                    with_entry(store, key, &version)
                } else {
                    store.clone()
                };
                (name.clone(), store)
            })
            .collect();
        Ok(Self {
            stores,
            clock,
            ..self.clone()
        })
    }

    pub fn read(&self, node: &str, key: &str) -> Result<Option<&str>, CapError> {
        self.require_quorum(self.reachable(node)?)?;

        // 同一組的節點資料永遠一致，所以讀本地就等於讀到這一組的最新值。
        // AP 少數派讀到的「最新」其實是分區前的舊資料。
        Ok(self.stores[node]
            .get(key)
            .map(|version| version.value.as_str()))
    }

    /// 恢復連線，用 last-write-wins 合併各節點的資料。
    ///
    /// 回傳合併後的叢集，以及「分區期間被確認過、合併時卻被丟掉」的寫入。
    pub fn heal(&self) -> (Self, Vec<Version>) {
        let keys: BTreeSet<&String> = self.stores.values().flat_map(Store::keys).collect();
        // 每個 key 在各節點上的不同版本，依時間由舊到新排好；最後一個就是 LWW 的贏家
        let candidates: Vec<(&String, Vec<&Version>)> = keys
            .into_iter()
            .map(|key| (key, versions_by_time(self.stores.values(), key)))
            .collect();
        let merged: Store = candidates
            .iter()
            .filter_map(|(key, versions)| {
                versions
                    .last()
                    .map(|winner| ((*key).clone(), (*winner).clone()))
            })
            .collect();

        // 分區前就存在的舊版本輸掉只是「落後」，分區期間寫入的版本輸掉才是「遺失」。
        let since = self.partitioned_at.unwrap_or(self.clock);
        let lost = candidates
            .iter()
            .flat_map(|(_, versions)| &versions[..versions.len().saturating_sub(1)])
            .filter(|version| version.timestamp > since)
            .map(|version| (*version).clone())
            .collect();

        let healed = Self {
            stores: self
                .nodes
                .iter()
                .map(|node| (node.clone(), merged.clone()))
                .collect(),
            groups: vec![self.nodes.iter().cloned().collect()],
            partitioned_at: None,
            ..self.clone()
        };
        (healed, lost)
    }

    fn reachable(&self, node: &str) -> Result<&BTreeSet<String>, CapError> {
        self.groups
            .iter()
            .find(|group| group.contains(node))
            .ok_or_else(|| CapError::UnknownNode(node.to_owned()))
    }

    fn require_quorum(&self, peers: &BTreeSet<String>) -> Result<(), CapError> {
        match self.mode {
            Mode::Ap => Ok(()),
            Mode::Cp if peers.len() >= self.quorum() => Ok(()),
            Mode::Cp => Err(CapError::NoQuorum {
                reachable: peers.len(),
                quorum: self.quorum(),
            }),
        }
    }
}

fn sorted<'a>(names: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let mut names: Vec<&str> = names.collect();
    names.sort_unstable();
    names
}

/// 回傳多了這一筆的新 Store，原本的不變
fn with_entry(store: &Store, key: &str, version: &Version) -> Store {
    let mut updated = store.clone();
    updated.insert(key.to_owned(), version.clone());
    updated
}

fn versions_by_time<'a>(stores: impl Iterator<Item = &'a Store>, key: &str) -> Vec<&'a Version> {
    let mut versions: Vec<&Version> = stores.filter_map(|store| store.get(key)).collect();
    versions.sort_by_key(|version| version.timestamp);
    versions.dedup();
    versions
}
