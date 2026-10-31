//! The node's data: an append-only log of writes and the file behind it.

pub mod log_file;
pub mod replicated_log;

use serde::{Deserialize, Serialize};

/// One write. `index` orders writes; `epoch` says which primary term created it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub index: i64,
    pub epoch: i64,
    pub key: String,
    pub value: String,
}

/// A consistent read of the log: the entries asked for, plus what surrounds them at that moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSlice {
    /// Epoch of the entry just before `entries` (0 when they start at index 1).
    pub prev_epoch: i64,
    pub entries: Vec<LogEntry>,
    pub last_index: i64,
}
