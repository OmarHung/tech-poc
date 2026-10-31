//! The replication API between the two nodes.

use serde::{Deserialize, Serialize};

use crate::storage::LogEntry;

/// Primary to standby: "append these after (prev_index, prev_epoch)". An empty batch is the heartbeat.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReplicateRequest {
    pub leader: Option<String>,
    pub epoch: i64,
    pub prev_index: i64,
    pub prev_epoch: i64,
    pub entries: Option<Vec<LogEntry>>,
    pub leader_last_index: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicateStatus {
    /// Applied. The standby now matches the primary up to `match_index`.
    Ok,
    /// The standby lacks or disagrees with the entry at `prev_index`; retry from `match_index + 1`.
    Mismatch,
    /// Not accepted from this sender at all (older epoch, or the standby is asking the witness for the lease).
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicateReply {
    pub status: ReplicateStatus,
    /// The standby's epoch after handling the request. Higher than the sender's means the sender was deposed.
    pub epoch: i64,
    pub match_index: i64,
    /// How long from receiving the request the standby will not seek the lease (0 when rejected).
    pub promise_ms: i64,
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format_matches_the_other_implementations() {
        let reply = ReplicateReply {
            status: ReplicateStatus::Mismatch,
            epoch: 2,
            match_index: 7,
            promise_ms: 3000,
            reason: None,
        };
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"status":"mismatch","epoch":2,"matchIndex":7,"promiseMs":3000,"reason":null}"#
        );

        let request: ReplicateRequest = serde_json::from_str(
            r#"{"leader":"a","epoch":2,"prevIndex":1,"prevEpoch":1,"entries":[{"index":2,"epoch":2,"key":"k","value":"v"}],"leaderLastIndex":2}"#,
        )
        .unwrap();
        assert_eq!(request.entries.unwrap()[0].key, "k");
        assert_eq!(
            serde_json::from_str::<ReplicateRequest>("{}").unwrap(),
            ReplicateRequest::default()
        );
    }
}
