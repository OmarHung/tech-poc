//! Node ids and the witness API. Nodes call the witness; nothing else should.

use serde::{Deserialize, Serialize};

const MAX_NODE_ID_LENGTH: usize = 32;

/// Node ids appear in URLs, file names and logs, so they are kept to a safe alphabet.
pub fn is_valid_node_id(id: &str) -> bool {
    (1..=MAX_NODE_ID_LENGTH).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LeaseRequest {
    pub node: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LeaseReply {
    /// Whether the caller now holds the lease for `ttl_ms` from when it sent the request.
    pub granted: bool,
    pub holder: Option<String>,
    /// Goes up by one on every handover and doubles as the fencing token.
    pub epoch: i64,
    pub ttl_ms: i32,
    /// Whether the node that does not hold the lease may take over. Reset to false on every handover.
    pub standby_in_sync: bool,
    /// Goes up on every accepted sync report and every handover.
    pub sync_version: i64,
    /// Why the lease was refused.
    pub reason: Option<String>,
}

/// Sent by the lease holder to say whether the other node has every write it acknowledged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SyncRequest {
    pub node: Option<String>,
    pub epoch: i64,
    pub in_sync: bool,
    /// For `in_sync = true` only: the latest sync version the holder has seen. If anything changed since,
    /// the claim may be out of date (for example a request that arrived late) and is refused.
    pub expected_version: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SyncReply {
    pub epoch: i64,
    pub standby_in_sync: bool,
    pub sync_version: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_use_a_safe_alphabet() {
        for good in ["a", "node-2", &"x".repeat(32)] {
            assert!(is_valid_node_id(good), "{good}");
        }
        for bad in ["", "A", "a b", "a_b", "é", &"x".repeat(33)] {
            assert!(!is_valid_node_id(bad), "{bad}");
        }
    }

    #[test]
    fn contracts_use_camel_case() {
        let reply = LeaseReply {
            granted: true,
            holder: Some("a".into()),
            epoch: 2,
            ttl_ms: 3000,
            standby_in_sync: false,
            sync_version: 5,
            reason: None,
        };
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"granted":true,"holder":"a","epoch":2,"ttlMs":3000,"standbyInSync":false,"syncVersion":5,"reason":null}"#
        );
        let request: SyncRequest =
            serde_json::from_str(r#"{"node":"a","epoch":1,"inSync":true}"#).unwrap();
        assert_eq!(request.expected_version, 0);
        assert!(serde_json::from_str::<SyncRequest>(r#"{"epoch":"1"}"#).is_err());
    }
}
