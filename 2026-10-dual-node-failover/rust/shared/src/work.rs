//! Wire format of the two architectures where the workers never talk to each other: an upstream that
//! feeds them (an event log for hot-hot redundancy, a task queue for dispatched workers) and a sink that
//! receives their results.
//!
//! Requests default every missing field (like the .NET records), so validation decides what is wrong and
//! says so; replies are strict, so a garbled one reads as "no answer".

use serde::{Deserialize, Serialize};

pub const MAX_WORK_ID_LENGTH: usize = 64;

/// Accounts and result keys: 1-64 ASCII letters, digits, '-', '_' or '.'.
pub fn is_valid_work_id(value: &str) -> bool {
    (1..=MAX_WORK_ID_LENGTH).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// An event appended to the upstream log. Both hot-hot workers read every event.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NewEvent {
    pub account: Option<String>,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoggedEvent {
    pub seq: i64,
    pub account: String,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventPage {
    pub events: Vec<LoggedEvent>,
    /// The newest sequence number in the log, so a reader knows how far behind it is.
    pub last: i64,
}

/// A task for the upstream queue. Exactly one worker holds it at a time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NewTask {
    pub input: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewTaskReply {
    pub id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TaskLeaseRequest {
    pub worker: Option<String>,
    pub wait_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskLease {
    pub id: i64,
    pub input: i64,
    /// How many times this task has been handed out, this time included. Doubles as a fencing token.
    pub attempt: i32,
    /// The lease runs out this long after the request was sent, unless a heartbeat extends it.
    pub ttl_ms: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TaskAttempt {
    pub worker: Option<String>,
    pub attempt: i32,
}

/// One result delivered to the sink. `key` identifies the event or task it is for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResultRecord {
    pub key: Option<String>,
    pub node: Option<String>,
    pub attempt: i32,
    pub value: Option<String>,
}

/// The sink's verdict on one result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultReply {
    /// "applied" (first copy), "duplicate" (same value again), "conflict" (a different value),
    /// "stale" (an older attempt than one already seen).
    pub status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_ids_use_a_safe_alphabet() {
        for good in ["acct-1", "event_2", "task.3", "A", &"x".repeat(64)] {
            assert!(is_valid_work_id(good), "{good}");
        }
        for bad in ["", "a b", "a/b", "é", &"x".repeat(65)] {
            assert!(!is_valid_work_id(bad), "{bad}");
        }
    }

    #[test]
    fn requests_default_missing_fields_and_stay_strict_about_types() {
        let lease: TaskLeaseRequest = serde_json::from_str(r#"{"worker":"a"}"#).unwrap();
        let record: ResultRecord = serde_json::from_str("{}").unwrap();

        assert_eq!((lease.worker.as_deref(), lease.wait_ms), (Some("a"), 0));
        assert_eq!(record, ResultRecord::default());
        assert!(serde_json::from_str::<NewEvent>(r#"{"account":"a","amount":"5"}"#).is_err());
        assert!(serde_json::from_str::<NewTask>(r#"{"input":1.5}"#).is_err());
        assert!(serde_json::from_str::<TaskAttempt>(r#"{"attempt":2147483648}"#).is_err());
    }

    #[test]
    fn contracts_use_camel_case() {
        let lease = TaskLease {
            id: 1,
            input: 3,
            attempt: 2,
            ttl_ms: 2000,
        };
        let page = EventPage {
            events: vec![LoggedEvent {
                seq: 1,
                account: "acct".into(),
                amount: -5,
            }],
            last: 1,
        };

        assert_eq!(
            serde_json::to_string(&lease).unwrap(),
            r#"{"id":1,"input":3,"attempt":2,"ttlMs":2000}"#
        );
        assert_eq!(
            serde_json::to_string(&page).unwrap(),
            r#"{"events":[{"seq":1,"account":"acct","amount":-5}],"last":1}"#
        );
        assert!(serde_json::from_str::<ResultReply>(r#"{"error":"bad"}"#).is_err());
    }
}
