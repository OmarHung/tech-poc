//! The client-facing store. Only the primary serves, and in sync mode only what is safe to acknowledge.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use shared::error::ApiError;

use crate::cluster::node_state::{NodeState, not_primary};
use crate::cluster::primary_term::PrimaryTerm;
use crate::config::ReplicationMode;
use crate::storage::LogEntry;

pub const MAX_KEY_LENGTH: usize = 128;
pub const MAX_VALUE_LENGTH: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KvItem {
    pub key: String,
    pub value: String,
    pub index: i64,
    pub epoch: i64,
}

impl From<LogEntry> for KvItem {
    fn from(entry: LogEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            index: entry.index,
            epoch: entry.epoch,
        }
    }
}

/// Body of `PUT /kv/{key}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PutBody {
    pub value: Option<String>,
}

pub struct KvService {
    state: Arc<NodeState>,
}

impl KvService {
    pub fn new(state: Arc<NodeState>) -> Self {
        Self { state }
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<KvItem, ApiError> {
        let (entry, term) = self.state.append_as_primary(key, value)?;
        self.wait_durable(&term, &entry).await?;
        Ok(entry.into())
    }

    pub async fn get(&self, key: &str) -> Result<KvItem, ApiError> {
        let term = self.state.serving_term().ok_or_else(not_primary)?;
        let entry = self
            .state
            .log()
            .get(key)
            .ok_or_else(|| ApiError::NotFound(format!("key '{key}' not found")))?;
        // Never show a value that could still vanish in a failover.
        self.wait_durable(&term, &entry).await?;
        Ok(entry.into())
    }

    async fn wait_durable(
        &self,
        term: &Arc<PrimaryTerm>,
        entry: &LogEntry,
    ) -> Result<(), ApiError> {
        let config = self.state.config();
        match config.replication {
            ReplicationMode::Async => Ok(()),
            ReplicationMode::Sync => {
                term.wait_durable(entry.index, config.replication_timeout, self.state.clock())
                    .await
            }
        }
    }
}

pub fn validate_key(key: &str) -> Result<(), ApiError> {
    let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':');
    if key.is_empty() || key.len() > MAX_KEY_LENGTH || !key.chars().all(allowed) {
        return Err(ApiError::InvalidInput(format!(
            "key must be 1-{MAX_KEY_LENGTH} ASCII letters, digits, '-', '_', '.' or ':'"
        )));
    }
    Ok(())
}

/// Length counted in UTF-16 code units, the way the .NET version counts characters.
pub fn validate_value(value: Option<String>) -> Result<String, ApiError> {
    match value {
        Some(value) if value.encode_utf16().count() <= MAX_VALUE_LENGTH => Ok(value),
        _ => Err(ApiError::InvalidInput(format!(
            "value must be a string of at most {MAX_VALUE_LENGTH} characters"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_short_and_ascii() {
        for good in ["order-1", "a.b:c_d", &"k".repeat(128)] {
            assert!(validate_key(good).is_ok(), "{good}");
        }
        for bad in ["", "bad key", "é", "a/b", &"k".repeat(129)] {
            assert!(validate_key(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn values_are_required_and_bounded() {
        assert_eq!(
            validate_value(Some("x".repeat(4096))).map(|v| v.len()),
            Ok(4096)
        );
        assert!(validate_value(Some("x".repeat(4097))).is_err());
        assert!(
            validate_value(Some("😀".repeat(2049))).is_err(),
            "two UTF-16 units each"
        );
        assert!(validate_value(None).is_err());
    }
}
