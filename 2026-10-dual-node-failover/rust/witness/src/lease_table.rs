use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shared::clock::MonotonicClock;
use shared::error::ApiError;
use shared::state_file::{self, StateFileError};
use shared::wire::{LeaseReply, SyncReply, SyncRequest};
use tracing::{info, warn};

use crate::config::WitnessConfig;

const STATE_FILE_NAME: &str = "witness.json";

/// What the witness keeps on disk: who holds the lease, in which epoch, and whether the other node may take over.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WitnessState {
    pub epoch: i64,
    pub holder: Option<String>,
    pub standby_in_sync: bool,
    /// Bumped on every accepted sync report and every handover, so a stale "in sync" can be told apart.
    pub sync_version: i64,
}

/// What `GET /health` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WitnessHealth {
    pub status: String,
    pub service: String,
    pub holder: Option<String>,
    pub epoch: i64,
    pub standby_in_sync: bool,
    pub sync_version: i64,
    pub lease_remaining_ms: i64,
}

/// The tie-breaker between the two nodes. It hands out one lease at a time and bumps the epoch on every
/// handover, so a deposed primary can be recognised by its old epoch (the fencing token).
///
/// It also remembers whether the standby has every acknowledged write, and refuses to promote it if
/// not: a stale standby taking over would silently drop writes the clients were told had succeeded.
pub struct LeaseTable {
    config: WitnessConfig,
    clock: MonotonicClock,
    state_path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    state: WitnessState,
    expires_at: Duration,
}

impl LeaseTable {
    pub fn open(config: WitnessConfig) -> Result<Self, StateFileError> {
        let state_path = config.data_dir.join(STATE_FILE_NAME);
        let state = state_file::load::<WitnessState>(&state_path)?.unwrap_or_default();
        let clock = MonotonicClock::new();
        // After a restart the old holder may still be inside its lease, so nobody else gets it for one more TTL.
        let expires_at = if state.holder.is_some() {
            clock.now() + config.lease_ttl
        } else {
            Duration::ZERO
        };
        Ok(Self {
            config,
            clock,
            state_path,
            inner: Mutex::new(Inner { state, expires_at }),
        })
    }

    pub fn config(&self) -> &WitnessConfig {
        &self.config
    }

    pub fn state(&self) -> WitnessState {
        self.lock().state.clone()
    }

    /// Grants, renews or refuses the lease. Renewing and acquiring are the same call.
    pub fn acquire(&self, node: Option<&str>) -> Result<LeaseReply, ApiError> {
        let node = self.require_known(node)?;
        let mut inner = self.lock();
        let now = self.clock.now();
        if inner.state.holder.as_deref() == Some(node) {
            // Also after expiry: nobody else took over in between, so the holder still has the newest data.
            inner.expires_at = now + self.config.lease_ttl;
            return Ok(self.reply(&inner.state, None));
        }
        if let Some(holder) = inner.state.holder.as_deref() {
            if now < inner.expires_at {
                return Ok(self.reply(&inner.state, Some(format!("lease held by {holder}"))));
            }
            if !inner.state.standby_in_sync {
                let reason = format!(
                    "{node} may be missing acknowledged writes; waiting for {holder} to return"
                );
                return Ok(self.reply(&inner.state, Some(reason)));
            }
        }
        let previous = inner
            .state
            .holder
            .clone()
            .unwrap_or_else(|| "nobody".into());
        // The node that just lost the lease is behind until the new holder says otherwise.
        let next = WitnessState {
            epoch: inner.state.epoch + 1,
            holder: Some(node.to_owned()),
            standby_in_sync: false,
            sync_version: inner.state.sync_version + 1,
        };
        self.save(&next)?;
        inner.state = next;
        inner.expires_at = now + self.config.lease_ttl;
        warn!(
            "Lease handed from {previous} to {node}, epoch {}",
            inner.state.epoch
        );
        Ok(self.reply(&inner.state, None))
    }

    /// Records whether the standby may be promoted. Only the holder, in its own epoch, may say so.
    /// "Not in sync" is always accepted; "in sync" only if nothing changed since the holder last looked,
    /// because an HTTP request the holder gave up on can still arrive later and must not undo a newer
    /// "not in sync".
    pub fn set_standby_in_sync(&self, request: &SyncRequest) -> Result<SyncReply, ApiError> {
        let node = self.require_known(request.node.as_deref())?;
        let mut inner = self.lock();
        if inner.state.holder.as_deref() != Some(node) || inner.state.epoch != request.epoch {
            return Err(ApiError::Conflict(format!(
                "{node} is not the lease holder in epoch {} (holder {}, epoch {})",
                request.epoch,
                inner.state.holder.as_deref().unwrap_or("none"),
                inner.state.epoch
            )));
        }
        if request.in_sync && request.expected_version != inner.state.sync_version {
            return Err(ApiError::Conflict(format!(
                "standby state changed since version {} (now {}); check again",
                request.expected_version, inner.state.sync_version
            )));
        }
        let changed = inner.state.standby_in_sync != request.in_sync;
        let next = WitnessState {
            standby_in_sync: request.in_sync,
            sync_version: inner.state.sync_version + 1,
            ..inner.state.clone()
        };
        self.save(&next)?;
        inner.state = next;
        if changed {
            info!(
                "Epoch {}: standby in sync = {}",
                inner.state.epoch, request.in_sync
            );
        }
        Ok(SyncReply {
            epoch: inner.state.epoch,
            standby_in_sync: inner.state.standby_in_sync,
            sync_version: inner.state.sync_version,
        })
    }

    pub fn health(&self) -> WitnessHealth {
        let inner = self.lock();
        let remaining = match inner.state.holder {
            Some(_) => inner.expires_at.saturating_sub(self.clock.now()),
            None => Duration::ZERO,
        };
        WitnessHealth {
            status: "ok".into(),
            service: "witness".into(),
            holder: inner.state.holder.clone(),
            epoch: inner.state.epoch,
            standby_in_sync: inner.state.standby_in_sync,
            sync_version: inner.state.sync_version,
            lease_remaining_ms: i64::try_from(remaining.as_millis()).unwrap_or(i64::MAX),
        }
    }

    fn reply(&self, state: &WitnessState, reason: Option<String>) -> LeaseReply {
        LeaseReply {
            granted: reason.is_none(),
            holder: state.holder.clone(),
            epoch: state.epoch,
            ttl_ms: i32::try_from(self.config.lease_ttl.as_millis()).unwrap_or(i32::MAX),
            standby_in_sync: state.standby_in_sync,
            sync_version: state.sync_version,
            reason,
        }
    }

    /// Persists before the change takes effect, so a restart never forgets a lease it handed out.
    fn save(&self, state: &WitnessState) -> Result<(), ApiError> {
        state_file::save(&self.state_path, state).map_err(|error| {
            ApiError::Internal(format!("could not save the witness state: {error}"))
        })
    }

    fn require_known<'a>(&self, node: Option<&'a str>) -> Result<&'a str, ApiError> {
        match node {
            Some(node) if self.config.nodes.iter().any(|known| known == node) => Ok(node),
            _ => Err(ApiError::InvalidInput(format!(
                "node must be one of {}, got '{}'",
                self.config.nodes.join(", "),
                node.unwrap_or("")
            ))),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Nothing panics while holding this lock; the state on disk is written before memory changes.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::time::advance;

    use super::*;

    const TTL: Duration = Duration::from_secs(3);

    fn table(dir: &TempDir) -> LeaseTable {
        LeaseTable::open(WitnessConfig {
            data_dir: dir.path().to_owned(),
            lease_ttl: TTL,
            ..WitnessConfig::default()
        })
        .unwrap()
    }

    fn sync(node: &str, epoch: i64, in_sync: bool, expected_version: i64) -> SyncRequest {
        SyncRequest {
            node: Some(node.into()),
            epoch,
            in_sync,
            expected_version,
        }
    }

    fn mark_in_sync(table: &LeaseTable, node: &str, epoch: i64) {
        table
            .set_standby_in_sync(&sync(node, epoch, true, table.state().sync_version))
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn first_request_gets_epoch_one_and_the_other_node_is_not_yet_eligible() {
        let dir = tempfile::tempdir().unwrap();

        let reply = table(&dir).acquire(Some("a")).unwrap();

        assert!(reply.granted);
        assert_eq!(reply.holder.as_deref(), Some("a"));
        assert_eq!(
            (reply.epoch, reply.ttl_ms, reply.sync_version),
            (1, 3000, 1)
        );
        assert!(!reply.standby_in_sync);
        assert_eq!(reply.reason, None);
    }

    #[tokio::test(start_paused = true)]
    async fn holder_renews_in_the_same_epoch_even_after_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        table.acquire(Some("a")).unwrap();

        advance(TTL * 2).await;
        let reply = table.acquire(Some("a")).unwrap();

        assert!(reply.granted);
        assert_eq!(reply.epoch, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn other_node_is_refused_while_the_lease_is_valid() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        table.acquire(Some("a")).unwrap();
        mark_in_sync(&table, "a", 1);

        advance(TTL - Duration::from_millis(1)).await;
        let reply = table.acquire(Some("b")).unwrap();

        assert!(!reply.granted);
        assert_eq!(reply.holder.as_deref(), Some("a"));
        assert!(reply.reason.unwrap().contains("held by a"));
    }

    #[tokio::test(start_paused = true)]
    async fn in_sync_standby_takes_over_after_expiry_with_a_new_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        table.acquire(Some("a")).unwrap();
        mark_in_sync(&table, "a", 1);

        advance(TTL).await;
        let reply = table.acquire(Some("b")).unwrap();

        assert!(reply.granted);
        assert_eq!(reply.holder.as_deref(), Some("b"));
        assert_eq!(reply.epoch, 2);
        assert!(!reply.standby_in_sync); // a is behind until b says otherwise
    }

    #[tokio::test(start_paused = true)]
    async fn stale_standby_is_never_promoted() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        table.acquire(Some("a")).unwrap(); // standby starts out not in sync

        advance(TTL * 10).await;
        let reply = table.acquire(Some("b")).unwrap();

        assert!(!reply.granted);
        assert_eq!(reply.holder.as_deref(), Some("a"));
        assert!(
            reply
                .reason
                .unwrap()
                .contains("missing acknowledged writes")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deposed_holder_is_refused_and_cannot_change_the_sync_flag() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        table.acquire(Some("a")).unwrap();
        mark_in_sync(&table, "a", 1);
        advance(TTL).await;
        table.acquire(Some("b")).unwrap();

        assert!(!table.acquire(Some("a")).unwrap().granted);
        let wrong_holder = table
            .set_standby_in_sync(&sync("a", 1, false, 0))
            .unwrap_err();
        let wrong_epoch = table
            .set_standby_in_sync(&sync("b", 1, true, table.state().sync_version))
            .unwrap_err();
        assert!(
            matches!(wrong_holder, ApiError::Conflict(_)),
            "{wrong_holder}"
        );
        assert!(
            matches!(wrong_epoch, ApiError::Conflict(_)),
            "{wrong_epoch}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn state_survives_a_restart_and_the_holder_keeps_the_lease_for_one_more_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let before = table(&dir);
        before.acquire(Some("a")).unwrap();
        mark_in_sync(&before, "a", 1);
        advance(TTL * 5).await;

        let after = table(&dir);

        assert_eq!(
            after.state(),
            WitnessState {
                epoch: 1,
                holder: Some("a".into()),
                standby_in_sync: true,
                sync_version: 2
            }
        );
        assert!(!after.acquire(Some("b")).unwrap().granted);
        advance(TTL).await;
        assert!(after.acquire(Some("b")).unwrap().granted);
    }

    #[tokio::test(start_paused = true)]
    async fn in_sync_report_is_refused_if_the_state_changed_since_the_holder_looked() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        let seen = table.acquire(Some("a")).unwrap().sync_version;
        table.set_standby_in_sync(&sync("a", 1, false, 0)).unwrap(); // e.g. sent after the report below, arrived first

        let error = table
            .set_standby_in_sync(&sync("a", 1, true, seen))
            .unwrap_err();

        assert!(matches!(error, ApiError::Conflict(_)), "{error}");
        assert!(!table.state().standby_in_sync);
    }

    #[tokio::test(start_paused = true)]
    async fn not_in_sync_reports_are_always_accepted_and_every_report_bumps_the_version() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        let granted = table.acquire(Some("a")).unwrap().sync_version;

        let first = table
            .set_standby_in_sync(&sync("a", 1, false, 999))
            .unwrap();
        let second = table
            .set_standby_in_sync(&sync("a", 1, true, first.sync_version))
            .unwrap();

        assert_eq!(first.sync_version, granted + 1);
        assert_eq!(
            second,
            SyncReply {
                epoch: 1,
                standby_in_sync: true,
                sync_version: granted + 2
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn handover_bumps_the_version_so_old_reports_cannot_apply() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        table.acquire(Some("a")).unwrap();
        mark_in_sync(&table, "a", 1);
        let before = table.state().sync_version;
        advance(TTL).await;

        let reply = table.acquire(Some("b")).unwrap();

        assert_eq!(reply.sync_version, before + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_nodes_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);

        for node in [Some("c"), Some(""), None] {
            let error = table.acquire(node).unwrap_err();
            assert!(matches!(error, ApiError::InvalidInput(_)), "{error}");
            let request = SyncRequest {
                node: node.map(str::to_owned),
                epoch: 0,
                in_sync: true,
                expected_version: 0,
            };
            assert!(matches!(
                table.set_standby_in_sync(&request),
                Err(ApiError::InvalidInput(_))
            ));
        }
        assert_eq!(
            table.acquire(None).unwrap_err().message(),
            "node must be one of a, b, got ''"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn health_reports_the_lease() {
        let dir = tempfile::tempdir().unwrap();
        let table = table(&dir);
        assert_eq!(table.health().lease_remaining_ms, 0);
        table.acquire(Some("a")).unwrap();

        advance(Duration::from_secs(1)).await;
        let health = table.health();

        assert_eq!(
            (health.holder.as_deref(), health.lease_remaining_ms),
            (Some("a"), 2000)
        );
        advance(TTL).await;
        assert_eq!(table.health().lease_remaining_ms, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_save_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("w");
        let table = LeaseTable::open(WitnessConfig {
            data_dir: data_dir.clone(),
            ..WitnessConfig::default()
        })
        .unwrap();
        std::fs::write(&data_dir, "a file where the directory should be").unwrap();

        let error = table.acquire(Some("a")).unwrap_err();

        assert!(matches!(error, ApiError::Internal(_)), "{error}");
        assert_eq!(table.state(), WitnessState::default());
    }
}
