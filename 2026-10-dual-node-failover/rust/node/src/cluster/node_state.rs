use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shared::clock::MonotonicClock;
use shared::error::ApiError;
use shared::state_file::{self, StateFileError};
use tokio::sync::watch;
use tracing::{error, info, warn};

use super::MAX_BATCH;
use super::contracts::{ReplicateReply, ReplicateRequest, ReplicateStatus};
use super::primary_term::{Arbiter, PrimaryTerm};
use crate::config::{ArbiterMode, NodeConfig};
use crate::storage::LogEntry;
use crate::storage::replicated_log::{AppendError, AppendStatus, ReplicatedLog};

const EPOCH_FILE_NAME: &str = "epoch.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochRecord {
    pub epoch: i64,
}

/// What `GET /health` returns. The gateway routes to the node that reports `role = primary`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeHealth {
    pub status: String,
    pub node: String,
    pub role: String,
    pub epoch: i64,
    pub last_index: i64,
    pub match_index: Option<i64>,
    pub peer: Option<String>,
    pub lease_remaining_ms: Option<i64>,
    pub truncated_entries: i64,
    pub arbiter: String,
    pub replication: String,
}

/// Role, epoch and leases of this node, and the only way to write to its log as primary. One lock
/// covers all of them, so a node can never append as primary after it has stepped down.
///
/// A primary may serve while either lease is valid: the witness lease, or the "peer lease" the standby
/// grants by accepting replication (it promises not to seek the lease for one TTL after hearing from
/// the primary). With two of three votes either way, losing the witness alone or the standby alone
/// does not stop the primary.
pub struct NodeState {
    config: NodeConfig,
    arbiter: Arc<dyn Arbiter>,
    log: Arc<ReplicatedLog>,
    clock: MonotonicClock,
    epoch_path: PathBuf,
    inner: Mutex<StateInner>,
    /// Mirrors `inner.term` so the replicator can wait for the next term.
    terms: watch::Sender<Option<Arc<PrimaryTerm>>>,
}

struct StateInner {
    epoch: i64,
    term: Option<Arc<PrimaryTerm>>,
    witness_lease_until: Duration,
    peer_lease_until: Duration,
    last_heard_from_primary: Duration,
    candidate: bool,
}

impl StateInner {
    fn lease_until(&self) -> Duration {
        self.witness_lease_until.max(self.peer_lease_until)
    }
}

impl NodeState {
    pub fn new(
        config: NodeConfig,
        log: Arc<ReplicatedLog>,
        arbiter: Arc<dyn Arbiter>,
    ) -> Result<Self, StateFileError> {
        let epoch_path = config.data_dir.join(EPOCH_FILE_NAME);
        let saved = state_file::load::<EpochRecord>(&epoch_path)?.map_or(0, |record| record.epoch);
        let epoch = saved.max(log.last_epoch());
        let clock = MonotonicClock::new();
        // Count startup as having just heard from a primary: a node that restarts must not seek the lease
        // while the primary may still be relying on the promise this node made before it went down.
        let last_heard_from_primary = clock.now();
        Ok(Self {
            config,
            arbiter,
            log,
            clock,
            epoch_path,
            inner: Mutex::new(StateInner {
                epoch,
                term: None,
                witness_lease_until: Duration::ZERO,
                peer_lease_until: Duration::ZERO,
                last_heard_from_primary,
                candidate: false,
            }),
            terms: watch::Sender::new(None),
        })
    }

    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn log(&self) -> &Arc<ReplicatedLog> {
        &self.log
    }

    pub fn clock(&self) -> &MonotonicClock {
        &self.clock
    }

    pub fn epoch(&self) -> i64 {
        self.lock().epoch
    }

    /// The current term while this node is primary, even if its lease just ran out.
    pub fn current_term(&self) -> Option<Arc<PrimaryTerm>> {
        self.lock().term.clone()
    }

    /// The current term if this node may serve right now (primary with a valid lease).
    pub fn serving_term(&self) -> Option<Arc<PrimaryTerm>> {
        let inner = self.lock();
        self.serving_term_locked(&inner)
    }

    /// The current term, or the next one once this node is promoted.
    pub async fn wait_for_term(&self) -> Arc<PrimaryTerm> {
        let mut terms = self.terms.subscribe();
        loop {
            if let Ok(term) = terms.wait_for(Option::is_some).await
                && let Some(term) = term.as_ref()
            {
                return Arc::clone(term);
            }
            // Unreachable while `self` is alive (it owns the sender); never spin if it were.
            std::future::pending::<()>().await;
        }
    }

    pub fn append_as_primary(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(LogEntry, Arc<PrimaryTerm>), ApiError> {
        let inner = self.lock();
        let term = self.serving_term_locked(&inner).ok_or_else(not_primary)?;
        let entry = self
            .log
            .append(term.epoch(), key, value)
            .map_err(|error| ApiError::Internal(format!("could not write the log: {error}")))?;
        term.notify_appended();
        Ok((entry, term))
    }

    /// The standby side of replication.
    pub fn handle_replicate(&self, request: &ReplicateRequest) -> Result<ReplicateReply, ApiError> {
        let mut inner = self.lock();
        let source = format!(
            "replication from {}",
            request.leader.as_deref().unwrap_or("")
        );
        if request.epoch < inner.epoch {
            return Ok(self.rejected(
                &inner,
                format!("epoch {} is older than {}", request.epoch, inner.epoch),
            ));
        }
        if inner.candidate {
            // Accepting would promise the sender a lease while this node may be granted one by the witness.
            self.observe_epoch_locked(&mut inner, request.epoch, &source)
                .map_err(epoch_write_failed)?;
            return Ok(self.rejected(&inner, "asking the witness for the lease".into()));
        }
        if request.epoch > inner.epoch {
            self.observe_epoch_locked(&mut inner, request.epoch, &source)
                .map_err(epoch_write_failed)?;
        } else if inner.term.is_some() {
            // Only possible without a witness.
            return Ok(self.rejected(&inner, format!("also primary in epoch {}", inner.epoch)));
        }
        inner.last_heard_from_primary = self.clock.now();
        let truncated_before = self.log.truncated_entries();
        let outcome = self
            .log
            .append_from(
                request.epoch,
                request.prev_index,
                request.prev_epoch,
                request.entries.as_deref().unwrap_or_default(),
                request.leader_last_index,
            )
            .map_err(|error| match error {
                AppendError::Invalid(message) => ApiError::InvalidInput(message),
                AppendError::Io(error) => {
                    ApiError::Internal(format!("could not write the log: {error}"))
                }
            })?;
        let dropped = self.log.truncated_entries() - truncated_before;
        if dropped > 0 {
            info!(
                "Dropped {dropped} log entries the primary of epoch {} never had; log now ends at {}",
                request.epoch,
                self.log.last_index()
            );
        }
        // The promise: this node will not seek the lease for one TTL from now (see try_begin_candidacy).
        let promise_ms = millis(self.config.lease_ttl);
        Ok(match outcome.status {
            AppendStatus::Ok => ReplicateReply {
                status: ReplicateStatus::Ok,
                epoch: inner.epoch,
                match_index: outcome.match_index,
                promise_ms,
                reason: None,
            },
            AppendStatus::Mismatch => ReplicateReply {
                status: ReplicateStatus::Mismatch,
                epoch: inner.epoch,
                match_index: outcome.match_index,
                promise_ms,
                reason: Some("log does not contain prevIndex/prevEpoch".into()),
            },
        })
    }

    /// Starts a primary term. Returns false if a newer epoch is already known (or cannot be saved).
    ///
    /// `standby_in_sync` and `witness_version` are what the arbiter said about the other node when it
    /// granted the lease (version 0 without a witness).
    pub fn try_promote(
        &self,
        epoch: i64,
        lease_until: Duration,
        standby_in_sync: bool,
        witness_version: i64,
    ) -> bool {
        let mut inner = self.lock();
        if epoch < inner.epoch {
            return false;
        }
        if let Err(error) = self.save_epoch_locked(&mut inner, epoch) {
            error!("Could not save epoch {epoch}, so not taking the primary role: {error}");
            return false;
        }
        if inner.term.is_some() {
            self.step_down_locked(&mut inner, "starting a new term");
        }
        let term = Arc::new(PrimaryTerm::new(
            epoch,
            standby_in_sync,
            witness_version,
            Arc::clone(&self.log),
            Arc::clone(&self.arbiter),
            MAX_BATCH as i64,
        ));
        inner.term = Some(Arc::clone(&term));
        inner.witness_lease_until = lease_until;
        inner.peer_lease_until = Duration::ZERO;
        self.terms.send_replace(Some(term));
        warn!(
            "Promoted to primary in epoch {epoch} at index {}; standby in sync: {standby_in_sync}",
            self.log.last_index()
        );
        true
    }

    pub fn extend_witness_lease(&self, term: &Arc<PrimaryTerm>, until: Duration) {
        let mut inner = self.lock();
        if is_current(&inner, term) && until > inner.witness_lease_until {
            inner.witness_lease_until = until;
        }
    }

    pub fn extend_peer_lease(&self, term: &Arc<PrimaryTerm>, until: Duration) {
        let mut inner = self.lock();
        if is_current(&inner, term) && until > inner.peer_lease_until {
            inner.peer_lease_until = until;
        }
    }

    pub fn step_down(&self, term: &Arc<PrimaryTerm>, reason: &str) {
        let mut inner = self.lock();
        if is_current(&inner, term) {
            self.step_down_locked(&mut inner, reason);
        }
    }

    pub fn step_down_if_lease_expired(&self) {
        let mut inner = self.lock();
        if inner.term.is_some() && self.serving_term_locked(&inner).is_none() {
            self.step_down_locked(&mut inner, "lease expired");
        }
    }

    /// Adopts a newer epoch seen elsewhere; a primary that sees one has been deposed.
    pub fn observe_epoch(&self, epoch: i64, source: &str) {
        let mut inner = self.lock();
        if let Err(error) = self.observe_epoch_locked(&mut inner, epoch, source) {
            error!("Could not save epoch {epoch} seen in {source}: {error}");
        }
    }

    /// Marks this standby as a candidate if it has not heard from a primary for a full lease (plus the
    /// election delay). While a candidate it refuses replication, so no primary can count on it while
    /// it asks for the lease.
    pub fn try_begin_candidacy(&self) -> bool {
        let mut inner = self.lock();
        let silence = self
            .clock
            .now()
            .saturating_sub(inner.last_heard_from_primary);
        if inner.term.is_some()
            || inner.candidate
            || silence < self.config.lease_ttl + self.config.election_delay
        {
            return false;
        }
        inner.candidate = true;
        true
    }

    pub fn end_candidacy(&self) {
        self.lock().candidate = false;
    }

    pub fn health(&self) -> NodeHealth {
        let inner = self.lock();
        let serving = self.serving_term_locked(&inner);
        let lease_remaining_ms = match (&serving, self.config.arbiter) {
            (Some(_), ArbiterMode::Witness) => {
                Some(millis(inner.lease_until().saturating_sub(self.clock.now())))
            }
            _ => None,
        };
        NodeHealth {
            status: "ok".into(),
            node: self.config.node_id.clone(),
            role: if serving.is_some() {
                "primary"
            } else {
                "standby"
            }
            .into(),
            epoch: inner.epoch,
            last_index: self.log.last_index(),
            match_index: serving.as_ref().map(|term| term.match_index()),
            peer: serving
                .as_ref()
                .map(|term| term.peer_state().wire_name().to_owned()),
            lease_remaining_ms,
            truncated_entries: self.log.truncated_entries(),
            arbiter: self.config.arbiter.wire_name().into(),
            replication: self.config.replication.wire_name().into(),
        }
    }

    fn serving_term_locked(&self, inner: &StateInner) -> Option<Arc<PrimaryTerm>> {
        inner
            .term
            .as_ref()
            .filter(|_| self.clock.now() < inner.lease_until())
            .cloned()
    }

    fn observe_epoch_locked(
        &self,
        inner: &mut StateInner,
        epoch: i64,
        source: &str,
    ) -> io::Result<()> {
        if epoch <= inner.epoch {
            return Ok(());
        }
        self.save_epoch_locked(inner, epoch)?;
        if inner.term.is_some() {
            self.step_down_locked(inner, &format!("epoch {epoch} seen in {source}"));
            // Someone newer is primary; give it a full lease to reach this node before competing again.
            inner.last_heard_from_primary = self.clock.now();
        }
        Ok(())
    }

    fn step_down_locked(&self, inner: &mut StateInner, reason: &str) {
        let Some(term) = inner.term.take() else {
            return;
        };
        inner.witness_lease_until = Duration::ZERO;
        inner.peer_lease_until = Duration::ZERO;
        term.end();
        self.terms.send_replace(None);
        warn!(
            "Stepped down from epoch {} at index {}: {reason}",
            term.epoch(),
            self.log.last_index()
        );
    }

    /// Persists first, so a restart can never come back with an older epoch than this node acted on.
    fn save_epoch_locked(&self, inner: &mut StateInner, epoch: i64) -> io::Result<()> {
        if epoch == inner.epoch {
            return Ok(());
        }
        state_file::save(&self.epoch_path, &EpochRecord { epoch })?;
        inner.epoch = epoch;
        Ok(())
    }

    fn rejected(&self, inner: &StateInner, reason: String) -> ReplicateReply {
        ReplicateReply {
            status: ReplicateStatus::Rejected,
            epoch: inner.epoch,
            match_index: self.log.last_index(),
            promise_ms: 0,
            reason: Some(reason),
        }
    }

    fn lock(&self) -> MutexGuard<'_, StateInner> {
        // Nothing panics while holding this lock (see ReplicatedLog::lock).
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub fn not_primary() -> ApiError {
    ApiError::Unavailable("not primary".into())
}

fn is_current(inner: &StateInner, term: &Arc<PrimaryTerm>) -> bool {
    inner
        .term
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, term))
}

fn epoch_write_failed(error: io::Error) -> ApiError {
    ApiError::Internal(format!("could not save the epoch: {error}"))
}

/// Whole milliseconds, truncated like .NET's `(long)TimeSpan.TotalMilliseconds`.
pub fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::time::advance;

    use super::*;
    use crate::cluster::witness_client::NoArbiter;

    const TTL: Duration = Duration::from_secs(3);

    struct Fixture {
        dir: TempDir,
        logs: usize,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().unwrap(),
                logs: 0,
            }
        }

        fn state(&mut self) -> NodeState {
            self.state_with(|config| config)
        }

        fn state_with(&mut self, tweak: impl FnOnce(NodeConfig) -> NodeConfig) -> NodeState {
            let config = tweak(NodeConfig {
                data_dir: self.dir.path().to_owned(),
                lease_ttl: TTL,
                ..NodeConfig::default()
            });
            self.logs += 1;
            let log =
                ReplicatedLog::open(&self.dir.path().join(format!("log-{}.jsonl", self.logs)))
                    .unwrap();
            NodeState::new(config, Arc::new(log), Arc::new(NoArbiter)).unwrap()
        }
    }

    fn request(epoch: i64, prev_index: i64, entries: Vec<LogEntry>) -> ReplicateRequest {
        ReplicateRequest {
            leader: Some("b".into()),
            epoch,
            prev_index,
            prev_epoch: if prev_index == 0 { 0 } else { epoch },
            leader_last_index: prev_index + entries.len() as i64,
            entries: Some(entries),
        }
    }

    fn entry(index: i64, epoch: i64, key: &str, value: &str) -> LogEntry {
        LogEntry {
            index,
            epoch,
            key: key.into(),
            value: value.into(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn standby_refuses_writes() {
        let state = Fixture::new().state();

        assert!(state.serving_term().is_none());
        assert_eq!(state.append_as_primary("k", "v").err(), Some(not_primary()));
        assert_eq!(state.health().role, "standby");
    }

    #[tokio::test(start_paused = true)]
    async fn primary_serves_until_its_lease_runs_out() {
        let state = Fixture::new().state();
        state.try_promote(1, state.clock().now() + TTL, false, 0);

        let (entry1, term) = state.append_as_primary("k", "v").unwrap();
        assert_eq!(entry1, entry(1, 1, "k", "v"));
        assert_eq!(state.health().role, "primary");

        advance(TTL).await;
        assert!(state.serving_term().is_none());
        assert!(state.append_as_primary("k", "v2").is_err());
        state.step_down_if_lease_expired();
        assert!(state.current_term().is_none());
        assert!(term.is_ended());
    }

    #[tokio::test(start_paused = true)]
    async fn peer_lease_keeps_the_primary_serving_without_the_witness() {
        let state = Fixture::new().state();
        state.try_promote(1, state.clock().now() + TTL, true, 0);
        let term = state.current_term().unwrap();

        advance(TTL - Duration::from_secs(1)).await;
        state.extend_peer_lease(&term, state.clock().now() + TTL);
        advance(Duration::from_secs(2)).await;

        assert!(Arc::ptr_eq(&state.serving_term().unwrap(), &term));
    }

    #[tokio::test(start_paused = true)]
    async fn lease_extensions_for_an_old_term_are_ignored() {
        let state = Fixture::new().state();
        state.try_promote(1, state.clock().now() + TTL, false, 0);
        let old = state.current_term().unwrap();
        state.try_promote(2, state.clock().now() + TTL, false, 0);

        state.extend_witness_lease(&old, state.clock().now() + TTL * 10);
        state.extend_peer_lease(&old, state.clock().now() + TTL * 10);
        state.step_down(&old, "ignored");
        advance(TTL).await;

        assert!(old.is_ended());
        assert!(state.serving_term().is_none());
        assert!(state.current_term().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn witness_lease_extension_keeps_the_current_term() {
        let state = Fixture::new().state();
        state.try_promote(1, state.clock().now() + TTL, true, 0);
        let term = state.current_term().unwrap();

        state.extend_witness_lease(&term, state.clock().now() + TTL * 2);
        advance(TTL + Duration::from_secs(1)).await;

        assert!(state.serving_term().is_some());
        state.step_down(&term, "test");
        assert!(state.current_term().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn try_promote_refuses_an_older_epoch() {
        let state = Fixture::new().state();
        state.observe_epoch(5, "test");

        assert!(!state.try_promote(4, state.clock().now() + TTL, false, 0));
        assert!(state.try_promote(5, state.clock().now() + TTL, false, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_term_completes_on_promotion() {
        let state = Arc::new(Fixture::new().state());
        let waiting = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { state.wait_for_term().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        state.try_promote(1, state.clock().now() + TTL, false, 0);

        let current = state.current_term().unwrap();
        assert!(Arc::ptr_eq(&waiting.await.unwrap(), &current));
        assert!(Arc::ptr_eq(&state.wait_for_term().await, &current));
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_applies_entries_from_the_current_primary() {
        let state = Fixture::new().state();

        let reply = state
            .handle_replicate(&request(1, 0, vec![entry(1, 1, "k", "v")]))
            .unwrap();

        assert_eq!(
            reply,
            ReplicateReply {
                status: ReplicateStatus::Ok,
                epoch: 1,
                match_index: 1,
                promise_ms: 3000,
                reason: None
            }
        );
        assert_eq!(state.log().get("k").map(|e| e.value), Some("v".into()));
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_reports_a_mismatch() {
        let state = Fixture::new().state();

        let reply = state.handle_replicate(&request(1, 3, vec![])).unwrap();

        assert_eq!(reply.status, ReplicateStatus::Mismatch);
        assert_eq!(reply.match_index, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_rejects_a_batch_that_does_not_fit() {
        let state = Fixture::new().state();
        let mut bad = request(1, 0, vec![entry(1, 1, "k", "v")]);
        bad.leader_last_index = 0;

        let error = state.handle_replicate(&bad).unwrap_err();

        assert!(matches!(error, ApiError::InvalidInput(_)), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_drops_what_a_newer_primary_never_had() {
        let state = Fixture::new().state();
        state
            .handle_replicate(&request(
                1,
                0,
                vec![entry(1, 1, "a", "1"), entry(2, 1, "b", "2")],
            ))
            .unwrap();

        let reply = state
            .handle_replicate(&request(2, 1, vec![]).with_prev_epoch(1))
            .unwrap();

        assert_eq!(reply.status, ReplicateStatus::Ok);
        assert_eq!(state.health().truncated_entries, 1);
        assert_eq!(state.log().last_index(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_from_a_deposed_primary_is_rejected_with_the_newer_epoch() {
        let state = Fixture::new().state();
        state.observe_epoch(3, "test");

        let reply = state
            .handle_replicate(&request(2, 0, vec![entry(1, 2, "k", "v")]))
            .unwrap();

        assert_eq!(reply.status, ReplicateStatus::Rejected);
        assert_eq!(reply.epoch, 3);
        assert_eq!(reply.promise_ms, 0);
        assert!(state.log().get("k").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_with_a_newer_epoch_deposes_this_primary() {
        let state = Fixture::new().state();
        state.try_promote(1, state.clock().now() + TTL, false, 0);
        let term = state.current_term().unwrap();

        let reply = state.handle_replicate(&request(2, 0, vec![])).unwrap();

        assert_eq!(reply.status, ReplicateStatus::Ok);
        assert!(state.current_term().is_none());
        assert!(term.is_ended());
        assert_eq!(state.epoch(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn replicate_in_the_same_epoch_while_primary_is_rejected() {
        let state = Fixture::new().state();
        state.try_promote(1, Duration::MAX, false, 0);

        let reply = state.handle_replicate(&request(1, 0, vec![])).unwrap();

        assert_eq!(reply.status, ReplicateStatus::Rejected);
        assert!(state.serving_term().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn candidacy_starts_only_after_a_full_lease_of_silence() {
        let state = Fixture::new().state();

        assert!(!state.try_begin_candidacy()); // just started: the old primary may still count on us
        advance(TTL).await;
        state.handle_replicate(&request(1, 0, vec![])).unwrap();
        advance(TTL - Duration::from_millis(1)).await;
        assert!(!state.try_begin_candidacy());
        advance(Duration::from_millis(1)).await;

        assert!(state.try_begin_candidacy());
        assert!(!state.try_begin_candidacy());
    }

    #[tokio::test(start_paused = true)]
    async fn candidate_refuses_replication_so_no_primary_can_count_on_it() {
        let state = Fixture::new().state();
        advance(TTL).await;
        assert!(state.try_begin_candidacy());

        let reply = state
            .handle_replicate(&request(1, 0, vec![entry(1, 1, "k", "v")]))
            .unwrap();

        assert_eq!(reply.status, ReplicateStatus::Rejected);
        assert_eq!(state.epoch(), 1);
        assert!(state.log().get("k").is_none());
        state.end_candidacy();
        assert_eq!(
            state
                .handle_replicate(&request(1, 0, vec![]))
                .unwrap()
                .status,
            ReplicateStatus::Ok
        );
    }

    #[tokio::test(start_paused = true)]
    async fn election_delay_postpones_candidacy() {
        let state = Fixture::new().state_with(|config| NodeConfig {
            election_delay: Duration::from_secs(1),
            ..config
        });

        advance(TTL).await;
        assert!(!state.try_begin_candidacy());
        advance(Duration::from_secs(1)).await;

        assert!(state.try_begin_candidacy());
    }

    #[tokio::test(start_paused = true)]
    async fn a_deposed_primary_waits_a_full_lease_before_competing() {
        let state = Fixture::new().state();
        advance(TTL).await;
        state.try_promote(1, state.clock().now() + TTL, false, 0);

        state.observe_epoch(2, "witness");

        assert!(state.current_term().is_none());
        assert!(!state.try_begin_candidacy());
        advance(TTL).await;
        assert!(state.try_begin_candidacy());
    }

    #[tokio::test(start_paused = true)]
    async fn epoch_survives_a_restart() {
        let mut fixture = Fixture::new();
        fixture.state().observe_epoch(7, "test");

        assert_eq!(fixture.state().epoch(), 7);
    }

    #[tokio::test(start_paused = true)]
    async fn epoch_starts_at_least_at_the_last_logged_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicatedLog::open(&dir.path().join("log.jsonl")).unwrap();
        log.append(4, "k", "v").unwrap();
        let config = NodeConfig {
            data_dir: dir.path().to_owned(),
            ..NodeConfig::default()
        };

        let state = NodeState::new(config, Arc::new(log), Arc::new(NoArbiter)).unwrap();

        assert_eq!(state.epoch(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn health_describes_a_primary() {
        let state = Fixture::new().state();
        state.try_promote(4, state.clock().now() + TTL, false, 0);
        state.append_as_primary("k", "v").unwrap();

        let health = state.health();

        assert_eq!(
            health,
            NodeHealth {
                status: "ok".into(),
                node: "a".into(),
                role: "primary".into(),
                epoch: 4,
                last_index: 1,
                match_index: Some(0),
                peer: Some("catching-up".into()),
                lease_remaining_ms: Some(3000),
                truncated_entries: 0,
                arbiter: "witness".into(),
                replication: "sync".into(),
            }
        );
        assert_eq!(
            serde_json::to_value(&health).unwrap()["leaseRemainingMs"],
            serde_json::json!(3000)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn health_of_a_standby_has_no_term_fields() {
        let state = Fixture::new().state();

        let json = serde_json::to_string(&state.health()).unwrap();

        assert_eq!(
            json,
            r#"{"status":"ok","node":"a","role":"standby","epoch":0,"lastIndex":0,"matchIndex":null,"peer":null,"leaseRemainingMs":null,"truncatedEntries":0,"arbiter":"witness","replication":"sync"}"#
        );
    }

    impl ReplicateRequest {
        fn with_prev_epoch(mut self, prev_epoch: i64) -> Self {
            self.prev_epoch = prev_epoch;
            self
        }
    }
}
