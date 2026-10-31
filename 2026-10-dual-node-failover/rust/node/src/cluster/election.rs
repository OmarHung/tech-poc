use std::sync::Arc;
use std::time::Duration;

use shared::wire::LeaseReply;
use tokio::time::{MissedTickBehavior, interval};
use tracing::debug;

use super::node_state::NodeState;
use super::primary_term::PrimaryTerm;
use super::witness_client::LeaseSource;
use crate::config::ArbiterMode;

/// Decides when this node becomes or stops being primary. Runs every RENEW_INTERVAL_MS.
pub struct ElectionWorker {
    state: Arc<NodeState>,
    witness: Arc<dyn LeaseSource>,
}

impl ElectionWorker {
    pub fn new(state: Arc<NodeState>, witness: Arc<dyn LeaseSource>) -> Self {
        Self { state, witness }
    }

    pub async fn run(self) {
        let mut timer = interval(self.state.config().renew_interval);
        // Like .NET's PeriodicTimer: a tick missed during a slow call fires once, then the schedule resumes.
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            match self.state.config().arbiter {
                ArbiterMode::Witness => self.tick_with_witness().await,
                ArbiterMode::None => self.tick_without_witness(),
            }
        }
    }

    pub async fn tick_with_witness(&self) {
        self.state.step_down_if_lease_expired();
        if let Some(term) = self.state.current_term() {
            self.renew(&term).await;
            return;
        }
        if !self.state.try_begin_candidacy() {
            return;
        }
        let _candidacy = Candidacy(&self.state);
        let sent_at = self.state.clock().now();
        let Some(reply) = self.witness.acquire().await else {
            return;
        };
        if !reply.granted {
            self.state.observe_epoch(reply.epoch, "witness");
            debug!(
                "Witness refused the lease: {}",
                reply.reason.as_deref().unwrap_or("")
            );
            return;
        }
        self.state.try_promote(
            reply.epoch,
            self.lease_end(sent_at, &reply),
            reply.standby_in_sync,
            reply.sync_version,
        );
    }

    /// The classic setup: the standby promotes itself once the primary has been quiet for a full lease.
    pub fn tick_without_witness(&self) {
        if self.state.current_term().is_some() || !self.state.try_begin_candidacy() {
            return;
        }
        let _candidacy = Candidacy(&self.state);
        self.state
            .try_promote(self.state.epoch() + 1, Duration::MAX, false, 0);
    }

    async fn renew(&self, term: &Arc<PrimaryTerm>) {
        let sent_at = self.state.clock().now();
        let Some(reply) = self.witness.acquire().await else {
            return; // the peer lease may still carry this node
        };
        if reply.granted && reply.epoch == term.epoch() {
            self.state
                .extend_witness_lease(term, self.lease_end(sent_at, &reply));
            term.observe_witness(reply.sync_version, reply.standby_in_sync);
            return;
        }
        if reply.granted {
            self.state.try_promote(
                reply.epoch,
                self.lease_end(sent_at, &reply),
                reply.standby_in_sync,
                reply.sync_version,
            );
            return;
        }
        self.state.observe_epoch(reply.epoch, "witness");
        let reason = format!(
            "witness refused renewal: {}",
            reply.reason.as_deref().unwrap_or("")
        );
        self.state.step_down(term, &reason);
    }

    /// The lease started no later than when the request was sent; end it a margin early for clock drift.
    fn lease_end(&self, sent_at: Duration, reply: &LeaseReply) -> Duration {
        let ttl = Duration::from_millis(u64::try_from(reply.ttl_ms).unwrap_or(0));
        (sent_at + ttl).saturating_sub(self.state.config().lease_safety_margin())
    }
}

/// Ends the candidacy however the attempt finishes, including a cancelled or panicking one.
struct Candidacy<'a>(&'a NodeState);

impl Drop for Candidacy<'_> {
    fn drop(&mut self) {
        self.0.end_candidacy();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tempfile::TempDir;
    use tokio::time::advance;

    use super::*;
    use crate::cluster::witness_client::NoArbiter;
    use crate::config::NodeConfig;
    use crate::storage::replicated_log::ReplicatedLog;

    const TTL: Duration = Duration::from_secs(3);

    /// Answers lease requests from a queue; an empty queue means the witness is unreachable.
    #[derive(Default)]
    struct ScriptedWitness {
        replies: Mutex<Vec<LeaseReply>>,
        calls: Mutex<usize>,
    }

    impl ScriptedWitness {
        fn answer(&self, reply: LeaseReply) {
            self.replies.lock().unwrap().push(reply);
        }

        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }

    #[async_trait]
    impl LeaseSource for ScriptedWitness {
        async fn acquire(&self) -> Option<LeaseReply> {
            *self.calls.lock().unwrap() += 1;
            let mut replies = self.replies.lock().unwrap();
            (!replies.is_empty()).then(|| replies.remove(0))
        }
    }

    fn granted(epoch: i64, standby_in_sync: bool, sync_version: i64) -> LeaseReply {
        LeaseReply {
            granted: true,
            holder: Some("a".into()),
            epoch,
            ttl_ms: 3000,
            standby_in_sync,
            sync_version,
            reason: None,
        }
    }

    fn refused(epoch: i64) -> LeaseReply {
        LeaseReply {
            granted: false,
            holder: Some("b".into()),
            epoch,
            ttl_ms: 3000,
            reason: Some("lease held by b".into()),
            ..LeaseReply::default()
        }
    }

    fn worker(
        arbiter: crate::config::ArbiterMode,
    ) -> (TempDir, ElectionWorker, Arc<ScriptedWitness>) {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            data_dir: dir.path().to_owned(),
            lease_ttl: TTL,
            arbiter,
            ..NodeConfig::default()
        };
        let log = Arc::new(ReplicatedLog::open(&dir.path().join("log.jsonl")).unwrap());
        let state = Arc::new(NodeState::new(config, log, Arc::new(NoArbiter)).unwrap());
        let witness = Arc::new(ScriptedWitness::default());
        (dir, ElectionWorker::new(state, witness.clone()), witness)
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_node_waits_a_full_lease_before_asking() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);

        election.tick_with_witness().await;
        advance(TTL).await;
        witness.answer(granted(1, false, 1));
        election.tick_with_witness().await;

        assert_eq!(witness.calls(), 1);
        let health = election.state.health();
        assert_eq!((health.role.as_str(), health.epoch), ("primary", 1));
        assert_eq!(health.lease_remaining_ms, Some(2700)); // TTL minus the 10% safety margin
        assert!(!election.state.try_begin_candidacy());
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_adopts_the_witness_epoch_and_ends_the_candidacy() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        advance(TTL).await;
        witness.answer(refused(4));

        election.tick_with_witness().await;

        assert_eq!(election.state.epoch(), 4);
        assert!(election.state.current_term().is_none());
        assert!(
            election.state.try_begin_candidacy(),
            "candidacy must have ended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_witness_promotes_nobody() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        advance(TTL).await;

        election.tick_with_witness().await;

        assert_eq!(witness.calls(), 1);
        assert!(election.state.current_term().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_extends_the_lease_and_passes_on_the_sync_state() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        advance(TTL).await;
        witness.answer(granted(1, true, 5));
        election.tick_with_witness().await;
        let term = election.state.current_term().unwrap();

        advance(Duration::from_secs(2)).await;
        witness.answer(granted(1, false, 6)); // e.g. a solo report whose reply was lost
        election.tick_with_witness().await;
        advance(Duration::from_secs(2)).await;

        assert!(election.state.serving_term().is_some());
        assert_eq!(
            term.peer_state(),
            crate::cluster::primary_term::PeerState::CatchingUp
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_witness_does_not_end_the_term_by_itself() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        advance(TTL).await;
        witness.answer(granted(1, true, 1));
        election.tick_with_witness().await;

        election.tick_with_witness().await;

        assert!(election.state.serving_term().is_some());
        advance(TTL).await;
        election.tick_with_witness().await;
        assert!(
            election.state.current_term().is_none(),
            "lease expired without renewal"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_renewal_steps_down() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        advance(TTL).await;
        witness.answer(granted(1, true, 1));
        election.tick_with_witness().await;
        let term = election.state.current_term().unwrap();

        witness.answer(refused(2));
        election.tick_with_witness().await;

        assert!(term.is_ended());
        assert_eq!(election.state.epoch(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_renewal_granted_in_a_newer_epoch_starts_a_new_term() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        advance(TTL).await;
        witness.answer(granted(1, true, 1));
        election.tick_with_witness().await;
        let first = election.state.current_term().unwrap();

        witness.answer(granted(3, false, 4));
        election.tick_with_witness().await;

        assert!(first.is_ended());
        assert_eq!(election.state.current_term().map(|t| t.epoch()), Some(3));
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_witness_the_standby_promotes_itself_after_silence() {
        let (_dir, election, witness) = worker(ArbiterMode::None);

        election.tick_without_witness();
        assert!(election.state.current_term().is_none());
        advance(TTL).await;
        election.tick_without_witness();
        election.tick_without_witness();

        assert_eq!(election.state.epoch(), 1);
        assert_eq!(election.state.health().lease_remaining_ms, None);
        assert_eq!(witness.calls(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn run_ticks_on_the_renew_interval() {
        let (_dir, election, witness) = worker(ArbiterMode::Witness);
        let state = Arc::clone(&election.state);
        for _ in 0..20 {
            witness.answer(granted(1, true, 1));
        }
        let running = tokio::spawn(election.run());

        tokio::time::sleep(TTL + Duration::from_millis(1100)).await;
        running.abort();

        assert_eq!(state.current_term().map(|t| t.epoch()), Some(1));
        assert!(witness.calls() >= 2, "{} calls", witness.calls());
    }

    #[test]
    fn lease_end_never_underflows() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let (_dir, election, _) = worker(ArbiterMode::Witness);
            let reply = LeaseReply {
                ttl_ms: -5,
                ..granted(1, true, 1)
            };
            assert_eq!(election.lease_end(Duration::ZERO, &reply), Duration::ZERO);
        });
    }
}
