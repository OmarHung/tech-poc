use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use shared::clock::MonotonicClock;
use shared::error::ApiError;
use shared::wire::SyncReply;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::storage::replicated_log::ReplicatedLog;

/// Where the standby stands, as far as the primary knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// The witness may promote the standby, so every acknowledged write must already be on it.
    InSync,
    /// The witness agreed not to promote the standby; a write is acknowledged once it is on this node.
    Solo,
    /// Writes wait for the standby, and once it has everything the witness is told it may take over again.
    CatchingUp,
}

impl PeerState {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::InSync => "in-sync",
            Self::Solo => "solo",
            Self::CatchingUp => "catching-up",
        }
    }
}

/// Reports to the arbiter whether the standby may take over. Without a witness every report is accepted.
#[async_trait]
pub trait Arbiter: Send + Sync {
    /// `expected_version` is the latest witness version this node has seen; only checked for
    /// `in_sync = true`. Returns the witness's answer, or `None` if the report was refused or the
    /// witness could not be reached.
    async fn set_standby_in_sync(
        &self,
        epoch: i64,
        in_sync: bool,
        expected_version: i64,
    ) -> Option<SyncReply>;
}

/// One stint as primary: created on promotion, ended on step-down. Tracks how far the standby has
/// replicated and decides when a write is safe to acknowledge.
pub struct PrimaryTerm {
    epoch: i64,
    log: Arc<ReplicatedLog>,
    arbiter: Arc<dyn Arbiter>,
    /// How far behind the standby may be when writes start waiting for it again.
    catch_up_slack: i64,
    ended: CancellationToken,
    /// One witness report at a time, so this node never has two in flight that could arrive in the
    /// wrong order. Reports from an older process or a call this node gave up on are handled by the
    /// witness's version check.
    witness_calls: tokio::sync::Mutex<()>,
    /// Bumped whenever `match_index` or `peer_state` moves, or the term ends.
    progress: watch::Sender<u64>,
    /// Bumped on every append, and when the term ends.
    appended: watch::Sender<u64>,
    inner: Mutex<TermInner>,
}

struct TermInner {
    peer_state: PeerState,
    match_index: i64,
    catch_up_target: i64,
    witness_version: i64,
    /// The "go on alone" request in flight, shared by every write waiting for it.
    solo_request: Option<watch::Receiver<Option<bool>>>,
}

impl PrimaryTerm {
    /// `standby_in_sync` and `witness_version` are what the witness said when it granted the lease.
    pub fn new(
        epoch: i64,
        standby_in_sync: bool,
        witness_version: i64,
        log: Arc<ReplicatedLog>,
        arbiter: Arc<dyn Arbiter>,
        catch_up_slack: i64,
    ) -> Self {
        let catch_up_target = log.last_index();
        Self {
            epoch,
            log,
            arbiter,
            catch_up_slack,
            ended: CancellationToken::new(),
            witness_calls: tokio::sync::Mutex::new(()),
            progress: watch::Sender::new(0),
            appended: watch::Sender::new(0),
            inner: Mutex::new(TermInner {
                peer_state: if standby_in_sync {
                    PeerState::InSync
                } else {
                    PeerState::CatchingUp
                },
                match_index: 0,
                catch_up_target,
                witness_version,
                solo_request: None,
            }),
        }
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Cancelled when the term ends.
    pub fn ended(&self) -> &CancellationToken {
        &self.ended
    }

    pub fn is_ended(&self) -> bool {
        self.ended.is_cancelled()
    }

    pub fn peer_state(&self) -> PeerState {
        self.lock().peer_state
    }

    pub fn match_index(&self) -> i64 {
        self.lock().match_index
    }

    /// Fires on the next append. Take it before reading the log so no append slips between.
    pub fn append_signal(&self) -> watch::Receiver<u64> {
        self.appended.subscribe()
    }

    pub fn notify_appended(&self) {
        pulse(&self.appended);
    }

    /// Ends the term: waiting writes fail, the replicator stops.
    pub(crate) fn end(&self) {
        let _inner = self.lock();
        self.ended.cancel();
        pulse(&self.progress);
        pulse(&self.appended);
    }

    /// Returns once `index` may be acknowledged: it is on the standby, or the witness has agreed the
    /// standby is out. If the standby has not confirmed within `timeout`, asks the witness to go on alone.
    pub async fn wait_durable(
        self: &Arc<Self>,
        index: i64,
        timeout: Duration,
        clock: &MonotonicClock,
    ) -> Result<(), ApiError> {
        let deadline = clock.now() + timeout;
        loop {
            let mut progress = {
                let inner = self.lock();
                if index <= inner.match_index || inner.peer_state == PeerState::Solo {
                    return Ok(());
                }
                if self.ended.is_cancelled() {
                    return Err(ApiError::Unavailable(
                        "lost the primary role before the write reached the standby".into(),
                    ));
                }
                self.progress.subscribe()
            };
            let now = clock.now();
            if now >= deadline {
                if !self.try_go_solo().await {
                    return Err(ApiError::Unavailable(
                        "standby unreachable and the witness did not allow going on alone".into(),
                    ));
                }
                continue;
            }
            // Progress or the deadline: either way the loop looks again.
            let _ = tokio::time::timeout(deadline - now, progress.changed()).await;
        }
    }

    /// Asks the witness to let this node acknowledge writes on its own. Writes waiting for the standby
    /// share one request. The replicator also calls it when a term starts with the witness saying the
    /// standby is behind: that both avoids stalling the first writes after a failover and outdates any
    /// late "in sync" from an older process.
    pub async fn try_go_solo(self: &Arc<Self>) -> bool {
        let mut result = {
            let mut inner = self.lock();
            if inner.peer_state == PeerState::Solo {
                return true;
            }
            match &inner.solo_request {
                // A closed channel means the request task died without answering; start a new one.
                Some(pending) if pending.has_changed().is_ok() => pending.clone(),
                _ => {
                    let (answer, pending) = watch::channel(None);
                    inner.solo_request = Some(pending.clone());
                    let term = Arc::clone(self);
                    // Spawned, so a caller that gives up (client gone) does not cancel it for the others.
                    tokio::spawn(async move {
                        let granted = term.request_solo().await;
                        term.lock().solo_request = None;
                        answer.send_replace(Some(granted));
                    });
                    pending
                }
            }
        };
        match result.wait_for(Option::is_some).await {
            Ok(granted) => (*granted).unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Records that the standby holds everything up to `match_index`. Returns true when the standby
    /// has caught up and the witness should be told it may be promoted again.
    pub fn record_replicated(&self, match_index: i64) -> bool {
        let mut inner = self.lock();
        inner.match_index = inner.match_index.max(match_index);
        if inner.peer_state == PeerState::Solo {
            // Read under this lock: a write acknowledged while Solo was appended before it saw Solo, so
            // it is at or below this index, and every write from here on waits for the standby.
            let last_index = self.log.last_index();
            if last_index - inner.match_index <= self.catch_up_slack {
                self.start_catching_up(&mut inner, last_index, "the standby is close behind");
            }
        }
        pulse(&self.progress);
        inner.peer_state == PeerState::CatchingUp && inner.match_index >= inner.catch_up_target
    }

    /// Takes in what the witness reported on lease renewal. If it says the standby is behind while this
    /// node thinks it is in sync (a "go on alone" whose reply was lost), the standby goes through
    /// catching up and is reported again.
    pub fn observe_witness(&self, version: i64, standby_in_sync: bool) {
        let mut inner = self.lock();
        if version < inner.witness_version {
            return; // an older reply overtaken by one already seen
        }
        inner.witness_version = version;
        if !standby_in_sync && inner.peer_state == PeerState::InSync {
            let last_index = self.log.last_index();
            self.start_catching_up(
                &mut inner,
                last_index,
                "the witness says the standby is behind",
            );
        }
    }

    /// Tells the witness the standby may be promoted again, if it still has every acknowledged write.
    pub async fn try_mark_in_sync(&self, cancel: &CancellationToken) {
        // Biased: an already-cancelled caller must not start a report, like a cancelled token in .NET.
        let _turn = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            turn = self.witness_calls.lock() => turn,
        };
        let expected_version = {
            let inner = self.lock();
            if inner.peer_state != PeerState::CatchingUp
                || inner.match_index < inner.catch_up_target
            {
                return;
            }
            inner.witness_version
        };
        let reply = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            reply = self.arbiter.set_standby_in_sync(self.epoch, true, expected_version) => reply,
        };
        let Some(reply) = reply else {
            return;
        };
        let mut inner = self.lock();
        inner.witness_version = inner.witness_version.max(reply.sync_version);
        if inner.peer_state == PeerState::CatchingUp {
            inner.peer_state = PeerState::InSync;
            info!(
                "Epoch {}: standby in sync at index {}; the witness may promote it again (version {})",
                self.epoch, inner.match_index, inner.witness_version
            );
        }
    }

    async fn request_solo(&self) -> bool {
        let _turn = tokio::select! {
            biased;
            () = self.ended.cancelled() => return false,
            turn = self.witness_calls.lock() => turn,
        };
        let version = self.lock().witness_version;
        let reply = tokio::select! {
            biased;
            () = self.ended.cancelled() => return false,
            reply = self.arbiter.set_standby_in_sync(self.epoch, false, version) => reply,
        };
        let Some(reply) = reply else {
            return false;
        };
        let mut inner = self.lock();
        inner.witness_version = inner.witness_version.max(reply.sync_version);
        if inner.peer_state != PeerState::Solo {
            info!(
                "Epoch {}: standby out of sync at match index {}; acknowledging writes alone (witness version {})",
                self.epoch, inner.match_index, inner.witness_version
            );
        }
        inner.peer_state = PeerState::Solo;
        pulse(&self.progress);
        true
    }

    fn start_catching_up(&self, inner: &mut TermInner, target: i64, why: &str) {
        inner.peer_state = PeerState::CatchingUp;
        inner.catch_up_target = target;
        info!(
            "Epoch {}: standby catching up to index {target} ({why}); writes wait for it again",
            self.epoch
        );
    }

    fn lock(&self) -> MutexGuard<'_, TermInner> {
        // Nothing panics while holding this lock (see ReplicatedLog::lock).
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn pulse(signal: &watch::Sender<u64>) {
    signal.send_modify(|version| *version = version.wrapping_add(1));
}

#[cfg(test)]
pub(crate) mod tests {
    use tempfile::TempDir;
    use tokio::task::JoinHandle;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(1);

    /// Behaves like the witness's sync rules for the holder in its own epoch.
    pub(crate) struct FakeWitness {
        state: Mutex<FakeWitnessState>,
    }

    pub(crate) struct FakeWitnessState {
        pub version: i64,
        pub standby_in_sync: bool,
        pub reachable: bool,
        pub requests: Vec<(bool, i64)>,
    }

    impl FakeWitness {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(FakeWitnessState {
                    version: 10,
                    standby_in_sync: false,
                    reachable: true,
                    requests: Vec::new(),
                }),
            })
        }

        pub(crate) fn with<R>(&self, f: impl FnOnce(&mut FakeWitnessState) -> R) -> R {
            f(&mut self.state.lock().unwrap())
        }
    }

    #[async_trait]
    impl Arbiter for FakeWitness {
        async fn set_standby_in_sync(
            &self,
            epoch: i64,
            in_sync: bool,
            expected_version: i64,
        ) -> Option<SyncReply> {
            self.with(|s| {
                s.requests.push((in_sync, expected_version));
                if !s.reachable || (in_sync && expected_version != s.version) {
                    return None;
                }
                s.version += 1;
                s.standby_in_sync = in_sync;
                Some(SyncReply {
                    epoch,
                    standby_in_sync: in_sync,
                    sync_version: s.version,
                })
            })
        }
    }

    struct Fixture {
        _dir: TempDir,
        log: Arc<ReplicatedLog>,
        witness: Arc<FakeWitness>,
        clock: MonotonicClock,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let log = Arc::new(ReplicatedLog::open(&dir.path().join("log.jsonl")).unwrap());
            Self {
                _dir: dir,
                log,
                witness: FakeWitness::new(),
                clock: MonotonicClock::new(),
            }
        }

        fn term(&self, standby_in_sync: bool, slack: i64) -> Arc<PrimaryTerm> {
            let version = self.witness.with(|s| s.version);
            Arc::new(PrimaryTerm::new(
                1,
                standby_in_sync,
                version,
                self.log.clone(),
                self.witness.clone(),
                slack,
            ))
        }

        async fn solo_term(&self, slack: i64) -> Arc<PrimaryTerm> {
            let term = self.term(false, slack);
            assert!(term.try_go_solo().await);
            term
        }

        fn append(&self, key: &str) -> i64 {
            self.log.append(1, key, key).unwrap().index
        }

        fn wait(&self, term: &Arc<PrimaryTerm>, index: i64) -> JoinHandle<Result<(), ApiError>> {
            let term = Arc::clone(term);
            let clock = self.clock;
            tokio::spawn(async move { term.wait_durable(index, TIMEOUT, &clock).await })
        }

        fn requests(&self) -> Vec<(bool, i64)> {
            self.witness.with(|s| s.requests.clone())
        }
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn in_sync_write_is_durable_once_the_standby_has_it() {
        let f = Fixture::new();
        let term = f.term(true, 16);
        let index = f.append("k");

        let wait = f.wait(&term, index);
        settle().await;
        assert!(!wait.is_finished());
        term.record_replicated(index);

        wait.await.unwrap().unwrap();
        assert!(f.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn in_sync_slow_standby_goes_solo_when_the_witness_agrees() {
        let f = Fixture::new();
        let term = f.term(true, 16);
        let index = f.append("k");

        f.wait(&term, index).await.unwrap().unwrap();

        assert_eq!(term.peer_state(), PeerState::Solo);
        assert_eq!(f.requests(), vec![(false, 10)]);
        assert!(f.clock.now() >= TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn in_sync_slow_standby_fails_the_write_when_the_witness_refuses() {
        let f = Fixture::new();
        f.witness.with(|s| s.reachable = false);
        let term = f.term(true, 16);
        let index = f.append("k");

        let error = f.wait(&term, index).await.unwrap().unwrap_err();

        assert!(error.message().contains("witness"), "{error}");
        assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(term.peer_state(), PeerState::InSync);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_start_writes_wait_until_the_witness_agrees_to_solo() {
        let f = Fixture::new();
        let term = f.term(false, 16);
        assert_eq!(term.peer_state(), PeerState::CatchingUp);
        let index = f.append("k");

        let wait = f.wait(&term, index);
        settle().await;
        assert!(!wait.is_finished());
        assert!(term.try_go_solo().await);

        wait.await.unwrap().unwrap();
        assert_eq!(term.peer_state(), PeerState::Solo);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_writes_share_one_solo_request() {
        let f = Fixture::new();
        let term = f.term(true, 16);
        let first = f.append("a");
        let second = f.append("b");

        let waits = [f.wait(&term, first), f.wait(&term, second)];
        for wait in waits {
            wait.await.unwrap().unwrap();
        }

        assert_eq!(f.requests(), vec![(false, 10)]);
    }

    #[tokio::test(start_paused = true)]
    async fn solo_write_is_durable_right_away() {
        let f = Fixture::new();
        let term = f.solo_term(16).await;
        let index = f.append("k");

        term.wait_durable(index, TIMEOUT, &f.clock).await.unwrap();
        assert_eq!(f.clock.now(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn solo_catches_up_then_reports_in_sync_against_the_latest_version() {
        let f = Fixture::new();
        let term = f.solo_term(16).await; // witness version 10 -> 11
        f.append("a");
        f.append("b");

        assert!(term.record_replicated(2));
        assert_eq!(term.peer_state(), PeerState::CatchingUp);
        term.try_mark_in_sync(&CancellationToken::new()).await;

        assert_eq!(term.peer_state(), PeerState::InSync);
        assert_eq!(f.requests().last(), Some(&(true, 11)));
        assert!(f.witness.with(|s| s.standby_in_sync));
    }

    #[tokio::test(start_paused = true)]
    async fn in_sync_report_is_refused_when_the_witness_changed_in_between() {
        let f = Fixture::new();
        let term = f.solo_term(16).await;
        f.append("a");
        term.record_replicated(1);
        f.witness.with(|s| s.version += 5); // e.g. a late "not in sync" from an older process got there first

        term.try_mark_in_sync(&CancellationToken::new()).await;

        assert_eq!(term.peer_state(), PeerState::CatchingUp);
        assert!(!f.witness.with(|s| s.standby_in_sync));
    }

    #[tokio::test(start_paused = true)]
    async fn catching_up_new_writes_wait_for_the_standby_again() {
        let f = Fixture::new();
        let term = f.solo_term(16).await;
        f.append("a");
        term.record_replicated(0); // within the slack: catch-up target is index 1
        assert_eq!(term.peer_state(), PeerState::CatchingUp);
        let index = f.append("b");

        let wait = f.wait(&term, index);
        assert!(term.record_replicated(1));
        term.try_mark_in_sync(&CancellationToken::new()).await;
        assert_eq!(term.peer_state(), PeerState::InSync);
        settle().await;
        assert!(!wait.is_finished());
        term.record_replicated(2);

        wait.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn catching_up_does_not_report_before_the_target() {
        let f = Fixture::new();
        let term = f.solo_term(16).await;
        f.append("a");
        f.append("b");

        assert!(!term.record_replicated(1));
        term.try_mark_in_sync(&CancellationToken::new()).await;

        assert_eq!(term.peer_state(), PeerState::CatchingUp);
        assert!(f.requests().iter().all(|(in_sync, _)| !in_sync));
    }

    #[tokio::test(start_paused = true)]
    async fn solo_stays_solo_while_the_standby_is_far_behind() {
        let f = Fixture::new();
        let term = f.solo_term(1).await;
        f.append("a");
        f.append("b");
        f.append("c");

        assert!(!term.record_replicated(1));
        assert_eq!(term.peer_state(), PeerState::Solo);
    }

    #[tokio::test(start_paused = true)]
    async fn witness_saying_behind_sends_an_in_sync_standby_through_catching_up() {
        let f = Fixture::new();
        let term = f.term(true, 16);
        f.append("a");

        term.observe_witness(11, false); // a "go on alone" whose reply was lost

        assert_eq!(term.peer_state(), PeerState::CatchingUp);
        assert!(term.record_replicated(1));
    }

    #[tokio::test(start_paused = true)]
    async fn witness_replies_older_than_one_already_seen_are_ignored() {
        let f = Fixture::new();
        let term = f.term(true, 16);

        term.observe_witness(9, false);

        assert_eq!(term.peer_state(), PeerState::InSync);
    }

    #[tokio::test(start_paused = true)]
    async fn ended_term_fails_writes_still_waiting() {
        let f = Fixture::new();
        let term = f.term(true, 16);
        let index = f.append("k");

        let wait = f.wait(&term, index);
        settle().await;
        term.end();

        let error = wait.await.unwrap().unwrap_err();
        assert!(error.message().contains("lost the primary role"), "{error}");
        assert!(term.is_ended());
        assert!(!term.try_go_solo().await);
        assert!(f.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_in_sync_report_does_nothing() {
        let f = Fixture::new();
        let term = f.solo_term(16).await;
        term.record_replicated(0);
        let cancel = CancellationToken::new();
        cancel.cancel();

        term.try_mark_in_sync(&cancel).await;

        assert_eq!(term.peer_state(), PeerState::CatchingUp);
    }

    #[tokio::test(start_paused = true)]
    async fn append_signal_fires_on_the_next_append() {
        let f = Fixture::new();
        let term = f.term(true, 16);
        let mut signal = term.append_signal();

        assert!(!signal.has_changed().unwrap());
        term.notify_appended();

        signal.changed().await.unwrap();
        assert!(!term.append_signal().has_changed().unwrap());
    }

    #[test]
    fn peer_states_have_wire_names() {
        let names: Vec<&str> = [PeerState::InSync, PeerState::Solo, PeerState::CatchingUp]
            .into_iter()
            .map(PeerState::wire_name)
            .collect();
        assert_eq!(names, ["in-sync", "solo", "catching-up"]);
    }
}
