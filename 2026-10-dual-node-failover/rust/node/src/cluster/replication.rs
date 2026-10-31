use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{info, warn};
use url::Url;

use super::MAX_BATCH;
use super::contracts::{ReplicateReply, ReplicateRequest, ReplicateStatus};
use super::node_state::NodeState;
use super::primary_term::{PeerState, PrimaryTerm};
use super::rpc::RpcClient;

/// While this node is primary, ships its log to the standby one batch at a time and heartbeats it when
/// idle. Every answer from the standby renews the peer lease and moves the term's match index.
pub struct ReplicationWorker {
    state: Arc<NodeState>,
    rpc: RpcClient,
    replicate_url: Url,
}

impl ReplicationWorker {
    pub fn new(state: Arc<NodeState>, rpc: RpcClient, replicate_url: Url) -> Self {
        Self {
            state,
            rpc,
            replicate_url,
        }
    }

    pub async fn run(self) {
        loop {
            let term = self.state.wait_for_term().await;
            self.replicate_term(&term).await;
            if !term.is_ended() {
                // Deposed but the step-down could not be completed (e.g. the epoch could not be saved):
                // try again shortly instead of spinning.
                tokio::time::sleep(self.retry_delay()).await;
            }
        }
    }

    /// Replicates until the term ends (which also cancels a request or wait in progress) or the
    /// standby reports a newer epoch.
    async fn replicate_term(&self, term: &Arc<PrimaryTerm>) {
        let ended = term.ended().clone();
        tokio::select! {
            biased;
            () = ended.cancelled() => {}
            () = self.replicate(term) => {}
        }
    }

    async fn replicate(&self, term: &Arc<PrimaryTerm>) {
        let config = self.state.config();
        let log = self.state.log();
        let clock = self.state.clock();
        let renew = config.renew_interval;
        let mut next_index = log.last_index() + 1; // optimistic; a mismatch walks it back
        let mut last_sent: Option<Duration> = None; // none yet: heartbeat right away
        let mut in_sync_call: Option<JoinHandle<()>> = None;
        if term.peer_state() != PeerState::InSync {
            // The witness says the standby is behind; see PrimaryTerm::try_go_solo.
            let term = Arc::clone(term);
            tokio::spawn(async move { term.try_go_solo().await });
        }
        loop {
            if term.is_ended() {
                return;
            }
            let mut appended = term.append_signal();
            let Some(slice) = log.read(next_index, MAX_BATCH) else {
                // The log only shrinks under a node that is no longer primary, i.e. the term has ended.
                if term.is_ended() {
                    info!(
                        "Log truncated under the deposed primary of epoch {}; replication stopped",
                        term.epoch()
                    );
                    return;
                }
                warn!(
                    "Standby matched past this log's end (index {}); starting over from the end",
                    next_index - 1
                );
                next_index = log.last_index() + 1;
                continue;
            };
            let idle = last_sent.map(|sent| clock.now().saturating_sub(sent));
            if let Some(idle) = idle
                && slice.entries.is_empty()
                && idle < renew
            {
                // Wait for a write, or until it is time for a heartbeat.
                let _ = tokio::time::timeout(renew - idle, appended.changed()).await;
                continue;
            }

            let sent_at = clock.now();
            last_sent = Some(sent_at);
            let request = ReplicateRequest {
                leader: Some(config.node_id.clone()),
                epoch: term.epoch(),
                prev_index: next_index - 1,
                prev_epoch: slice.prev_epoch,
                entries: Some(slice.entries),
                leader_last_index: slice.last_index,
            };
            let reply = match self.send(&request).await {
                Some(reply) if reply.status != ReplicateStatus::Rejected => reply,
                rejected => {
                    if let Some(reply) = rejected
                        && reply.epoch > term.epoch()
                    {
                        self.state.observe_epoch(reply.epoch, "standby reply");
                        return;
                    }
                    tokio::time::sleep(self.retry_delay()).await;
                    continue;
                }
            };

            // The standby accepted this node as primary and promised not to seek the lease for a while.
            let promise = Duration::from_millis(u64::try_from(reply.promise_ms).unwrap_or(0));
            let peer_lease_end = (sent_at + promise).saturating_sub(config.lease_safety_margin());
            self.state.extend_peer_lease(term, peer_lease_end);
            if reply.status == ReplicateStatus::Mismatch {
                next_index = (next_index - 1)
                    .min(reply.match_index.saturating_add(1))
                    .max(1);
                continue;
            }
            next_index = reply.match_index.saturating_add(1);
            if term.record_replicated(reply.match_index)
                && in_sync_call.as_ref().is_none_or(JoinHandle::is_finished)
            {
                // In the background: a slow witness must not hold up replication.
                let term = Arc::clone(term);
                in_sync_call = Some(tokio::spawn(async move {
                    let ended = term.ended().clone();
                    term.try_mark_in_sync(&ended).await;
                }));
            }
        }
    }

    async fn send(&self, request: &ReplicateRequest) -> Option<ReplicateReply> {
        let response = self.rpc.post(&self.replicate_url, request).await?;
        if !response.status.is_success() {
            warn!(
                "Standby answered replication with {}",
                response.status.as_u16()
            );
            return None;
        }
        response.json()
    }

    fn retry_delay(&self) -> Duration {
        self.state.config().renew_interval / 5
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use tempfile::TempDir;

    use super::*;
    use crate::cluster::witness_client::NoArbiter;
    use crate::config::NodeConfig;
    use crate::storage::replicated_log::ReplicatedLog;

    type Script = Arc<Mutex<Vec<(StatusCode, serde_json::Value)>>>;

    /// A standby that answers from a script (then repeats its last answer) and records what it got.
    struct FakeStandby {
        url: Url,
        script: Script,
        seen: Arc<Mutex<Vec<ReplicateRequest>>>,
    }

    async fn fake_standby(script: Vec<(StatusCode, serde_json::Value)>) -> FakeStandby {
        let script: Script = Arc::new(Mutex::new(script));
        let seen: Arc<Mutex<Vec<ReplicateRequest>>> = Arc::default();
        let app = Router::new()
            .route(
                "/internal/replicate",
                post(
                    |State((script, seen)): State<(Script, Arc<Mutex<Vec<ReplicateRequest>>>)>,
                     Json(request): Json<ReplicateRequest>| async move {
                        seen.lock().unwrap().push(request);
                        let mut script = script.lock().unwrap();
                        let (status, body) = if script.len() > 1 {
                            script.remove(0)
                        } else {
                            script[0].clone()
                        };
                        (status, Json(body)).into_response()
                    },
                ),
            )
            .with_state((Arc::clone(&script), Arc::clone(&seen)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        FakeStandby {
            url: Url::parse(&format!("http://{address}/internal/replicate")).unwrap(),
            script,
            seen,
        }
    }

    fn reply(status: &str, epoch: i64, match_index: i64) -> (StatusCode, serde_json::Value) {
        let promise = if status == "rejected" { 0 } else { 600 };
        (
            StatusCode::OK,
            serde_json::json!({"status": status, "epoch": epoch, "matchIndex": match_index, "promiseMs": promise, "reason": null}),
        )
    }

    fn primary(dir: &TempDir, entries: usize) -> Arc<NodeState> {
        let config = NodeConfig {
            data_dir: dir.path().to_owned(),
            lease_ttl: Duration::from_millis(600),
            renew_interval: Duration::from_millis(100),
            replication_timeout: Duration::from_millis(200),
            ..NodeConfig::default()
        };
        let log = Arc::new(ReplicatedLog::open(&dir.path().join("log.jsonl")).unwrap());
        for i in 0..entries {
            log.append(1, &format!("k{i}"), "v").unwrap();
        }
        Arc::new(NodeState::new(config, log, Arc::new(NoArbiter)).unwrap())
    }

    fn start(state: &Arc<NodeState>, standby: &FakeStandby) -> JoinHandle<()> {
        let rpc = RpcClient::new(Duration::from_millis(500)).unwrap();
        tokio::spawn(ReplicationWorker::new(Arc::clone(state), rpc, standby.url.clone()).run())
    }

    async fn wait_until(what: &str, condition: impl Fn() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test]
    async fn walks_back_on_mismatch_then_ships_the_log_and_marks_the_standby_in_sync() {
        let dir = tempfile::tempdir().unwrap();
        let state = primary(&dir, 3);
        let standby = fake_standby(vec![reply("mismatch", 2, 1), reply("ok", 2, 3)]).await;
        state.try_promote(2, Duration::MAX, false, 0);
        let term = state.current_term().unwrap();
        let worker = start(&state, &standby);

        wait_until("the standby to be in sync", || {
            term.peer_state() == PeerState::InSync
        })
        .await;
        worker.abort();

        let seen = standby.seen.lock().unwrap();
        assert_eq!(
            (seen[0].prev_index, seen[0].entries.as_ref().map(Vec::len)),
            (3, Some(0))
        );
        assert_eq!(
            (seen[1].prev_index, seen[1].entries.as_ref().map(Vec::len)),
            (1, Some(2))
        );
        assert_eq!(seen[1].leader_last_index, 3);
        assert_eq!(term.match_index(), 3);
        assert!(state.health().lease_remaining_ms.is_some());
    }

    #[tokio::test]
    async fn a_newer_epoch_from_the_standby_deposes_this_primary() {
        let dir = tempfile::tempdir().unwrap();
        let state = primary(&dir, 0);
        let standby = fake_standby(vec![reply("rejected", 5, 0)]).await;
        state.try_promote(2, Duration::MAX, true, 0);
        let term = state.current_term().unwrap();
        let worker = start(&state, &standby);

        wait_until("the term to end", || term.is_ended()).await;
        worker.abort();

        assert_eq!(state.epoch(), 5);
        assert!(state.current_term().is_none());
    }

    #[tokio::test]
    async fn rejections_and_errors_are_retried_and_heartbeats_continue() {
        let dir = tempfile::tempdir().unwrap();
        let state = primary(&dir, 0);
        let standby = fake_standby(vec![
            reply("rejected", 2, 0),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"error": "x"}),
            ),
            (StatusCode::OK, serde_json::json!("not a reply")),
            reply("ok", 2, 0),
        ])
        .await;
        state.try_promote(2, Duration::MAX, true, 0);
        let worker = start(&state, &standby);

        wait_until("heartbeats after the failures", || {
            standby.seen.lock().unwrap().len() >= 6
        })
        .await;
        worker.abort();

        assert!(state.current_term().is_some());
        assert_eq!(standby.script.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn new_writes_are_shipped_as_they_arrive() {
        let dir = tempfile::tempdir().unwrap();
        let state = primary(&dir, 0);
        let standby = fake_standby(vec![reply("ok", 2, 0), reply("ok", 2, 1)]).await;
        state.try_promote(2, Duration::MAX, true, 0);
        let term = state.current_term().unwrap();
        let worker = start(&state, &standby);
        wait_until("the first heartbeat", || {
            !standby.seen.lock().unwrap().is_empty()
        })
        .await;

        state.append_as_primary("k", "v").unwrap();

        wait_until("the write to replicate", || term.match_index() == 1).await;
        worker.abort();
        let seen = standby.seen.lock().unwrap();
        assert_eq!(
            seen[1].entries.as_ref().map(|e| e[0].key.clone()),
            Some("k".into())
        );
    }

    #[tokio::test]
    async fn a_truncated_log_under_a_deposed_primary_ends_the_term_quietly() {
        let dir = tempfile::tempdir().unwrap();
        let state = primary(&dir, 2);
        let standby = fake_standby(vec![reply("ok", 2, 2)]).await;
        state.try_promote(2, Duration::MAX, true, 0);
        let term = state.current_term().unwrap();
        let worker = start(&state, &standby);
        wait_until("replication", || term.match_index() == 2).await;

        // A newer primary deposes this one and truncates its log.
        let request = ReplicateRequest {
            leader: Some("b".into()),
            epoch: 3,
            prev_index: 1,
            prev_epoch: 1,
            entries: Some(vec![]),
            leader_last_index: 1,
        };
        state.handle_replicate(&request).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(term.is_ended());
        assert!(
            !worker.is_finished(),
            "the worker waits for the next term instead of crashing"
        );
        worker.abort();
    }
}
