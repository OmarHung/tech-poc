use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde::{Deserialize, Serialize};

use crate::config::{WorkerConfig, WorkerMode};
use crate::hothot::ledger::Ledger;

/// Where a dispatched worker is with its current task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskPhase {
    Working,
    /// Writing the result to the sink.
    Delivering,
    /// Reporting the task done to the upstream.
    Completing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed,
    /// The lease was lost (or the sink unreachable for a whole lease) before the result was written.
    Abandoned,
    /// The result was written, but another attempt owned the task by the time it was reported.
    Stale,
}

/// What `GET /health` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerHealth {
    pub status: String,
    pub worker: String,
    pub mode: String,
    /// Hot-hot: the last event applied and saved.
    pub offset: i64,
    /// How the sink answered this worker's results, by verdict.
    pub sink_replies: BTreeMap<String, i64>,
    pub completed: i64,
    pub abandoned: i64,
    pub stale_completions: i64,
    pub current_task: Option<i64>,
    pub phase: Option<TaskPhase>,
}

/// Counters behind `GET /health`, updated by the work loop.
pub struct WorkerStatus {
    worker_id: String,
    mode: WorkerMode,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    sink_replies: BTreeMap<String, i64>,
    completed: i64,
    abandoned: i64,
    stale_completions: i64,
    current: Option<(i64, TaskPhase)>,
    ledger: Ledger,
}

impl WorkerStatus {
    pub fn new(config: &WorkerConfig) -> Self {
        Self {
            worker_id: config.worker_id.clone(),
            mode: config.mode,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn ledger(&self) -> Ledger {
        self.lock().ledger.clone()
    }

    pub fn set_ledger(&self, ledger: Ledger) {
        self.lock().ledger = ledger;
    }

    pub fn sink_replied(&self, status: &str) {
        *self
            .lock()
            .sink_replies
            .entry(status.to_owned())
            .or_default() += 1;
    }

    pub fn task_started(&self, id: i64) {
        self.task_phase(id, TaskPhase::Working);
    }

    pub fn task_phase(&self, id: i64, phase: TaskPhase) {
        self.lock().current = Some((id, phase));
    }

    pub fn task_finished(&self, outcome: TaskOutcome) {
        let mut inner = self.lock();
        inner.current = None;
        match outcome {
            TaskOutcome::Completed => inner.completed += 1,
            TaskOutcome::Abandoned => inner.abandoned += 1,
            TaskOutcome::Stale => inner.stale_completions += 1,
        }
    }

    pub fn health(&self) -> WorkerHealth {
        let inner = self.lock();
        WorkerHealth {
            status: "ok".into(),
            worker: self.worker_id.clone(),
            mode: self.mode.wire_name().into(),
            offset: inner.ledger.offset,
            sink_replies: inner.sink_replies.clone(),
            completed: inner.completed,
            abandoned: inner.abandoned,
            stale_completions: inner.stale_completions,
            current_task: inner.current.map(|(id, _)| id),
            phase: inner.current.map(|(_, phase)| phase),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Nothing panics while holding this lock.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn health_shows_the_current_task_and_the_counters() {
        let status = WorkerStatus::new(&WorkerConfig {
            worker_id: "b".into(),
            mode: WorkerMode::Dispatch,
            ..WorkerConfig::default()
        });

        status.task_started(7);
        let working = serde_json::to_value(status.health()).unwrap();
        status.task_phase(7, TaskPhase::Completing);
        let completing = status.health();
        status.sink_replied("applied");
        status.sink_replied("applied");
        status.sink_replied("stale");
        status.task_finished(TaskOutcome::Completed);
        status.task_finished(TaskOutcome::Abandoned);
        status.task_finished(TaskOutcome::Stale);

        assert_eq!(
            working,
            json!({"status": "ok", "worker": "b", "mode": "dispatch", "offset": 0, "sinkReplies": {},
                   "completed": 0, "abandoned": 0, "staleCompletions": 0, "currentTask": 7,
                   "phase": "working"})
        );
        assert_eq!(completing.phase, Some(TaskPhase::Completing));
        let done = status.health();
        assert_eq!(
            (done.completed, done.abandoned, done.stale_completions),
            (1, 1, 1)
        );
        assert_eq!((done.current_task, done.phase), (None, None));
        assert_eq!(
            done.sink_replies,
            BTreeMap::from([("applied".into(), 2), ("stale".into(), 1)])
        );
    }

    #[test]
    fn health_shows_the_hot_hot_offset() {
        let status = WorkerStatus::new(&WorkerConfig::default());

        status.set_ledger(Ledger {
            offset: 12,
            balances: BTreeMap::from([("x".into(), 3)]),
        });

        assert_eq!(status.health().offset, 12);
        assert_eq!(status.health().mode, "hothot");
        assert_eq!(status.ledger().balances["x"], 3);
    }
}
