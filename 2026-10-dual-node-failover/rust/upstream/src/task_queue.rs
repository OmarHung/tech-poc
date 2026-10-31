use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shared::clock::MonotonicClock;
use shared::error::ApiError;
use shared::wire::is_valid_node_id;
use shared::work::{TaskAttempt, TaskLease};
use tokio::sync::watch;
use tracing::warn;

/// Inputs are squared by the workers; this keeps the square inside an i64.
pub const MAX_INPUT: i64 = 3_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseView {
    pub id: i64,
    pub worker: String,
    pub attempt: i32,
}

/// What `GET /tasks/stats` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStats {
    pub total: i64,
    pub queued: i64,
    pub leased: i64,
    pub done: i64,
    pub reassignments: i64,
    pub stale_completions: i64,
    pub leases: Vec<LeaseView>,
}

/// The dispatcher for worker-style redundancy. Every task is leased to one worker at a time; a worker
/// that stops heartbeating loses the lease and the task goes back to the front of the queue with a
/// higher attempt number. Workers never talk to each other: the queue is the only thing that knows who
/// does what.
///
/// Delivery is at-least-once: a worker that did the work but died before reporting it, or that was
/// frozen past its lease, means the task runs twice.
pub struct TaskQueue {
    clock: MonotonicClock,
    ttl: Duration,
    inner: Mutex<Inner>,
    /// Bumped, under the lock, whenever a task becomes available: added or requeued.
    available: watch::Sender<u64>,
}

#[derive(Default)]
struct Inner {
    tasks: BTreeMap<i64, Item>,
    pending: VecDeque<i64>,
    reassignments: i64,
    stale_completions: i64,
}

struct Item {
    id: i64,
    input: i64,
    attempt: i32,
    worker: Option<String>,
    /// `None` when nobody holds the task.
    lease_until: Option<Duration>,
    done: bool,
}

impl TaskQueue {
    pub fn new(ttl: Duration) -> Self {
        Self {
            clock: MonotonicClock::new(),
            ttl,
            inner: Mutex::new(Inner::default()),
            available: watch::Sender::new(0),
        }
    }

    pub fn add(&self, input: i64) -> Result<i64, ApiError> {
        if !(-MAX_INPUT..=MAX_INPUT).contains(&input) {
            return Err(ApiError::InvalidInput(format!(
                "input must be between -{MAX_INPUT} and {MAX_INPUT}"
            )));
        }
        let mut inner = self.lock();
        let id = i64::try_from(inner.tasks.len()).unwrap_or(i64::MAX) + 1;
        inner.tasks.insert(
            id,
            Item {
                id,
                input,
                attempt: 0,
                worker: None,
                lease_until: None,
                done: false,
            },
        );
        inner.pending.push_back(id);
        self.pulse();
        Ok(id)
    }

    /// Hands the oldest waiting task to `worker`, waiting up to `wait` for one.
    pub async fn lease(
        &self,
        worker: Option<&str>,
        wait: Duration,
    ) -> Result<Option<TaskLease>, ApiError> {
        let worker = require_worker(worker)?;
        let deadline = self.clock.now() + wait;
        loop {
            let mut available = {
                let mut inner = self.lock();
                self.expire_locked(&mut inner);
                if let Some(lease) = self.lease_first_locked(&mut inner, worker) {
                    return Ok(Some(lease));
                }
                // Taken under the lock: a task added or requeued after this point wakes the wait below.
                self.available.subscribe()
            };
            let remaining = deadline.saturating_sub(self.clock.now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, available.changed())
                    .await
                    .is_err()
            {
                return Ok(None);
            }
        }
    }

    /// Extends the lease if `request` still holds it.
    pub fn heartbeat(&self, id: i64, request: &TaskAttempt) -> Result<bool, ApiError> {
        require_worker(request.worker.as_deref())?;
        let mut inner = self.lock();
        self.expire_locked(&mut inner);
        if !holds(&inner, id, request)? {
            return Ok(false);
        }
        let until = self.clock.now() + self.ttl;
        if let Some(item) = inner.tasks.get_mut(&id) {
            item.lease_until = Some(until);
        }
        Ok(true)
    }

    /// Marks the task done if `request` still holds it; anything else is a stale completion.
    pub fn complete(&self, id: i64, request: &TaskAttempt) -> Result<bool, ApiError> {
        require_worker(request.worker.as_deref())?;
        let mut inner = self.lock();
        self.expire_locked(&mut inner);
        let repeated = inner.tasks.get(&id).is_some_and(|item| {
            item.done && item.worker == request.worker && item.attempt == request.attempt
        });
        if repeated {
            return Ok(true); // the same report again, e.g. a retry after its reply was lost
        }
        if !holds(&inner, id, request)? {
            inner.stale_completions += 1;
            warn!(
                "Stale completion of task {id} attempt {} by {}",
                request.attempt,
                request.worker.as_deref().unwrap_or("")
            );
            return Ok(false);
        }
        if let Some(item) = inner.tasks.get_mut(&id) {
            item.done = true;
            item.lease_until = None;
        }
        Ok(true)
    }

    /// Puts every task whose lease ran out back at the front of the queue. Called on every access and
    /// by the reaper.
    pub fn expire_overdue(&self) {
        let mut inner = self.lock();
        self.expire_locked(&mut inner);
    }

    pub fn stats(&self) -> TaskStats {
        let inner = self.lock();
        let leases: Vec<LeaseView> = inner
            .tasks
            .values()
            .filter(|t| !t.done && t.lease_until.is_some())
            .filter_map(|t| {
                t.worker.as_ref().map(|worker| LeaseView {
                    id: t.id,
                    worker: worker.clone(),
                    attempt: t.attempt,
                })
            })
            .collect();
        TaskStats {
            total: count(inner.tasks.len()),
            queued: count(inner.pending.len()),
            leased: count(leases.len()),
            done: count(inner.tasks.values().filter(|t| t.done).count()),
            reassignments: inner.reassignments,
            stale_completions: inner.stale_completions,
            leases,
        }
    }

    fn lease_first_locked(&self, inner: &mut Inner, worker: &str) -> Option<TaskLease> {
        while let Some(id) = inner.pending.pop_front() {
            // Every queued id is a known task; skip one that is not rather than hand out nothing.
            let Some(item) = inner.tasks.get_mut(&id) else {
                continue;
            };
            item.attempt = item.attempt.saturating_add(1);
            item.worker = Some(worker.to_owned());
            item.lease_until = Some(self.clock.now() + self.ttl);
            return Some(TaskLease {
                id,
                input: item.input,
                attempt: item.attempt,
                ttl_ms: i32::try_from(self.ttl.as_millis()).unwrap_or(i32::MAX),
            });
        }
        None
    }

    fn expire_locked(&self, inner: &mut Inner) {
        let now = self.clock.now();
        let expired: Vec<i64> = inner
            .tasks
            .values()
            .filter(|t| !t.done && t.lease_until.is_some_and(|until| until <= now))
            .map(|t| t.id)
            .collect();
        // Highest id first, so after pushing each to the front the oldest task is first in line.
        for id in expired.iter().rev() {
            if let Some(item) = inner.tasks.get_mut(id) {
                warn!(
                    "Lease on task {id} attempt {} by {} expired; requeued",
                    item.attempt,
                    item.worker.as_deref().unwrap_or("")
                );
                item.lease_until = None;
                item.worker = None;
            }
            inner.pending.push_front(*id); // retried work goes first: it has waited longest
            inner.reassignments += 1;
        }
        if !expired.is_empty() {
            self.pulse();
        }
    }

    fn pulse(&self) {
        self.available
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Nothing panics while holding this lock.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Whether `request` holds the lease on task `id` right now. An unknown task is a 404.
fn holds(inner: &Inner, id: i64, request: &TaskAttempt) -> Result<bool, ApiError> {
    let item = inner
        .tasks
        .get(&id)
        .ok_or_else(|| ApiError::NotFound(format!("task {id} not found")))?;
    Ok(!item.done
        && item.worker == request.worker
        && item.attempt == request.attempt
        && item.lease_until.is_some())
}

fn require_worker(worker: Option<&str>) -> Result<&str, ApiError> {
    worker.filter(|w| is_valid_node_id(w)).ok_or_else(|| {
        ApiError::InvalidInput("worker must be 1-32 lowercase letters, digits or '-'".into())
    })
}

fn count(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::advance;

    use super::*;

    const TTL: Duration = Duration::from_secs(2);

    fn attempt(worker: &str, attempt: i32) -> TaskAttempt {
        TaskAttempt {
            worker: Some(worker.into()),
            attempt,
        }
    }

    fn lease(id: i64, input: i64, attempt: i32) -> TaskLease {
        TaskLease {
            id,
            input,
            attempt,
            ttl_ms: 2000,
        }
    }

    async fn lease_now(queue: &TaskQueue, worker: &str) -> Option<TaskLease> {
        queue.lease(Some(worker), Duration::ZERO).await.unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn hands_out_tasks_in_order() {
        let queue = TaskQueue::new(TTL);
        assert_eq!(queue.add(10), Ok(1));
        assert_eq!(queue.add(20), Ok(2));

        let first = lease_now(&queue, "a").await;
        let second = lease_now(&queue, "b").await;

        assert_eq!(first, Some(lease(1, 10, 1)));
        assert_eq!(second, Some(lease(2, 20, 1)));
        assert_eq!(lease_now(&queue, "a").await, None);
        let stats = queue.stats();
        assert_eq!((stats.total, stats.queued, stats.leased), (2, 0, 2));
        assert_eq!(
            stats.leases,
            vec![
                LeaseView {
                    id: 1,
                    worker: "a".into(),
                    attempt: 1
                },
                LeaseView {
                    id: 2,
                    worker: "b".into(),
                    attempt: 1
                }
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_worker_gets_the_next_task() {
        let queue = Arc::new(TaskQueue::new(TTL));
        let waiter = Arc::clone(&queue);
        let waiting =
            tokio::spawn(async move { waiter.lease(Some("a"), Duration::from_secs(1)).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        queue.add(7).unwrap();

        assert_eq!(waiting.await.unwrap().unwrap(), Some(lease(1, 7, 1)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_worker_gives_up_after_the_wait() {
        let queue = TaskQueue::new(TTL);
        let started = tokio::time::Instant::now();

        let leased = queue
            .lease(Some("a"), Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(leased, None);
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_worker_is_woken_by_an_expired_lease() {
        let queue = Arc::new(TaskQueue::new(TTL));
        queue.add(1).unwrap();
        lease_now(&queue, "a").await.unwrap();
        let waiter = Arc::clone(&queue);
        let waiting =
            tokio::spawn(async move { waiter.lease(Some("b"), Duration::from_secs(5)).await });
        tokio::task::yield_now().await;

        advance(TTL).await;
        queue.expire_overdue(); // what the reaper does

        assert_eq!(waiting.await.unwrap().unwrap(), Some(lease(1, 1, 2)));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_keep_the_lease() {
        let queue = TaskQueue::new(TTL);
        queue.add(1).unwrap();
        let leased = lease_now(&queue, "a").await.unwrap();

        advance(TTL * 3 / 4).await;
        assert_eq!(queue.heartbeat(leased.id, &attempt("a", 1)), Ok(true));
        advance(TTL * 3 / 4).await;

        assert_eq!(queue.complete(leased.id, &attempt("a", 1)), Ok(true));
        let stats = queue.stats();
        assert_eq!((stats.done, stats.leased, stats.reassignments), (1, 0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_lease_goes_back_to_the_front_with_the_next_attempt() {
        let queue = TaskQueue::new(TTL);
        queue.add(1).unwrap();
        queue.add(2).unwrap();
        queue.add(3).unwrap();
        lease_now(&queue, "a").await.unwrap();
        lease_now(&queue, "a").await.unwrap();

        advance(TTL).await;
        let retried = [
            lease_now(&queue, "b").await,
            lease_now(&queue, "b").await,
            lease_now(&queue, "b").await,
        ];

        assert_eq!(
            retried,
            [
                Some(lease(1, 1, 2)),
                Some(lease(2, 2, 2)),
                Some(lease(3, 3, 1))
            ]
        );
        assert_eq!(queue.stats().reassignments, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_lease_can_neither_heartbeat_nor_complete() {
        let queue = TaskQueue::new(TTL);
        queue.add(1).unwrap();
        lease_now(&queue, "a").await.unwrap();
        advance(TTL).await;
        lease_now(&queue, "b").await.unwrap();

        assert_eq!(queue.heartbeat(1, &attempt("a", 1)), Ok(false));
        assert_eq!(queue.complete(1, &attempt("a", 1)), Ok(false));
        assert_eq!(queue.complete(1, &attempt("b", 2)), Ok(true));
        assert_eq!(queue.complete(1, &attempt("a", 1)), Ok(false));
        assert_eq!(queue.heartbeat(1, &attempt("b", 2)), Ok(false)); // done

        let stats = queue.stats();
        assert_eq!((stats.stale_completions, stats.done), (2, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn the_same_worker_with_an_older_attempt_is_stale() {
        let queue = TaskQueue::new(TTL);
        queue.add(1).unwrap();
        lease_now(&queue, "a").await.unwrap();
        advance(TTL).await;
        lease_now(&queue, "a").await.unwrap();

        assert_eq!(queue.complete(1, &attempt("a", 1)), Ok(false));
        assert_eq!(queue.complete(1, &attempt("a", 2)), Ok(true));
        assert_eq!(queue.stats().stale_completions, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn expire_overdue_requeues_without_anyone_asking() {
        let queue = TaskQueue::new(TTL);
        queue.add(1).unwrap();
        lease_now(&queue, "a").await.unwrap();

        advance(TTL - Duration::from_millis(1)).await;
        queue.expire_overdue();
        assert_eq!(queue.stats().leased, 1);
        advance(Duration::from_millis(1)).await;
        queue.expire_overdue();

        let stats = queue.stats();
        assert_eq!((stats.queued, stats.leased), (1, 0));
        assert!(stats.leases.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_repeated_completion_by_the_same_attempt_is_not_stale() {
        let queue = TaskQueue::new(TTL);
        queue.add(1).unwrap();
        lease_now(&queue, "a").await.unwrap();

        assert_eq!(queue.complete(1, &attempt("a", 1)), Ok(true));
        assert_eq!(queue.complete(1, &attempt("a", 1)), Ok(true)); // a retry whose first reply was lost
        advance(TTL * 2).await;
        assert_eq!(queue.complete(1, &attempt("a", 1)), Ok(true)); // done tasks never expire

        let stats = queue.stats();
        assert_eq!((stats.stale_completions, stats.reassignments), (0, 0));
    }

    #[test]
    fn inputs_whose_square_would_overflow_are_rejected() {
        let queue = TaskQueue::new(TTL);

        assert!(queue.add(MAX_INPUT).is_ok());
        assert!(queue.add(-MAX_INPUT).is_ok());
        for bad in [MAX_INPUT + 1, -MAX_INPUT - 1, i64::MIN, i64::MAX] {
            let error = queue.add(bad).unwrap_err();
            assert_eq!(
                error.message(),
                "input must be between -3000000000 and 3000000000"
            );
        }
        assert_eq!(queue.stats().total, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_tasks_and_bad_workers_are_rejected() {
        let queue = TaskQueue::new(TTL);

        assert!(matches!(
            queue.complete(99, &attempt("a", 1)),
            Err(ApiError::NotFound(_))
        ));
        assert!(matches!(
            queue.heartbeat(99, &attempt("a", 1)),
            Err(ApiError::NotFound(_))
        ));
        for bad in [None, Some(""), Some("Bad Worker"), Some("a_b")] {
            let request = TaskAttempt {
                worker: bad.map(str::to_owned),
                attempt: 1,
            };
            assert!(matches!(
                queue.heartbeat(1, &request),
                Err(ApiError::InvalidInput(_))
            ));
            assert!(matches!(
                queue.complete(1, &request),
                Err(ApiError::InvalidInput(_))
            ));
            assert!(matches!(
                queue.lease(bad, Duration::ZERO).await,
                Err(ApiError::InvalidInput(_))
            ));
        }
        assert_eq!(queue.stats().stale_completions, 0);
    }
}
