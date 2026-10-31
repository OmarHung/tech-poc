use std::sync::Arc;
use std::time::Duration;

use shared::clock::MonotonicClock;
use shared::work::{ResultRecord, TaskLease};
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep};
use tracing::{info, warn};

use super::lease_tracker::{LeaseTracker, ttl};
use crate::client::{LeaseReply, WorkClient};
use crate::config::{RETRY_DELAY, WorkerConfig};
use crate::status::{TaskOutcome, TaskPhase, WorkerStatus};

/// How writing a task's result ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Written,
    /// The lease was lost first; the task is someone else's now.
    LeaseLost,
    /// The sink could not be reached for a whole lease, so the task was given up.
    SinkDown,
}

/// Takes one task at a time from the upstream queue, keeps the lease alive with heartbeats while
/// working, writes the result to the sink, then reports the task done. Writing before reporting makes
/// delivery at-least-once: a crash in between means another worker does the task again, and the sink
/// sees a second copy.
pub struct DispatchWorker {
    client: Arc<WorkClient>,
    status: Arc<WorkerStatus>,
    config: Arc<WorkerConfig>,
    clock: MonotonicClock,
}

impl DispatchWorker {
    pub fn new(
        client: Arc<WorkClient>,
        status: Arc<WorkerStatus>,
        config: Arc<WorkerConfig>,
    ) -> Self {
        Self {
            client,
            status,
            config,
            clock: MonotonicClock::new(),
        }
    }

    pub async fn run(self) {
        info!(
            "Dispatched worker {} starting; lease check {}",
            self.config.worker_id,
            if self.config.lease_check { "on" } else { "off" }
        );
        let mut sink_down = false;
        loop {
            if sink_down {
                if !self.sink_back().await {
                    continue;
                }
                sink_down = false;
            }
            let sent_at = self.clock.now();
            match self.client.lease_task().await {
                LeaseReply::Leased(lease) => {
                    let tracker =
                        LeaseTracker::new(self.clock, LeaseTracker::end(sent_at, lease.ttl_ms));
                    // Only giving a task up for an unreachable sink trips the breaker.
                    sink_down = self.run_task(&lease, &tracker).await == Delivery::SinkDown;
                }
                // The upstream already waited for a task; ask again right away.
                LeaseReply::NoTask => {}
                // Refused or reset: do not spin.
                LeaseReply::Unreachable => sleep(RETRY_DELAY).await,
            }
        }
    }

    /// Circuit breaker after giving a task up for an unreachable sink: lease nothing until the sink
    /// answers again. Otherwise this worker keeps taking tasks it cannot finish, and with no backlog it is
    /// the longest waiter, so it may get back the very task it just let go and stall it again.
    async fn sink_back(&self) -> bool {
        if self.client.sink_reachable().await {
            info!("Sink reachable again; taking tasks");
            return true;
        }
        sleep(RETRY_DELAY).await;
        false
    }

    /// Works on the task with heartbeats running alongside, in the same task: when the work is done
    /// (or this loop is aborted) the heartbeats stop with it, and a panic in either reaches the supervisor.
    async fn run_task(&self, lease: &TaskLease, tracker: &LeaseTracker) -> Delivery {
        self.status.task_started(lease.id);
        let work = self.work(lease, tracker);
        let heartbeats = self.heartbeat(lease, tracker);
        tokio::pin!(work, heartbeats);
        let mut heartbeating = true;
        loop {
            tokio::select! {
                biased;
                delivery = &mut work => return delivery,
                // Heartbeats end once the upstream says the lease is lost; the work notices through the tracker.
                () = &mut heartbeats, if heartbeating => heartbeating = false,
            }
        }
    }

    async fn work(&self, lease: &TaskLease, tracker: &LeaseTracker) -> Delivery {
        sleep(self.config.work_time).await; // the work itself
        if !self.still_holds(lease, tracker, "before writing the result") {
            return Delivery::LeaseLost;
        }
        self.status.task_phase(lease.id, TaskPhase::Delivering);
        let result = ResultRecord {
            key: Some(format!("task-{}", lease.id)),
            node: Some(self.config.worker_id.clone()),
            attempt: lease.attempt,
            value: Some(compute(lease.input)),
        };
        let delivery = self.deliver(lease, tracker, &result).await;
        if delivery != Delivery::Written {
            return delivery;
        }
        self.status.task_phase(lease.id, TaskPhase::Completing);
        let outcome = if self.complete(lease, tracker).await {
            TaskOutcome::Completed
        } else {
            TaskOutcome::Stale
        };
        self.status.task_finished(outcome);
        Delivery::Written
    }

    fn still_holds(&self, lease: &TaskLease, tracker: &LeaseTracker, when: &str) -> bool {
        if !self.config.lease_check || tracker.is_valid() {
            return true;
        }
        warn!(
            "Gave up task {} attempt {} {when}: lease lost",
            lease.id, lease.attempt
        );
        self.status.task_finished(TaskOutcome::Abandoned);
        false
    }

    /// Retries until the sink answers. With the lease check on, each try is cut off when the lease
    /// runs out, so a write cannot start late; only one already on the wire can still land after it.
    /// A worker that cannot reach the sink for a whole lease gives the task up (heartbeats stop with
    /// it), so the other worker can get it, and takes no more tasks until the sink is back.
    async fn deliver(
        &self,
        lease: &TaskLease,
        tracker: &LeaseTracker,
        result: &ResultRecord,
    ) -> Delivery {
        let give_up_at = self.clock.now() + ttl(lease.ttl_ms);
        loop {
            let timeout = if self.config.lease_check {
                self.config.rpc_timeout.min(tracker.remaining())
            } else {
                self.config.rpc_timeout
            };
            if timeout.is_zero() {
                // Only with the lease check on and nothing left of the lease (no await since it was
                // read, so no heartbeat can have extended it): still_holds() gives the task up.
                self.still_holds(lease, tracker, "while writing the result");
                return Delivery::LeaseLost;
            }
            if let Some(reply) = self.client.deliver(result, Some(timeout)).await {
                self.status.sink_replied(&reply);
                return Delivery::Written;
            }
            if self.clock.now() >= give_up_at {
                warn!(
                    "Gave up task {} attempt {}: sink unreachable for a whole lease",
                    lease.id, lease.attempt
                );
                self.status.task_finished(TaskOutcome::Abandoned);
                return Delivery::SinkDown;
            }
            sleep(RETRY_DELAY).await;
            if !self.still_holds(lease, tracker, "while the sink was unreachable") {
                return Delivery::LeaseLost;
            }
        }
    }

    /// Keeps reporting until the upstream answers. False means another attempt owns the task (it will
    /// run again).
    async fn complete(&self, lease: &TaskLease, tracker: &LeaseTracker) -> bool {
        let completed = loop {
            if let Some(completed) = self.client.complete(lease).await {
                break completed;
            }
            sleep(RETRY_DELAY).await;
        };
        if !completed {
            warn!(
                "Task {} attempt {} was reassigned before it was reported done",
                lease.id, lease.attempt
            );
            tracker.revoke();
        }
        completed
    }

    /// Heartbeats on a fixed schedule, each bounded by the interval, so one hung call cannot push the
    /// next one past the lease. The lease is extended from each heartbeat's send time. Returns once
    /// the upstream says the lease is lost.
    async fn heartbeat(&self, lease: &TaskLease, tracker: &LeaseTracker) {
        let every = (ttl(lease.ttl_ms) / 3).max(Duration::from_millis(1));
        let mut timer = interval_at(Instant::now() + every, every);
        // Like .NET's PeriodicTimer: a tick missed during a slow call fires once, then the schedule resumes.
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            let sent_at = self.clock.now();
            match self.client.heartbeat(lease, every).await {
                Some(true) => tracker.extend_to(LeaseTracker::end(sent_at, lease.ttl_ms)),
                Some(false) => {
                    tracker.revoke();
                    return;
                }
                None => {}
            }
        }
    }
}

/// The task's result. Deterministic, so a second run of the same task writes the same value.
pub fn compute(input: i64) -> String {
    // Wraps like .NET's unchecked arithmetic; the upstream keeps inputs small enough not to.
    format!("{input}^2={}", input.wrapping_mul(input))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_task_result_is_deterministic() {
        assert_eq!(compute(12), "12^2=144");
        assert_eq!(compute(-3), "-3^2=9");
        assert_eq!(compute(3_000_000_000), "3000000000^2=9000000000000000000");
    }
}
