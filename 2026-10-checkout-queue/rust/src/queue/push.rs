//! Feeds this instance's event streams. Admissions arrive over Redis pub/sub and are pushed at once;
//! everything else (position, wait estimate) is refreshed once per poll interval in one batched
//! call, so a stream never sends more often than polling would.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinSet;

use crate::clock::Clock;
use crate::sale_redis::{AdmittedSubscription, SaleRedis, StoreError};

use super::{QueueStore, TicketStreams, TicketView};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Work {
    /// The poll interval passed: refresh every open stream.
    RefreshAll,
    /// admit.lua just let these tickets in.
    Admitted(Vec<String>),
}

/// The single consumer: works out what changed and hands it to the streams.
pub struct TicketPushWorker {
    queue: QueueStore,
    streams: Arc<TicketStreams>,
    /// Last view pushed per ticket. Only this worker touches it.
    sent: HashMap<String, TicketView>,
}

impl TicketPushWorker {
    pub fn new(queue: QueueStore, streams: Arc<TicketStreams>) -> Self {
        Self {
            queue,
            streams,
            sent: HashMap::new(),
        }
    }

    pub async fn push(&mut self, work: Work) -> Result<(), StoreError> {
        let open: HashSet<String> = self.streams.ticket_ids().into_iter().collect();
        self.sent.retain(|ticket_id, _| open.contains(ticket_id));
        let wanted: Vec<String> = match work {
            Work::RefreshAll => open.into_iter().collect(),
            Work::Admitted(ids) => ids.into_iter().filter(|id| open.contains(id)).collect(),
        };
        if wanted.is_empty() {
            return Ok(());
        }
        for view in self.queue.watch(&wanted).await? {
            if self.sent.get(&view.ticket_id) == Some(&view) {
                continue;
            }
            self.streams.publish(&view);
            self.sent.insert(view.ticket_id.clone(), view);
        }
        Ok(())
    }
}

/// Subscribes to admissions first (so none is missed once requests are served), then starts the
/// subscription listener, the refresh timer and the consumer.
pub async fn start(
    redis: &SaleRedis,
    worker: TicketPushWorker,
    poll_interval: Duration,
    tasks: &mut JoinSet<Result<(), StoreError>>,
) {
    let (work, inbox) = mpsc::unbounded_channel();
    let subscription = subscribe(redis).await;
    tasks.spawn(listen_for_admissions(
        redis.clone(),
        subscription,
        work.clone(),
        poll_interval,
    ));
    tasks.spawn(refresh_every(
        Arc::clone(redis.clock()),
        poll_interval,
        work,
    ));
    tasks.spawn(consume(worker, inbox));
}

async fn subscribe(redis: &SaleRedis) -> Option<AdmittedSubscription> {
    match redis.subscribe_admitted().await {
        Ok(subscription) => Some(subscription),
        Err(error) => {
            tracing::warn!(
                "subscribing to admissions failed ({error}); streams fall back to the periodic refresh"
            );
            None
        }
    }
}

/// Forwards admitted ids. When the subscription drops or never came up, retries every poll interval;
/// meanwhile the periodic refresh still delivers admissions, only later.
async fn listen_for_admissions(
    redis: SaleRedis,
    mut subscription: Option<AdmittedSubscription>,
    work: UnboundedSender<Work>,
    retry_every: Duration,
) -> Result<(), StoreError> {
    loop {
        if let Some(mut live) = subscription.take() {
            while let Some(message) = live.messages.next().await {
                let ids = String::from_utf8_lossy(message.get_payload_bytes())
                    .split(' ')
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .collect();
                if work.send(Work::Admitted(ids)).is_err() {
                    return Ok(());
                }
            }
            tracing::warn!("subscription to admissions was lost; resubscribing");
        }
        redis.clock().sleep(retry_every).await;
        subscription = subscribe(&redis).await;
    }
}

async fn refresh_every(
    clock: Arc<dyn Clock>,
    period: Duration,
    work: UnboundedSender<Work>,
) -> Result<(), StoreError> {
    loop {
        clock.sleep(period).await;
        if work.send(Work::RefreshAll).is_err() {
            return Ok(());
        }
    }
}

/// Redis errors are logged and the next refresh tries again; anything else is a bug and stops the process.
async fn consume(
    mut worker: TicketPushWorker,
    mut inbox: UnboundedReceiver<Work>,
) -> Result<(), StoreError> {
    while let Some(work) = inbox.recv().await {
        match worker.push(work).await {
            Ok(()) => {}
            Err(StoreError::Unavailable(error)) => {
                tracing::warn!("pushing ticket updates failed ({error}); retrying next refresh");
            }
            Err(bug) => return Err(bug),
        }
    }
    Ok(())
}
