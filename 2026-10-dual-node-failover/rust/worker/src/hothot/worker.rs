use std::path::{Path, PathBuf};
use std::sync::Arc;

use shared::state_file::{self, StateFileError};
use shared::work::{LoggedEvent, ResultRecord};
use tokio::time::sleep;
use tracing::{error, info, warn};

use super::ledger::Ledger;
use crate::client::WorkClient;
use crate::config::{RETRY_DELAY, StartFrom, WorkerConfig};
use crate::status::WorkerStatus;

/// Events read and delivered between two checkpoints.
pub const BATCH: usize = 100;

const CHECKPOINT_FILE: &str = "checkpoint.json";

/// Reads every event from the upstream log, applies it to the ledger and sends the result to the sink.
/// The other worker does exactly the same; neither knows the other exists. After each batch the
/// position and balances are saved, so a restart replays only what was not yet saved (and re-sends
/// those results, which the sink ignores).
pub struct HotHotWorker {
    client: Arc<WorkClient>,
    status: Arc<WorkerStatus>,
    config: Arc<WorkerConfig>,
}

impl HotHotWorker {
    pub fn new(
        client: Arc<WorkClient>,
        status: Arc<WorkerStatus>,
        config: Arc<WorkerConfig>,
    ) -> Self {
        Self {
            client,
            status,
            config,
        }
    }

    pub fn checkpoint_path(config: &WorkerConfig) -> PathBuf {
        config.data_dir.join(CHECKPOINT_FILE)
    }

    /// The saved ledger, or an empty one if there is no checkpoint yet.
    pub fn load(path: &Path) -> Result<Ledger, StateFileError> {
        Ok(state_file::load(path)?.unwrap_or_default())
    }

    pub async fn run(self) {
        let ledger = match Self::load(&Self::checkpoint_path(&self.config)) {
            Ok(ledger) => ledger,
            Err(error) => {
                // Starting from zero would re-send every result with balances that ignore the lost checkpoint.
                error!("Cannot read the checkpoint, not processing: {error}");
                return;
            }
        };
        let mut ledger = match self.config.start_from {
            StartFrom::Checkpoint => ledger,
            StartFrom::Latest => self.skip_to_latest(ledger).await,
        };
        self.status.set_ledger(ledger.clone());
        info!(
            "Hot-hot worker {} starting after event {}",
            self.config.worker_id, ledger.offset
        );
        loop {
            let Some(page) = self.client.read_events(ledger.offset, BATCH).await else {
                sleep(RETRY_DELAY).await;
                continue;
            };
            if page.last < ledger.offset {
                // The upstream has fewer events than this worker already applied: it lost history (an
                // in-memory upstream restarted). New events would reuse old numbers, so stop rather
                // than send wrong results.
                error!(
                    "Upstream ends at event {} but this worker is at {}; it lost events. Not processing.",
                    page.last, ledger.offset
                );
                sleep(self.config.poll_wait).await;
                continue;
            }
            ledger = self.process(ledger, &page.events).await;
        }
    }

    /// Applies and delivers one page of events, then saves the checkpoint. A crash before the save
    /// means re-sending the batch, never skipping it.
    async fn process(&self, start: Ledger, events: &[LoggedEvent]) -> Ledger {
        let mut ledger = start.clone();
        for logged in events {
            let (next, balance) = match ledger.apply(logged) {
                Ok(applied) => applied,
                Err(gap) => {
                    // Only a misbehaving upstream does this; read again from where this worker is.
                    error!("Upstream sent {gap}; reading again");
                    sleep(self.config.poll_wait).await;
                    break;
                }
            };
            let result = ResultRecord {
                key: Some(format!("event-{}", logged.seq)),
                node: Some(self.config.worker_id.clone()),
                attempt: 0,
                value: Some(self.format(logged, balance)),
            };
            self.deliver(&result).await;
            ledger = next;
        }
        if ledger.offset != start.offset {
            self.save(&ledger);
            self.status.set_ledger(ledger.clone());
        }
        ledger
    }

    fn format(&self, logged: &LoggedEvent, balance: i64) -> String {
        if self.config.deterministic {
            format!("{}:{balance}", logged.account)
        } else {
            format!("{}:{balance}@{}", logged.account, wall_clock_ms())
        }
    }

    /// Retries until the sink answers. Any answer counts: a conflict is recorded there, retrying will
    /// not change it.
    async fn deliver(&self, result: &ResultRecord) {
        let reply = loop {
            if let Some(reply) = self.client.deliver(result, None).await {
                break reply;
            }
            sleep(RETRY_DELAY).await;
        };
        self.status.sink_replied(&reply);
        if reply == "conflict" {
            warn!(
                "Sink holds a different result for {} than {}",
                result.key.as_deref().unwrap_or(""),
                result.value.as_deref().unwrap_or("")
            );
        }
    }

    async fn skip_to_latest(&self, ledger: Ledger) -> Ledger {
        let last = loop {
            if let Some(last) = self.client.last_event().await {
                break last;
            }
            sleep(RETRY_DELAY).await;
        };
        if last <= ledger.offset {
            return ledger;
        }
        warn!(
            "Skipping events {}-{last} (START_FROM=latest); balances will miss them",
            ledger.offset + 1
        );
        let skipped = ledger.skip_to(last);
        self.save(&skipped);
        skipped
    }

    /// A failed save is logged and processing goes on: the next batch tries again, and a restart in
    /// between replays from the older checkpoint, which only re-sends results the sink already has.
    fn save(&self, ledger: &Ledger) {
        let path = Self::checkpoint_path(&self.config);
        if let Err(error) = state_file::save(&path, ledger) {
            error!("Cannot save the checkpoint at {}: {error}", path.display());
        }
    }
}

/// This worker's wall clock (Unix ms): the two copies of a result agree only when both workers handle
/// the event within the same millisecond.
fn wall_clock_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis())
}
