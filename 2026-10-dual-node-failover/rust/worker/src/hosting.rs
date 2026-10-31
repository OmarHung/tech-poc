//! Wiring: checks the settings and the checkpoint, builds the HTTP API and starts the work loop.

use std::fmt;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use shared::env::ConfigError;
use shared::http::with_served_by;
use shared::state_file::StateFileError;
use shared::supervise::spawn_supervised;
use tokio::task::JoinHandle;

use crate::client::WorkClient;
use crate::config::{WorkerConfig, WorkerMode};
use crate::dispatch::worker::DispatchWorker;
use crate::hothot::ledger::Ledger;
use crate::hothot::worker::HotHotWorker;
use crate::status::{WorkerHealth, WorkerStatus};

/// Why a worker could not start. Each names what to fix.
#[derive(Debug)]
pub enum StartupError {
    Config(ConfigError),
    Checkpoint(StateFileError),
    Http(String),
}

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::Checkpoint(error) => write!(f, "cannot use the checkpoint: {error}"),
            Self::Http(error) => write!(f, "cannot set up HTTP: {error}"),
        }
    }
}

impl std::error::Error for StartupError {}

/// One of two identical workers that never talk to each other. `MODE=hothot` reads and processes every
/// event (like the other worker does); `MODE=dispatch` takes tasks the upstream leases to it.
pub struct Worker {
    config: Arc<WorkerConfig>,
    client: Arc<WorkClient>,
    status: Arc<WorkerStatus>,
}

impl Worker {
    pub fn open(config: WorkerConfig) -> Result<Self, StartupError> {
        config.validate().map_err(StartupError::Config)?;
        let status = WorkerStatus::new(&config);
        if config.mode == WorkerMode::HotHot {
            // Refuse to start on a checkpoint that cannot be read, rather than silently from zero.
            let ledger: Ledger = HotHotWorker::load(&HotHotWorker::checkpoint_path(&config))
                .map_err(StartupError::Checkpoint)?;
            status.set_ledger(ledger);
        }
        let client = WorkClient::new(&config).map_err(StartupError::Http)?;
        Ok(Self {
            config: Arc::new(config),
            client: Arc::new(client),
            status: Arc::new(status),
        })
    }

    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    pub fn status(&self) -> &Arc<WorkerStatus> {
        &self.status
    }

    /// Starts the work loop for this worker's mode. Aborting the handle stops it.
    pub fn spawn_loop(&self) -> JoinHandle<()> {
        let (client, status, config) = (
            Arc::clone(&self.client),
            Arc::clone(&self.status),
            Arc::clone(&self.config),
        );
        match self.config.mode {
            WorkerMode::HotHot => spawn_supervised("hot-hot loop", move || {
                HotHotWorker::new(
                    Arc::clone(&client),
                    Arc::clone(&status),
                    Arc::clone(&config),
                )
                .run()
            }),
            WorkerMode::Dispatch => spawn_supervised("dispatch loop", move || {
                DispatchWorker::new(
                    Arc::clone(&client),
                    Arc::clone(&status),
                    Arc::clone(&config),
                )
                .run()
            }),
        }
    }

    // No auth: the PoC binds to 127.0.0.1 only.
    pub fn router(&self) -> Router {
        let router = Router::new()
            .route("/health", get(health))
            // The hot-hot ledger, so the verify script can check a restarted worker caught up to the same balances.
            .route("/state", get(state))
            .with_state(Arc::clone(&self.status));
        with_served_by(router, &format!("worker-{}", self.config.worker_id))
    }
}

async fn health(State(status): State<Arc<WorkerStatus>>) -> Json<WorkerHealth> {
    Json(status.health())
}

async fn state(State(status): State<Arc<WorkerStatus>>) -> Json<Ledger> {
    Json(status.ledger())
}
