//! Wiring: opens the node's state, builds its HTTP API and starts its background loops.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Serialize;
use shared::env::ConfigError;
use shared::error::ApiError;
use shared::http::{read_json, with_served_by};
use shared::state_file::StateFileError;
use shared::supervise::spawn_supervised;
use tokio::task::JoinHandle;
use url::Url;

use crate::cluster::contracts::{ReplicateReply, ReplicateRequest};
use crate::cluster::election::ElectionWorker;
use crate::cluster::node_state::{NodeHealth, NodeState};
use crate::cluster::primary_term::Arbiter;
use crate::cluster::replication::ReplicationWorker;
use crate::cluster::rpc::RpcClient;
use crate::cluster::witness_client::{LeaseSource, NoArbiter, WitnessClient};
use crate::config::{ArbiterMode, NodeConfig};
use crate::kv::{KvItem, KvService, PutBody, validate_key, validate_value};
use crate::storage::log_file::LogFileError;
use crate::storage::replicated_log::ReplicatedLog;

const LOG_FILE_NAME: &str = "log.jsonl";

/// Why a node could not start. Each names what to fix.
#[derive(Debug)]
pub enum StartupError {
    Config(ConfigError),
    Log(LogFileError),
    State(StateFileError),
    Http(String),
}

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::Log(error) => write!(f, "cannot open the log: {error}"),
            Self::State(error) => write!(f, "{error}"),
            Self::Http(error) => write!(f, "cannot set up HTTP: {error}"),
        }
    }
}

impl std::error::Error for StartupError {}

/// One data node: its state plus what it needs to talk to the peer and the witness.
pub struct Node {
    state: Arc<NodeState>,
    witness: Arc<WitnessClient>,
    rpc: RpcClient,
    replicate_url: Url,
}

impl Node {
    pub fn open(config: NodeConfig) -> Result<Self, StartupError> {
        config.validate().map_err(StartupError::Config)?;
        let log =
            ReplicatedLog::open(&config.data_dir.join(LOG_FILE_NAME)).map_err(StartupError::Log)?;
        let rpc =
            RpcClient::new(config.renew_interval).map_err(|e| StartupError::Http(e.to_string()))?;
        let witness = WitnessClient::new(rpc.clone(), &config.node_id, &config.witness_url)
            .map_err(|e| StartupError::Http(format!("WITNESS_URL: {e}")))?;
        let witness = Arc::new(witness);
        let replicate_url = config
            .peer_url
            .join("internal/replicate")
            .map_err(|e| StartupError::Http(format!("PEER_URL: {e}")))?;
        let arbiter: Arc<dyn Arbiter> = match config.arbiter {
            ArbiterMode::Witness => witness.clone(),
            ArbiterMode::None => Arc::new(NoArbiter),
        };
        let state = NodeState::new(config, Arc::new(log), arbiter).map_err(StartupError::State)?;
        Ok(Self {
            state: Arc::new(state),
            witness,
            rpc,
            replicate_url,
        })
    }

    pub fn state(&self) -> &Arc<NodeState> {
        &self.state
    }

    /// Starts the election and replication loops. Aborting the handles stops them.
    pub fn spawn_workers(&self) -> Vec<JoinHandle<()>> {
        let state = Arc::clone(&self.state);
        let witness: Arc<dyn LeaseSource> = self.witness.clone();
        let election = spawn_supervised("election loop", move || {
            ElectionWorker::new(Arc::clone(&state), Arc::clone(&witness)).run()
        });
        let (state, rpc, url) = (
            Arc::clone(&self.state),
            self.rpc.clone(),
            self.replicate_url.clone(),
        );
        let replication = spawn_supervised("replication loop", move || {
            ReplicationWorker::new(Arc::clone(&state), rpc.clone(), url.clone()).run()
        });
        vec![election, replication]
    }

    // No auth anywhere: the PoC binds to 127.0.0.1. A real deployment must at least keep /internal and
    // /admin off the client network and authenticate the peer (mTLS or similar).
    pub fn router(&self) -> Router {
        let app = AppState {
            kv: Arc::new(KvService::new(Arc::clone(&self.state))),
            node: Arc::clone(&self.state),
        };
        let router = Router::new()
            .route("/health", get(health))
            .route("/kv/{key}", put(put_kv).get(get_kv))
            .route("/internal/replicate", post(replicate))
            .route("/admin/data", get(admin_data))
            .with_state(app);
        with_served_by(router, &format!("node-{}", self.state.config().node_id))
    }
}

#[derive(Clone)]
struct AppState {
    node: Arc<NodeState>,
    kv: Arc<KvService>,
}

/// Everything this node holds, primary or not. The verify script compares the two nodes with it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminData {
    node: String,
    role: String,
    epoch: i64,
    last_index: i64,
    data: BTreeMap<String, String>,
}

async fn health(State(app): State<AppState>) -> Json<NodeHealth> {
    Json(app.node.health())
}

async fn put_kv(
    State(app): State<AppState>,
    key: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<KvItem>, ApiError> {
    let key = path_key(key)?;
    validate_key(&key)?;
    let body: PutBody = read_json(&headers, &body)?;
    let value = validate_value(body.value)?;
    Ok(Json(app.kv.put(&key, &value).await?))
}

async fn get_kv(
    State(app): State<AppState>,
    key: Result<Path<String>, PathRejection>,
) -> Result<Json<KvItem>, ApiError> {
    let key = path_key(key)?;
    validate_key(&key)?;
    Ok(Json(app.kv.get(&key).await?))
}

async fn replicate(
    State(app): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ReplicateReply>, ApiError> {
    let request: ReplicateRequest = read_json(&headers, &body)?;
    Ok(Json(app.node.handle_replicate(&request)?))
}

async fn admin_data(State(app): State<AppState>) -> Json<AdminData> {
    let health = app.node.health();
    Json(AdminData {
        node: health.node,
        role: health.role,
        epoch: health.epoch,
        last_index: health.last_index,
        data: app.node.log().snapshot(),
    })
}

fn path_key(key: Result<Path<String>, PathRejection>) -> Result<String, ApiError> {
    key.map(|Path(key)| key)
        .map_err(|_| ApiError::InvalidInput("key must be valid percent-encoded UTF-8".into()))
}
