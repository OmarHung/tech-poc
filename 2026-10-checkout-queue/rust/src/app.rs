//! Wiring: one [`Sale`] holds every store; [`router`] is the HTTP API; [`start_background`]
//! starts the two loops every instance runs (maintenance and the event-stream push).

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderValue;
use axum::middleware::map_response_with_state;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::task::JoinSet;

use crate::admin::{self, AuditStore, TicketBoardStore};
use crate::config::{Config, ConfigError};
use crate::demo;
use crate::http::{self, ApiError};
use crate::maintenance::{self, SaleMaintenance};
use crate::orders::{self, OrderStore};
use crate::queue::{self, QueueStore, TicketPushWorker, TicketStreams, push};
use crate::sale_redis::{SaleRedis, StoreError};

/// Background loops. They only end on a bug (an error that is not a Redis outage);
/// dropping the set stops them.
pub type BackgroundTasks = JoinSet<Result<(), StoreError>>;

/// Everything one process needs to serve one sale.
pub struct Sale {
    pub config: Arc<Config>,
    pub redis: SaleRedis,
    pub queue: QueueStore,
    pub orders: OrderStore,
    pub audit: AuditStore,
    pub board: TicketBoardStore,
    pub maintenance: SaleMaintenance,
    pub streams: Arc<TicketStreams>,
}

impl Sale {
    pub fn new(config: Config, redis: SaleRedis) -> Result<Arc<Self>, ConfigError> {
        config.validate()?;
        let config = Arc::new(config);
        let queue = QueueStore::new(redis.clone(), Arc::clone(&config));
        let orders = OrderStore::new(redis.clone(), Arc::clone(&config));
        Ok(Arc::new(Self {
            audit: AuditStore::new(redis.clone()),
            board: TicketBoardStore::new(redis.clone(), Arc::clone(&config)),
            maintenance: SaleMaintenance::new(queue.clone(), orders.clone()),
            streams: Arc::new(TicketStreams::default()),
            config,
            redis,
            queue,
            orders,
        }))
    }
}

pub fn router(sale: Arc<Sale>) -> Router {
    let served_by = HeaderValue::from_str(&sale.config.instance_name)
        .expect("Config::validate checked INSTANCE_NAME");
    Router::new()
        .route("/health", get(health))
        .route("/", get(demo::page))
        .merge(queue::routes())
        .merge(orders::routes())
        .merge(admin::routes())
        .with_state(sale)
        .layer(map_response_with_state(served_by, http::served_by))
}

/// Subscribes to admissions before returning (call it before serving requests, so no admission
/// is missed), then starts the push loops and, unless tests drive it themselves, the maintenance tick.
pub async fn start_background(sale: &Arc<Sale>, run_maintenance: bool) -> BackgroundTasks {
    let mut tasks = BackgroundTasks::new();
    let worker = TicketPushWorker::new(sale.queue.clone(), Arc::clone(&sale.streams));
    push::start(&sale.redis, worker, sale.config.poll_interval(), &mut tasks).await;
    if run_maintenance {
        tasks.spawn(maintenance::run_every(
            sale.maintenance.clone(),
            Arc::clone(sale.redis.clock()),
            sale.config.tick_interval(),
        ));
    }
    tasks
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Health {
    status: &'static str,
    instance: String,
    sale_id: String,
}

async fn health(State(sale): State<Arc<Sale>>) -> Result<Json<Health>, ApiError> {
    sale.redis.ping().await?;
    Ok(Json(Health {
        status: "ok",
        instance: sale.config.instance_name.clone(),
        sale_id: sale.config.sale_id.clone(),
    }))
}
