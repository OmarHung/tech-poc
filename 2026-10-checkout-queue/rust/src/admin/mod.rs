//! Read-only views for the PoC: the audit, the sale rules, and the board the demo page draws.
//! Unauthenticated on purpose: the process only listens on 127.0.0.1.

mod audit;
mod board;

use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

pub use audit::{AuditReport, AuditStore};
pub use board::{BoardEntry, MAX_ENTRIES, TicketBoardStore};

use crate::app::Sale;
use crate::config::Config;
use crate::http::ApiError;

/// The sale rules a client needs to draw the line and its countdowns. Deliberately leaves out
/// the Redis address, which may carry a password.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SaleSettings {
    pub instance: String,
    pub sale_id: String,
    pub max_active: u32,
    pub max_per_order: u32,
    pub admission_window_ms: u64,
    pub payment_window_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub tick_ms: u64,
}

impl From<&Config> for SaleSettings {
    fn from(config: &Config) -> Self {
        Self {
            instance: config.instance_name.clone(),
            sale_id: config.sale_id.clone(),
            max_active: config.max_active,
            max_per_order: config.max_per_order,
            admission_window_ms: config.admission_window_ms,
            payment_window_ms: config.payment_window_ms,
            heartbeat_timeout_ms: config.heartbeat_timeout_ms,
            poll_interval_ms: config.poll_interval_ms,
            tick_ms: config.tick_ms,
        }
    }
}

pub fn routes() -> Router<Arc<Sale>> {
    Router::new()
        .route("/admin/audit", get(audit))
        .route("/admin/config", get(settings))
        .route("/admin/tickets", get(tickets))
}

async fn audit(State(sale): State<Arc<Sale>>) -> Result<Json<AuditReport>, ApiError> {
    Ok(Json(sale.audit.get().await?))
}

async fn settings(State(sale): State<Arc<Sale>>) -> Json<SaleSettings> {
    Json(SaleSettings::from(sale.config.as_ref()))
}

async fn tickets(State(sale): State<Arc<Sale>>) -> Result<Json<Vec<BoardEntry>>, ApiError> {
    Ok(Json(sale.board.list().await?))
}
