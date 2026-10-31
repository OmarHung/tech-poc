use std::sync::Arc;

use serde::Serialize;

use crate::config::Config;
use crate::orders::OrderView;
use crate::queue::TicketView;
use crate::sale_redis::{SCRIPTS, SaleRedis, StoreError};

/// Enough for a hand-driven demo; a real line would page through it instead.
pub const MAX_ENTRIES: u32 = 300;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BoardEntry {
    pub ticket: TicketView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<OrderView>,
}

/// Every user's current ticket and order, newest first. The demo page draws its lanes from this,
/// so every tab on every instance shows the same line. Reading it is not a heartbeat.
#[derive(Clone)]
pub struct TicketBoardStore {
    redis: SaleRedis,
    config: Arc<Config>,
}

impl TicketBoardStore {
    pub fn new(redis: SaleRedis, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    pub async fn list(&self) -> Result<Vec<BoardEntry>, StoreError> {
        let now = self.redis.now_ms();
        let rows = self
            .redis
            .run_rows(&SCRIPTS.tickets, &[&now, &MAX_ENTRIES])
            .await?;
        let mut entries: Vec<BoardEntry> = Vec::with_capacity(rows.len());
        for row in rows {
            match (row.outcome.as_str(), entries.last_mut()) {
                ("ticket", _) => entries.push(BoardEntry {
                    ticket: TicketView::from_reply(&row, self.config.poll_interval_ms)?,
                    order: None,
                }),
                ("order", Some(last)) => last.order = Some(OrderView::from_reply(&row)?),
                _ => return Err(row.unexpected()),
            }
        }
        Ok(entries)
    }
}
