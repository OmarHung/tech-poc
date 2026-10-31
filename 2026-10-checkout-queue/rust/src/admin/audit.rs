use serde::Serialize;

use crate::sale_redis::{SCRIPTS, SaleRedis, ScriptReply, StoreError};

/// The numbers the PoC is judged on, recounted from the orders themselves rather than from
/// running counters, so a bug in the counting cannot hide a bug in the selling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditReport {
    pub initial_stock: i64,
    pub stock: i64,
    pub pending_orders: i64,
    pub pending_quantity: i64,
    pub paid_orders: i64,
    pub paid_quantity: i64,
    pub expired_orders: i64,
    pub expired_quantity: i64,
    pub charges: i64,
    pub duplicate_charges: i64,
    pub users_with_multiple_live_orders: i64,
    pub tickets: i64,
    pub waiting: i64,
    pub active: i64,
    /// Units promised to buyers beyond what existed. Must be 0.
    pub oversold: i64,
    /// Every unit is on the shelf, held by an unpaid order, or sold. Nothing lost, nothing made up.
    pub stock_balanced: bool,
}

impl AuditReport {
    fn from_reply(r: &ScriptReply) -> Result<Self, StoreError> {
        let initial_stock = r.int("initialStock")?;
        let stock = r.int("stock")?;
        let pending_quantity = r.int("pendingQuantity")?;
        let paid_quantity = r.int("paidQuantity")?;
        Ok(Self {
            initial_stock,
            stock,
            pending_orders: r.int("pendingOrders")?,
            pending_quantity,
            paid_orders: r.int("paidOrders")?,
            paid_quantity,
            expired_orders: r.int("expiredOrders")?,
            expired_quantity: r.int("expiredQuantity")?,
            charges: r.int("charges")?,
            duplicate_charges: r.int("duplicateCharges")?,
            users_with_multiple_live_orders: r.int("usersWithMultipleLiveOrders")?,
            tickets: r.int("tickets")?,
            waiting: r.int("waiting")?,
            active: r.int("active")?,
            oversold: (pending_quantity + paid_quantity - initial_stock).max(0),
            stock_balanced: stock >= 0 && stock + pending_quantity + paid_quantity == initial_stock,
        })
    }
}

#[derive(Clone)]
pub struct AuditStore {
    redis: SaleRedis,
}

impl AuditStore {
    pub fn new(redis: SaleRedis) -> Self {
        Self { redis }
    }

    pub async fn get(&self) -> Result<AuditReport, StoreError> {
        let reply = self.redis.run(&SCRIPTS.audit, &[]).await?;
        AuditReport::from_reply(&reply)
    }
}
