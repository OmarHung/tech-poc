use std::sync::Arc;

use crate::config::Config;
use crate::ids;
use crate::sale_redis::{SCRIPTS, SaleRedis, StoreError};

use super::OrderView;

/// Upper bound on orders one sweep releases, so a mass expiry cannot stall Redis.
const MAX_RELEASE_PER_TICK: u32 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckoutResult {
    Created(OrderView),
    /// This ticket already ordered the same quantity: the original order, replayed.
    Existing(OrderView),
    TicketNotFound,
    /// Still waiting (or skipped, or sold out); `status` is the ticket's status.
    NotAdmitted {
        status: String,
    },
    AdmissionExpired,
    /// This ticket already ordered a different quantity.
    TicketUsed {
        order_id: String,
    },
    /// Nothing on the shelf and nothing held by unpaid orders. Final.
    SoldOut,
    /// Less than asked is on the shelf right now, possibly 0 while unpaid orders hold the rest.
    /// Retrying while admitted may succeed.
    InsufficientStock {
        left: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayResult {
    /// Charged now.
    Paid(OrderView),
    /// Charged by an earlier request; not charged again.
    AlreadyPaid(OrderView),
    OrderExpired(OrderView),
    OrderNotFound,
}

/// Reserving stock for admitted users, taking (simulated) payment, and releasing what goes unpaid.
#[derive(Clone)]
pub struct OrderStore {
    redis: SaleRedis,
    config: Arc<Config>,
}

impl OrderStore {
    pub fn new(redis: SaleRedis, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    pub async fn checkout(
        &self,
        ticket_id: &str,
        quantity: u32,
    ) -> Result<CheckoutResult, StoreError> {
        let now = self.redis.now_ms();
        let reply = self
            .redis
            .run(
                &SCRIPTS.checkout,
                &[
                    &ticket_id,
                    &quantity,
                    &ids::new_id(),
                    &now,
                    &self.config.payment_window_ms,
                ],
            )
            .await?;
        Ok(match reply.outcome.as_str() {
            "created" => CheckoutResult::Created(OrderView::from_reply(&reply)?),
            "existing" => CheckoutResult::Existing(OrderView::from_reply(&reply)?),
            "ticket_not_found" => CheckoutResult::TicketNotFound,
            "not_admitted" => CheckoutResult::NotAdmitted {
                status: reply.get("detail")?.to_owned(),
            },
            "admission_expired" => CheckoutResult::AdmissionExpired,
            "ticket_used" => CheckoutResult::TicketUsed {
                order_id: reply.get("detail")?.to_owned(),
            },
            "sold_out" => CheckoutResult::SoldOut,
            "insufficient_stock" => CheckoutResult::InsufficientStock {
                left: reply.int("detail")?,
            },
            _ => return Err(reply.unexpected()),
        })
    }

    pub async fn order(&self, order_id: &str) -> Result<Option<OrderView>, StoreError> {
        let now = self.redis.now_ms();
        let reply = self.redis.run(&SCRIPTS.order, &[&order_id, &now]).await?;
        match reply.outcome.as_str() {
            "ok" => Ok(Some(OrderView::from_reply(&reply)?)),
            "not_found" => Ok(None),
            _ => Err(reply.unexpected()),
        }
    }

    pub async fn pay(&self, order_id: &str) -> Result<PayResult, StoreError> {
        let now = self.redis.now_ms();
        let reply = self.redis.run(&SCRIPTS.pay, &[&order_id, &now]).await?;
        Ok(match reply.outcome.as_str() {
            "paid" => PayResult::Paid(OrderView::from_reply(&reply)?),
            "already_paid" => PayResult::AlreadyPaid(OrderView::from_reply(&reply)?),
            "order_expired" => PayResult::OrderExpired(OrderView::from_reply(&reply)?),
            "order_not_found" => PayResult::OrderNotFound,
            _ => return Err(reply.unexpected()),
        })
    }

    /// Puts the stock of unpaid, overdue orders back. Returns how many orders were released.
    pub async fn expire_orders(&self) -> Result<i64, StoreError> {
        let now = self.redis.now_ms();
        let reply = self
            .redis
            .run(&SCRIPTS.expire_orders, &[&now, &MAX_RELEASE_PER_TICK])
            .await?;
        reply.int("released")
    }
}
