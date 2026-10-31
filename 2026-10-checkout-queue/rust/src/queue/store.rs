use std::sync::Arc;

use crate::config::Config;
use crate::ids;
use crate::sale_redis::{SCRIPTS, SaleRedis, ScriptArg, StoreError};

use super::TicketView;

/// Upper bound on tickets one admission pass looks at, so a long run of no-shows cannot stall Redis.
const MAX_SCAN_PER_TICK: u32 = 1_000;
/// Wait estimates use the admission rate over this many most recent seconds.
const ADMIT_RATE_WINDOW_SECONDS: u32 = 10;
/// Tickets per watch.lua call, so one call never blocks Redis for long.
const MAX_WATCH_BATCH: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinResult {
    Created(TicketView),
    /// This user already holds a live ticket: a refresh, a second tab or a retry.
    Existing(TicketView),
    SoldOut,
}

impl JoinResult {
    pub fn ticket(&self) -> Option<&TicketView> {
        match self {
            Self::Created(view) | Self::Existing(view) => Some(view),
            Self::SoldOut => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdmitResult {
    pub admitted: i64,
    pub abandoned: i64,
    pub expired: i64,
}

/// The virtual waiting room: take a number, poll, get let in when a slot frees up.
#[derive(Clone)]
pub struct QueueStore {
    redis: SaleRedis,
    config: Arc<Config>,
}

impl QueueStore {
    pub fn new(redis: SaleRedis, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    pub async fn join(&self, user_id: &str) -> Result<JoinResult, StoreError> {
        let now = self.redis.now_ms();
        let reply = self
            .redis
            .run(&SCRIPTS.join, &[&user_id, &ids::new_id(), &now])
            .await?;
        match reply.outcome.as_str() {
            "created" => Ok(JoinResult::Created(self.view(&reply)?)),
            "existing" => Ok(JoinResult::Existing(self.view(&reply)?)),
            "sold_out" => Ok(JoinResult::SoldOut),
            _ => Err(reply.unexpected()),
        }
    }

    /// Current view of a ticket. Also counts as the ticket's heartbeat.
    pub async fn ticket(&self, ticket_id: &str) -> Result<Option<TicketView>, StoreError> {
        let now = self.redis.now_ms();
        let reply = self.redis.run(&SCRIPTS.ticket, &[&ticket_id, &now]).await?;
        match reply.outcome.as_str() {
            "ok" => Ok(Some(self.view(&reply)?)),
            "not_found" => Ok(None),
            _ => Err(reply.unexpected()),
        }
    }

    /// Views of many tickets in a few round trips, for event streams. Counts as their heartbeat.
    /// Unknown ids are left out.
    pub async fn watch(&self, ticket_ids: &[String]) -> Result<Vec<TicketView>, StoreError> {
        let mut views = Vec::with_capacity(ticket_ids.len());
        for batch in ticket_ids.chunks(MAX_WATCH_BATCH) {
            let now = self.redis.now_ms();
            let args: Vec<ScriptArg<'_>> = std::iter::once(&now as ScriptArg<'_>)
                .chain(batch.iter().map(|id| id as ScriptArg<'_>))
                .collect();
            for row in self.redis.run_rows(&SCRIPTS.watch, &args).await? {
                if row.outcome != "ticket" {
                    return Err(row.unexpected());
                }
                views.push(self.view(&row)?);
            }
        }
        Ok(views)
    }

    /// One admission pass: drop lapsed admissions, then let in as many as there are free slots.
    pub async fn admit(&self) -> Result<AdmitResult, StoreError> {
        let now = self.redis.now_ms();
        let reply = self
            .redis
            .run(
                &SCRIPTS.admit,
                &[
                    &now,
                    &self.config.max_active,
                    &self.config.admission_window_ms,
                    &self.config.heartbeat_timeout_ms,
                    &MAX_SCAN_PER_TICK,
                    &ADMIT_RATE_WINDOW_SECONDS,
                ],
            )
            .await?;
        if reply.outcome != "ok" {
            return Err(reply.unexpected());
        }
        Ok(AdmitResult {
            admitted: reply.int("admitted")?,
            abandoned: reply.int("abandoned")?,
            expired: reply.int("expired")?,
        })
    }

    fn view(&self, reply: &crate::sale_redis::ScriptReply) -> Result<TicketView, StoreError> {
        TicketView::from_reply(reply, self.config.poll_interval_ms)
    }
}
