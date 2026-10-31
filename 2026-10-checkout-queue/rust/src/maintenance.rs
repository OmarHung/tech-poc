//! One maintenance pass, run by every instance on its own timer. Each step is one atomic script,
//! so there is no leader to elect and no instance whose loss stops the line.

use std::sync::Arc;
use std::time::Duration;

use crate::clock::Clock;
use crate::orders::OrderStore;
use crate::queue::{AdmitResult, QueueStore};
use crate::sale_redis::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceResult {
    pub released_orders: i64,
    pub admission: AdmitResult,
}

#[derive(Clone)]
pub struct SaleMaintenance {
    queue: QueueStore,
    orders: OrderStore,
}

impl SaleMaintenance {
    pub fn new(queue: QueueStore, orders: OrderStore) -> Self {
        Self { queue, orders }
    }

    pub async fn run_once(&self) -> Result<MaintenanceResult, StoreError> {
        // Release first: stock coming back from unpaid orders can be admitted in the same pass.
        let released_orders = self.orders.expire_orders().await?;
        let admission = self.queue.admit().await?;
        Ok(MaintenanceResult {
            released_orders,
            admission,
        })
    }
}

/// Runs a pass every `tick` on `clock`. Redis blips are logged and the next tick retries;
/// anything else is a bug, so the loop returns it and the process stops.
pub async fn run_every(
    maintenance: SaleMaintenance,
    clock: Arc<dyn Clock>,
    tick: Duration,
) -> Result<(), StoreError> {
    loop {
        clock.sleep(tick).await;
        match maintenance.run_once().await {
            Ok(_) => {}
            Err(StoreError::Unavailable(error)) => {
                tracing::warn!("maintenance tick failed ({error}); retrying next tick");
            }
            Err(bug) => return Err(bug),
        }
    }
}
