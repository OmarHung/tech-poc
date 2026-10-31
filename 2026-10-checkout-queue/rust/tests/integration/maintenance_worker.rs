use std::time::Duration;

use checkout_queue::app;
use checkout_queue::queue::TicketStatus;

use crate::support::{Proxy, TestSale, eventually, new_config};

/// Moves the clock one tick at a time until the ticket leaves the line (or 50 ticks pass).
async fn tick_until_admitted(sale: &TestSale, ticket_id: &str) -> TicketStatus {
    let mut status = TicketStatus::Waiting;
    for _attempt in 0..50 {
        if status != TicketStatus::Waiting {
            break;
        }
        sale.advance(sale.config().tick_interval());
        tokio::time::sleep(Duration::from_millis(20)).await;
        status = eventually(|| sale.sale.queue.ticket(ticket_id))
            .await
            .unwrap()
            .status;
    }
    status
}

pub async fn worker_admits_on_every_tick_without_being_asked() {
    let sale = TestSale::start().await;
    let ticket = sale.join("alice").await;

    let mut background = app::start_background(&sale.sale, true).await;
    let status = tick_until_admitted(&sale, &ticket.ticket_id).await;

    assert_eq!(status, TicketStatus::Admitted);
    assert!(background.try_join_next().is_none(), "no loop stopped");
}

pub async fn worker_keeps_ticking_through_a_redis_outage() {
    let mut proxy = Proxy::start().await;
    let sale = TestSale::start_at(&proxy.address(), new_config()).await;
    let mut background = app::start_background(&sale.sale, true).await;

    proxy.cut();
    for _tick in 0..3 {
        sale.advance(sale.config().tick_interval());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    proxy.restore().await;
    let ticket = eventually(|| sale.sale.queue.join("alice")).await;
    let status = tick_until_admitted(&sale, &ticket.ticket().unwrap().ticket_id).await;

    assert_eq!(status, TicketStatus::Admitted);
    assert!(background.try_join_next().is_none(), "no loop stopped");
}
