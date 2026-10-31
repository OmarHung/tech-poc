//! Fires many requests at once. They share one multiplexed connection and interleave on Redis
//! exactly like requests coming from several app instances.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use checkout_queue::config::Config;
use checkout_queue::orders::{CheckoutResult, OrderView, PayResult};
use checkout_queue::queue::{AdmitResult, JoinResult, TicketStatus};
use futures_util::future::join_all;

use crate::support::{TestSale, users};

const CLICKS_PER_USER: usize = 3;

fn order_of(result: &CheckoutResult) -> Option<&OrderView> {
    match result {
        CheckoutResult::Created(order) | CheckoutResult::Existing(order) => Some(order),
        _ => None,
    }
}

pub async fn parallel_checkouts_never_oversell_nor_order_twice_per_ticket() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 100,
        max_active: 100,
        ..c
    })
    .await;
    let names = users(100);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let tickets = sale.join_and_admit(&names).await;
    assert!(tickets.iter().all(|t| t.status == TicketStatus::Admitted));

    // 100 admitted users each want 2 of the 100 left and click three times: 300 requests at once.
    let attempts = tickets
        .iter()
        .flat_map(|t| std::iter::repeat_n(t.ticket_id.as_str(), CLICKS_PER_USER))
        .map(|id| sale.sale.orders.checkout(id, 2));
    let results: Vec<CheckoutResult> = join_all(attempts)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    let orders: Vec<&OrderView> = results.iter().filter_map(order_of).collect();
    let distinct: HashSet<&str> = orders.iter().map(|o| o.order_id.as_str()).collect();
    assert_eq!(distinct.len(), 50);
    let created = results
        .iter()
        .filter(|r| matches!(r, CheckoutResult::Created(_)))
        .count();
    assert_eq!(created, 50);
    let mut per_ticket: HashMap<&str, HashSet<&str>> = HashMap::new();
    for order in &orders {
        per_ticket
            .entry(order.ticket_id.as_str())
            .or_default()
            .insert(order.order_id.as_str());
    }
    assert!(
        per_ticket.values().all(|ids| ids.len() == 1),
        "{per_ticket:?}"
    );
    let audit = sale.audit().await;
    assert_eq!(
        (audit.stock, audit.oversold, audit.pending_quantity),
        (0, 0, 100)
    );
    assert!(audit.stock_balanced);
    assert_eq!(audit.users_with_multiple_live_orders, 0);
}

pub async fn parallel_payments_charge_each_order_once() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 20,
        max_active: 10,
        ..c
    })
    .await;
    let names = users(10);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let tickets = sale.join_and_admit(&names).await;
    let orders = join_all(
        tickets
            .iter()
            .map(|t| sale.sale.orders.checkout(&t.ticket_id, 1)),
    )
    .await;
    let order_ids: Vec<String> = orders
        .iter()
        .map(|r| order_of(r.as_ref().unwrap()).unwrap().order_id.clone())
        .collect();

    let payments = order_ids
        .iter()
        .flat_map(|id| std::iter::repeat_n(id.as_str(), 10))
        .map(|id| sale.sale.orders.pay(id));
    let results: Vec<PayResult> = join_all(payments)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    let paid = results
        .iter()
        .filter(|r| matches!(r, PayResult::Paid(_)))
        .count();
    let already = results
        .iter()
        .filter(|r| matches!(r, PayResult::AlreadyPaid(_)))
        .count();
    assert_eq!((paid, already), (10, 90));
    let audit = sale.audit().await;
    assert_eq!(
        (audit.charges, audit.duplicate_charges, audit.paid_orders),
        (10, 0, 10)
    );
}

pub async fn parallel_joins_from_the_same_user_yield_one_ticket() {
    let sale = TestSale::start().await;

    let results: Vec<JoinResult> = join_all((0..20).map(|_| sale.sale.queue.join("refresher")))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    let created = results
        .iter()
        .filter(|r| matches!(r, JoinResult::Created(_)))
        .count();
    assert_eq!(created, 1);
    let ids: HashSet<&str> = results
        .iter()
        .map(|r| r.ticket().unwrap().ticket_id.as_str())
        .collect();
    assert_eq!(ids.len(), 1);
    assert_eq!(sale.audit().await.tickets, 1);
}

pub async fn parallel_admitters_never_exceed_max_active() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 100,
        max_active: 5,
        ..c
    })
    .await;
    for user in users(50) {
        sale.join(&user).await;
    }

    // Every app instance runs its own admitter; ten of them tick at the same moment.
    let results: Vec<AdmitResult> = join_all((0..10).map(|_| sale.sale.queue.admit()))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.iter().map(|r| r.admitted).sum::<i64>(), 5);
    let audit = sale.audit().await;
    assert_eq!((audit.active, audit.waiting), (5, 45));
}

pub async fn payments_racing_the_expiry_sweep_return_each_orders_stock_once() {
    let sale = TestSale::start_with(|c| Config {
        max_active: 10,
        ..c
    })
    .await;
    let names = users(10);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let tickets = sale.join_and_admit(&names).await;
    let orders = join_all(
        tickets
            .iter()
            .map(|t| sale.sale.orders.checkout(&t.ticket_id, 1)),
    )
    .await;
    let order_ids: Vec<String> = orders
        .iter()
        .map(|r| order_of(r.as_ref().unwrap()).unwrap().order_id.clone())
        .collect();
    sale.advance(Duration::from_secs(120));

    let late_payments = join_all(
        order_ids
            .iter()
            .flat_map(|id| std::iter::repeat_n(id.as_str(), CLICKS_PER_USER))
            .map(|id| sale.sale.orders.pay(id)),
    );
    let sweeps = join_all((0..CLICKS_PER_USER).map(|_| sale.sale.maintenance.run_once()));
    let (payments, sweeps) = tokio::join!(late_payments, sweeps);

    assert!(
        payments
            .iter()
            .all(|p| matches!(p, Ok(PayResult::OrderExpired(_))))
    );
    assert!(sweeps.iter().all(Result::is_ok));
    let audit = sale.audit().await;
    assert_eq!(
        (audit.stock, audit.expired_orders, audit.charges),
        (10, 10, 0)
    );
    assert!(audit.stock_balanced);
}

pub async fn admitter_with_a_clock_slightly_behind_still_completes_its_pass() {
    let ahead = TestSale::start().await;
    let behind = TestSale::start_config(ahead.config().clone()).await;
    ahead.join("first").await;
    ahead.join("second").await;

    // "ahead" crosses into the next second first and marks it as when the line formed.
    ahead.advance(Duration::from_secs(1));
    behind.advance(Duration::from_millis(995));
    ahead.sale.queue.admit().await.unwrap();
    let result = behind.sale.queue.admit().await.unwrap();

    assert_eq!(result, AdmitResult::default());
}
