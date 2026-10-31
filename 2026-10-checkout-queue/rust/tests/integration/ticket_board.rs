//! The shared view every demo tab draws: all users, read without touching their heartbeat.

use std::time::Duration;

use axum::http::StatusCode;
use checkout_queue::config::Config;
use checkout_queue::orders::OrderStatus;
use checkout_queue::queue::TicketStatus;
use serde_json::json;

use crate::support::{TestApp, TestSale};

pub async fn list_shows_each_users_current_ticket_with_its_order() {
    let sale = TestSale::start().await;
    let order = sale.place_order("buyer", 2).await;
    let waiting = sale.join("waiter").await;

    let board = sale.sale.board.list().await.unwrap();

    let users: Vec<&str> = board.iter().map(|e| e.ticket.user_id.as_str()).collect();
    assert_eq!(users, ["waiter", "buyer"]);
    assert_eq!(
        (
            board[0].ticket.ticket_id.as_str(),
            board[0].ticket.status,
            board[0].ticket.position
        ),
        (waiting.ticket_id.as_str(), TicketStatus::Waiting, Some(1))
    );
    assert_eq!(board[0].order, None);
    let placed = board[1].order.as_ref().unwrap();
    assert_eq!(
        (placed.order_id.as_str(), placed.status, placed.quantity),
        (order.order_id.as_str(), OrderStatus::PendingPayment, 2)
    );
}

pub async fn list_shows_only_the_new_ticket_after_a_user_lines_up_again() {
    let sale = TestSale::start().await;
    let first = sale.join_and_admit(&["slow"]).await.remove(0);
    sale.advance(Duration::from_secs(60));
    let second = sale.join("slow").await;

    let board = sale.sale.board.list().await.unwrap();

    assert_eq!(board.len(), 1);
    assert_ne!(first.ticket_id, board[0].ticket.ticket_id);
    assert_eq!(second.ticket_id, board[0].ticket.ticket_id);
}

pub async fn list_is_not_a_heartbeat_so_watchers_cannot_keep_a_gone_user_in_line() {
    let sale = TestSale::start_with(|c| Config { max_active: 1, ..c }).await;
    sale.join("gone").await;

    sale.advance(Duration::from_secs(16));
    sale.sale.board.list().await.unwrap();
    sale.sale.queue.admit().await.unwrap();

    assert_eq!(
        sale.sale.board.list().await.unwrap()[0].ticket.status,
        TicketStatus::Abandoned
    );
}

pub async fn admin_tickets_returns_ticket_and_order_objects() {
    let app = TestApp::start().await;
    app.post_json("/queue/join", &json!({ "userId": "alice" }))
        .await;

    let response = app.get("/admin/tickets").await;
    let body = response.json();

    assert_eq!(response.status, StatusCode::OK);
    let entries = body.as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["ticket"]["userId"], "alice");
    assert_eq!(entries[0]["ticket"]["status"], "waiting");
    assert!(
        entries[0].get("order").is_none(),
        "no order yet, so the field is left out"
    );
}
