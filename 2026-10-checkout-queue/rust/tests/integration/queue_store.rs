use std::time::Duration;

use checkout_queue::config::Config;
use checkout_queue::ids;
use checkout_queue::orders::CheckoutResult;
use checkout_queue::queue::{AdmitResult, JoinResult, TicketStatus};

use crate::support::TestSale;

pub async fn join_puts_users_in_line_in_arrival_order() {
    let sale = TestSale::start().await;

    let first = sale.sale.queue.join("alice").await.unwrap();
    let second = sale.sale.queue.join("bob").await.unwrap();

    let JoinResult::Created(first) = first else {
        panic!("expected a new ticket, got {first:?}")
    };
    let second = second.ticket().unwrap().clone();
    assert_eq!(second.status, TicketStatus::Waiting);
    assert_eq!((first.position, second.position), (Some(1), Some(2)));
    assert!(second.seq > first.seq);
    assert_eq!(second.poll_after_ms, 1000);
}

pub async fn join_again_returns_the_same_ticket_so_nobody_holds_two_places() {
    let sale = TestSale::start().await;

    let first = sale.join("alice").await;
    let again = sale.sale.queue.join("alice").await.unwrap();

    let JoinResult::Existing(again) = again else {
        panic!("expected the existing ticket, got {again:?}")
    };
    assert_eq!(first.ticket_id, again.ticket_id);
}

pub async fn admit_lets_in_at_most_max_active_in_arrival_order() {
    let sale = TestSale::start_with(|c| Config { max_active: 2, ..c }).await;

    let tickets = sale.join_and_admit(&["a", "b", "c"]).await;

    assert_eq!(tickets[0].status, TicketStatus::Admitted);
    assert_eq!(tickets[1].status, TicketStatus::Admitted);
    assert_eq!(tickets[2].status, TicketStatus::Waiting);
    assert_eq!(tickets[2].position, Some(1));
    assert_eq!(tickets[0].admitted_at_ms, Some(sale.now_ms()));
    assert_eq!(
        tickets[0].admission_expires_at_ms,
        Some(sale.now_ms() + 60_000)
    );
    assert_eq!(tickets[0].position, None);
}

pub async fn admit_never_lets_in_more_users_than_stock_left() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 1,
        max_active: 5,
        ..c
    })
    .await;

    let tickets = sale.join_and_admit(&["a", "b"]).await;

    assert_eq!(tickets[0].status, TicketStatus::Admitted);
    assert_eq!(tickets[1].status, TicketStatus::Waiting);
}

pub async fn admit_skips_users_who_stopped_polling() {
    let sale = TestSale::start_with(|c| Config { max_active: 1, ..c }).await;
    let gone = sale.join("gone").await;
    let stays = sale.join("stays").await;

    sale.advance(Duration::from_secs(16));
    sale.ticket(&stays.ticket_id).await;
    let result = sale.sale.queue.admit().await.unwrap();

    assert_eq!(
        result,
        AdmitResult {
            admitted: 1,
            abandoned: 1,
            expired: 0
        }
    );
    assert_eq!(
        sale.ticket(&gone.ticket_id).await.status,
        TicketStatus::Abandoned
    );
    assert_eq!(
        sale.ticket(&stays.ticket_id).await.status,
        TicketStatus::Admitted
    );
}

pub async fn admission_window_expires_and_frees_the_slot_for_the_next_user() {
    let sale = TestSale::start_with(|c| Config { max_active: 1, ..c }).await;
    let tickets = sale.join_and_admit(&["slow", "next"]).await;

    sale.advance(Duration::from_secs(60));
    sale.ticket(&tickets[1].ticket_id).await;
    let expired_before_tick = sale.ticket(&tickets[0].ticket_id).await;
    let result = sale.sale.queue.admit().await.unwrap();

    assert_eq!(expired_before_tick.status, TicketStatus::Expired);
    assert_eq!(
        result,
        AdmitResult {
            admitted: 1,
            abandoned: 0,
            expired: 1
        }
    );
    assert_eq!(
        sale.ticket(&tickets[1].ticket_id).await.status,
        TicketStatus::Admitted
    );
}

pub async fn rejoin_after_admission_expired_goes_to_the_back_of_the_line() {
    let sale = TestSale::start_with(|c| Config { max_active: 1, ..c }).await;
    let tickets = sale.join_and_admit(&["slow", "next", "last"]).await;
    sale.advance(Duration::from_secs(60));

    let rejoined = sale.sale.queue.join("slow").await.unwrap();

    let JoinResult::Created(rejoined) = rejoined else {
        panic!("expected a new ticket, got {rejoined:?}")
    };
    assert_ne!(tickets[0].ticket_id, rejoined.ticket_id);
    assert_eq!(rejoined.position, Some(3));
}

pub async fn rejoin_after_being_skipped_for_not_polling_gets_a_new_ticket() {
    let sale = TestSale::start().await;
    let first = sale.join("napper").await;
    sale.advance(Duration::from_secs(16));
    sale.sale.queue.admit().await.unwrap();

    let rejoined = sale.sale.queue.join("napper").await.unwrap();

    let JoinResult::Created(rejoined) = rejoined else {
        panic!("expected a new ticket, got {rejoined:?}")
    };
    assert_ne!(first.ticket_id, rejoined.ticket_id);
}

pub async fn waiting_ticket_estimates_the_wait_once_admissions_are_flowing() {
    let sale = TestSale::start_with(|c| Config { max_active: 1, ..c }).await;
    for user in ["u0", "u1", "u2"] {
        sale.join(user).await;
    }
    let before_any_admission = sale.join("u2").await;

    sale.advance(Duration::from_secs(1));
    sale.sale.queue.admit().await.unwrap();
    let after_admission = sale.join("u2").await;

    assert_eq!(before_any_admission.estimated_wait_seconds, None);
    assert_eq!(after_admission.position, Some(2));
    assert!(
        after_admission
            .estimated_wait_seconds
            .is_some_and(|s| s > 0),
        "estimate was {:?}",
        after_admission.estimated_wait_seconds
    );
}

pub async fn wait_estimate_matches_a_steady_admission_rate() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 100,
        max_active: 2,
        ..c
    })
    .await;
    let users: Vec<String> = (0..30).map(|i| format!("u{i}")).collect();
    for user in &users {
        sale.join(user).await;
    }

    // Two people check out every second, so two are let in every second.
    for _second in 0..5 {
        sale.sale.queue.admit().await.unwrap();
        for user in &users {
            let ticket = sale.join(user).await;
            if ticket.status == TicketStatus::Admitted {
                let result = sale
                    .sale
                    .orders
                    .checkout(&ticket.ticket_id, 1)
                    .await
                    .unwrap();
                assert!(matches!(result, CheckoutResult::Created(_)), "{result:?}");
            }
        }
        sale.advance(Duration::from_secs(1));
    }
    sale.sale.queue.admit().await.unwrap();
    let tenth_in_line = sale.join(&users[21]).await;

    assert_eq!(tenth_in_line.position, Some(10));
    assert_eq!(tenth_in_line.estimated_wait_seconds, Some(5));
}

pub async fn get_ticket_returns_none_for_an_unknown_ticket() {
    let sale = TestSale::start().await;

    assert_eq!(sale.sale.queue.ticket(&ids::new_id()).await.unwrap(), None);
}
