use std::time::Duration;

use checkout_queue::config::Config;
use checkout_queue::ids;
use checkout_queue::orders::{CheckoutResult, OrderStatus, PayResult};
use checkout_queue::queue::{JoinResult, TicketStatus};

use crate::support::TestSale;

pub async fn checkout_before_admission_is_rejected() {
    let sale = TestSale::start().await;
    let ticket = sale.join("eager").await;

    let result = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 1)
        .await
        .unwrap();

    assert_eq!(
        result,
        CheckoutResult::NotAdmitted {
            status: "waiting".to_owned()
        }
    );
}

pub async fn checkout_reserves_stock_and_starts_the_payment_clock() {
    let sale = TestSale::start().await;
    let ticket = sale.join_and_admit(&["alice"]).await.remove(0);

    let result = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 2)
        .await
        .unwrap();

    let CheckoutResult::Created(order) = result else {
        panic!("expected a new order, got {result:?}")
    };
    assert_eq!(
        (
            order.ticket_id.as_str(),
            order.user_id.as_str(),
            order.quantity,
            order.status
        ),
        (
            ticket.ticket_id.as_str(),
            "alice",
            2,
            OrderStatus::PendingPayment
        )
    );
    assert_eq!(order.payment_expires_at_ms, sale.now_ms() + 120_000);
    assert_eq!(order.paid_at_ms, None);
    assert_eq!(sale.audit().await.stock, 8);
    let after = sale.ticket(&ticket.ticket_id).await;
    assert_eq!(
        (after.status, after.order_id),
        (TicketStatus::Ordered, Some(order.order_id))
    );
}

pub async fn checkout_repeated_returns_the_same_order_and_reserves_once() {
    let sale = TestSale::start().await;
    let ticket = sale.join_and_admit(&["double-clicker"]).await.remove(0);

    let first = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 2)
        .await
        .unwrap();
    let second = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 2)
        .await
        .unwrap();

    let (CheckoutResult::Created(first), CheckoutResult::Existing(second)) = (first, second) else {
        panic!("expected created, then existing")
    };
    assert_eq!(first.order_id, second.order_id);
    assert_eq!(sale.audit().await.stock, 8);
}

pub async fn checkout_repeated_with_a_different_quantity_is_rejected() {
    let sale = TestSale::start().await;
    let ticket = sale.join_and_admit(&["indecisive"]).await.remove(0);
    let first = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 1)
        .await
        .unwrap();

    let second = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 2)
        .await
        .unwrap();

    let CheckoutResult::Created(first) = first else {
        panic!("expected a new order, got {first:?}")
    };
    assert_eq!(
        second,
        CheckoutResult::TicketUsed {
            order_id: first.order_id
        }
    );
    assert_eq!(sale.audit().await.stock, 9);
}

pub async fn checkout_more_than_is_left_is_rejected_and_keeps_the_admission() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 1,
        ..c
    })
    .await;
    let ticket = sale.join_and_admit(&["alice"]).await.remove(0);

    let too_many = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 2)
        .await
        .unwrap();
    let retry = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 1)
        .await
        .unwrap();

    assert_eq!(too_many, CheckoutResult::InsufficientStock { left: 1 });
    assert!(matches!(retry, CheckoutResult::Created(_)), "{retry:?}");
}

pub async fn checkout_while_unpaid_orders_hold_the_rest_is_not_sold_out_yet() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 2,
        ..c
    })
    .await;
    let tickets = sale.join_and_admit(&["first", "second"]).await;
    let CheckoutResult::Created(held) = sale
        .sale
        .orders
        .checkout(&tickets[0].ticket_id, 2)
        .await
        .unwrap()
    else {
        panic!("first checkout failed")
    };

    let while_held = sale
        .sale
        .orders
        .checkout(&tickets[1].ticket_id, 1)
        .await
        .unwrap();
    sale.sale.orders.pay(&held.order_id).await.unwrap();
    let after_paid = sale
        .sale
        .orders
        .checkout(&tickets[1].ticket_id, 1)
        .await
        .unwrap();

    // "sold_out" is final everywhere; while stock may still come back it is only "insufficient".
    assert_eq!(while_held, CheckoutResult::InsufficientStock { left: 0 });
    assert_eq!(after_paid, CheckoutResult::SoldOut);
    assert_eq!(
        sale.ticket(&tickets[1].ticket_id).await.status,
        TicketStatus::SoldOut
    );
}

pub async fn checkout_after_the_admission_window_is_rejected() {
    let sale = TestSale::start().await;
    let ticket = sale.join_and_admit(&["slow"]).await.remove(0);
    sale.advance(Duration::from_secs(60));

    let result = sale
        .sale
        .orders
        .checkout(&ticket.ticket_id, 1)
        .await
        .unwrap();

    assert_eq!(result, CheckoutResult::AdmissionExpired);
    let audit = sale.audit().await;
    assert_eq!((audit.active, audit.stock), (0, 10));
}

pub async fn checkout_unknown_ticket_is_not_found() {
    let sale = TestSale::start().await;

    let result = sale.sale.orders.checkout(&ids::new_id(), 1).await.unwrap();

    assert_eq!(result, CheckoutResult::TicketNotFound);
}

pub async fn pay_charges_once_even_when_repeated() {
    let sale = TestSale::start().await;
    let order = sale.place_order("payer", 2).await;

    let first = sale.sale.orders.pay(&order.order_id).await.unwrap();
    let second = sale.sale.orders.pay(&order.order_id).await.unwrap();

    let PayResult::Paid(paid) = first else {
        panic!("expected a charge, got {first:?}")
    };
    assert_eq!(
        (paid.status, paid.paid_at_ms),
        (OrderStatus::Paid, Some(sale.now_ms()))
    );
    assert!(matches!(second, PayResult::AlreadyPaid(_)), "{second:?}");
    let audit = sale.audit().await;
    assert_eq!(
        (
            audit.charges,
            audit.duplicate_charges,
            audit.paid_orders,
            audit.paid_quantity
        ),
        (1, 0, 1, 2)
    );
}

pub async fn pay_after_the_deadline_expires_the_order_and_returns_the_stock() {
    let sale = TestSale::start().await;
    let order = sale.place_order("late", 2).await;
    sale.advance(Duration::from_secs(120));

    let shown_before_sweep = sale
        .sale
        .orders
        .order(&order.order_id)
        .await
        .unwrap()
        .unwrap();
    let result = sale.sale.orders.pay(&order.order_id).await.unwrap();
    let again = sale.sale.orders.pay(&order.order_id).await.unwrap();

    assert_eq!(shown_before_sweep.status, OrderStatus::Expired);
    assert!(matches!(result, PayResult::OrderExpired(_)), "{result:?}");
    assert!(matches!(again, PayResult::OrderExpired(_)), "{again:?}");
    let audit = sale.audit().await;
    assert_eq!(
        (
            audit.stock,
            audit.expired_orders,
            audit.expired_quantity,
            audit.charges
        ),
        (10, 1, 2, 0)
    );
}

pub async fn pay_unknown_order_is_not_found() {
    let sale = TestSale::start().await;

    assert_eq!(
        sale.sale.orders.pay(&ids::new_id()).await.unwrap(),
        PayResult::OrderNotFound
    );
    assert_eq!(sale.sale.orders.order(&ids::new_id()).await.unwrap(), None);
}

pub async fn maintenance_releases_unpaid_orders_and_the_stock_goes_to_the_next_in_line() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 1,
        max_active: 1,
        ..c
    })
    .await;
    let tickets = sale.join_and_admit(&["never-pays", "next"]).await;
    sale.sale
        .orders
        .checkout(&tickets[0].ticket_id, 1)
        .await
        .unwrap();
    assert_eq!(
        sale.ticket(&tickets[1].ticket_id).await.status,
        TicketStatus::Waiting
    );

    sale.advance(Duration::from_secs(120));
    sale.ticket(&tickets[1].ticket_id).await;
    let pass = sale.sale.maintenance.run_once().await.unwrap();
    let checkout = sale
        .sale
        .orders
        .checkout(&tickets[1].ticket_id, 1)
        .await
        .unwrap();

    assert_eq!(pass.released_orders, 1);
    assert_eq!(pass.admission.admitted, 1);
    assert!(
        matches!(checkout, CheckoutResult::Created(_)),
        "{checkout:?}"
    );
}

pub async fn rejoin_after_the_order_expired_gets_a_new_ticket_but_after_paying_does_not() {
    let sale = TestSale::start().await;
    let expired = sale.place_order("forgetful", 1).await;
    let paid = sale.place_order("buyer", 1).await;
    sale.sale.orders.pay(&paid.order_id).await.unwrap();
    sale.advance(Duration::from_secs(120));

    let forgetful = sale.sale.queue.join("forgetful").await.unwrap();
    let buyer = sale.sale.queue.join("buyer").await.unwrap();

    let JoinResult::Created(forgetful) = forgetful else {
        panic!("expected a new ticket, got {forgetful:?}")
    };
    assert_ne!(expired.ticket_id, forgetful.ticket_id);
    let JoinResult::Existing(buyer) = buyer else {
        panic!("expected the existing ticket, got {buyer:?}")
    };
    assert_eq!(
        (buyer.status, buyer.order_id),
        (TicketStatus::Ordered, Some(paid.order_id))
    );
}

pub async fn sold_out_turns_away_newcomers_and_tells_the_line() {
    let sale = TestSale::start_with(|c| Config {
        initial_stock: 1,
        ..c
    })
    .await;
    let order = sale.place_order("lucky", 1).await;
    let waiting = sale.join("hopeful").await;

    let while_pending = sale.ticket(&waiting.ticket_id).await;
    sale.sale.orders.pay(&order.order_id).await.unwrap();
    let after_paid = sale.ticket(&waiting.ticket_id).await;
    let newcomer = sale.sale.queue.join("late").await.unwrap();

    assert_eq!(while_pending.status, TicketStatus::Waiting);
    assert_eq!(after_paid.status, TicketStatus::SoldOut);
    assert_eq!(newcomer, JoinResult::SoldOut);
    assert_eq!(newcomer.ticket(), None);
}
