//! Server-sent events: the push alternative to polling a ticket.

use std::time::Duration;

use axum::http::StatusCode;
use checkout_queue::config::Config;
use checkout_queue::ids;
use checkout_queue::queue::TicketStatus;
use checkout_queue::sale_redis::StoreError;
use serde_json::json;

use crate::support::{EVENT_TIMEOUT, Proxy, TestApp, TestSale, eventually, raw_redis};

pub async fn watch_returns_the_views_and_counts_as_presence() {
    let sale = TestSale::start_with(|c| Config { max_active: 2, ..c }).await;
    let watched = sale.join("streaming").await;
    let silent = sale.join("silent").await;

    sale.advance(Duration::from_secs(16));
    let views = sale
        .sale
        .queue
        .watch(&[watched.ticket_id.clone(), ids::new_id()])
        .await
        .unwrap();
    sale.sale.queue.admit().await.unwrap();

    assert_eq!(views.len(), 1);
    assert_eq!(views[0].ticket_id, watched.ticket_id);
    assert_eq!(
        sale.ticket(&watched.ticket_id).await.status,
        TicketStatus::Admitted
    );
    assert_eq!(
        sale.ticket(&silent.ticket_id).await.status,
        TicketStatus::Abandoned
    );
}

pub async fn watch_handles_more_tickets_than_one_batch() {
    let sale = TestSale::start().await;
    let mut ids: Vec<String> = Vec::new();
    for i in 0..3 {
        ids.push(sale.join(&format!("u{i}")).await.ticket_id);
    }
    ids.extend((0..600).map(|_| ids::new_id()));

    let views = sale.sale.queue.watch(&ids).await.unwrap();

    assert_eq!(views.len(), 3);
}

pub async fn events_start_with_the_ticket_and_push_the_admission_right_away() {
    let app = TestApp::start().await;
    let ticket_id = app.join("listener").await;
    let mut events = app.events(&ticket_id).await;

    let first = events.next().await;
    app.sale.sale.maintenance.run_once().await.unwrap();
    let second = events.next().await;

    assert_eq!(events.status, StatusCode::OK);
    assert_eq!(first["status"], "waiting");
    assert_eq!(first["position"], 1);
    // No clock moved: only the pub/sub message can have delivered this.
    assert_eq!(second["status"], "admitted");
}

pub async fn events_carry_streaming_headers() {
    let app = TestApp::start().await;
    let ticket_id = app.join("listener").await;

    let events = app.events(&ticket_id).await;

    assert_eq!(events.status, StatusCode::OK);
    let header = |name: &str| {
        events
            .headers
            .get(name)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(header("content-type"), "text/event-stream");
    assert_eq!(header("cache-control"), "no-cache");
    assert_eq!(header("x-accel-buffering"), "no");
    assert_eq!(header("x-served-by"), "test-instance");
}

pub async fn events_end_once_the_ticket_can_no_longer_change() {
    let app = TestApp::start().await;
    let ticket_id = app.join("buyer").await;
    app.sale.sale.maintenance.run_once().await.unwrap();
    let mut events = app.events(&ticket_id).await;
    assert_eq!(events.next().await["status"], "admitted");

    app.post_json(
        "/checkout",
        &json!({ "ticketId": ticket_id, "quantity": 1 }),
    )
    .await;
    app.sale.advance(Duration::from_secs(1));
    let last = events.next().await;

    assert_eq!(last["status"], "ordered");
    assert!(
        events.ended().await,
        "the stream should end after a final status"
    );
}

pub async fn events_refresh_the_position_every_poll_interval() {
    let app = TestApp::start_with(|c| Config { max_active: 1, ..c }).await;
    app.join("a").await;
    app.join("b").await;
    let ticket_id = app.join("c").await;
    let mut events = app.events(&ticket_id).await;
    assert_eq!(events.next().await["position"], 3);

    // "a" is admitted; that message names only "a", so "c" hears about it on the next refresh.
    app.sale.sale.maintenance.run_once().await.unwrap();
    app.sale.advance(Duration::from_secs(1));
    let moved = events.next().await;

    assert_eq!(moved["status"], "waiting");
    assert_eq!(moved["position"], 2);
}

pub async fn closing_a_stream_unregisters_it() {
    let app = TestApp::start().await;
    let ticket_id = app.join("leaver").await;
    let mut events = app.events(&ticket_id).await;
    events.next().await;
    assert_eq!(
        app.sale.sale.streams.ticket_ids(),
        std::slice::from_ref(&ticket_id)
    );

    drop(events);

    assert!(app.sale.sale.streams.ticket_ids().is_empty());
}

pub async fn admissions_are_pushed_again_once_the_subscription_comes_back() {
    let mut proxy = Proxy::start().await;
    let sale = TestSale::start_at(&proxy.address(), crate::support::new_config()).await;
    proxy.cut();
    // The first subscribe fails: streams fall back to the periodic refresh until it is retried.
    let app = TestApp::from_sale(sale).await;
    proxy.restore().await;
    let ticket_id = eventually(|| app.sale.sale.queue.join("patient")).await;
    let ticket_id = ticket_id.ticket().unwrap().ticket_id.clone();
    let mut events = app.events(&ticket_id).await;
    assert_eq!(events.next().await["status"], "waiting");

    // The retry happens on the next refresh.
    app.sale.advance(Duration::from_secs(1));
    let channel = app.sale.sale.redis.admitted_channel();
    eventually(|| {
        let channel = channel.clone();
        async move {
            let (_, subscribers): (String, i64) = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(&channel)
                .query_async(&mut raw_redis().await)
                .await
                .unwrap();
            if subscribers == 1 {
                Ok(())
            } else {
                Err(subscribers)
            }
        }
    })
    .await;
    app.sale.sale.maintenance.run_once().await.unwrap();

    assert_eq!(events.next().await["status"], "admitted");
}

pub async fn a_view_the_code_does_not_understand_stops_the_push_loop() {
    let mut app = TestApp::start().await;
    let ticket_id = app.join("alice").await;
    let mut events = app.events(&ticket_id).await;
    events.next().await;
    let key = format!("cq:{{{}}}:ticket:{ticket_id}", app.sale.config().sale_id);
    let _: () = redis::cmd("HSET")
        .arg(&key)
        .arg("status")
        .arg("bogus")
        .query_async(&mut raw_redis().await)
        .await
        .unwrap();

    app.sale.advance(Duration::from_secs(1));
    let stopped = tokio::time::timeout(EVENT_TIMEOUT, app.background.join_next())
        .await
        .expect("the push loop stops");

    assert!(
        matches!(stopped, Some(Ok(Err(StoreError::Unexpected(_))))),
        "{stopped:?}"
    );
}
