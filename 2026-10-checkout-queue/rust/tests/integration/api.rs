//! Goes through the real router: strict JSON, error mapping, served-by header, wire names.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use checkout_queue::clock::{Clock, ManualClock};
use checkout_queue::config::Config;
use checkout_queue::sale_redis::{SaleRedis, StoreError};
use serde_json::json;

use crate::support::{Proxy, TestApp, eventually, raw_redis};

const UNKNOWN_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const JSON: &str = "application/json";

/// One malformed or unknown request and the status it must get, with the error envelope.
pub struct ErrorCase {
    pub name: &'static str,
    method: &'static str,
    /// `{unknown}` becomes a well-formed id that no ticket or order has.
    path: &'static str,
    /// Content type and body.
    body: Option<(&'static str, &'static str)>,
    expected: StatusCode,
}

const fn case(
    name: &'static str,
    method: &'static str,
    path: &'static str,
    body: Option<(&'static str, &'static str)>,
    expected: StatusCode,
) -> ErrorCase {
    ErrorCase {
        name,
        method,
        path,
        body,
        expected,
    }
}

const BAD: StatusCode = StatusCode::BAD_REQUEST;
const NOT_FOUND: StatusCode = StatusCode::NOT_FOUND;

pub const ERROR_CASES: &[ErrorCase] = &[
    case(
        "join_empty_user_id",
        "POST",
        "/queue/join",
        Some((JSON, r#"{"userId":""}"#)),
        BAD,
    ),
    case(
        "join_user_id_with_a_space",
        "POST",
        "/queue/join",
        Some((JSON, r#"{"userId":"has space"}"#)),
        BAD,
    ),
    case(
        "join_user_id_too_long",
        "POST",
        "/queue/join",
        Some((
            JSON,
            r#"{"userId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        )),
        BAD,
    ),
    case(
        "join_user_id_is_a_number",
        "POST",
        "/queue/join",
        Some((JSON, r#"{"userId":42}"#)),
        BAD,
    ),
    case(
        "join_user_id_is_null",
        "POST",
        "/queue/join",
        Some((JSON, r#"{"userId":null}"#)),
        BAD,
    ),
    case(
        "join_without_user_id",
        "POST",
        "/queue/join",
        Some((JSON, "{}")),
        BAD,
    ),
    case(
        "join_malformed_json",
        "POST",
        "/queue/join",
        Some((JSON, "{not json")),
        BAD,
    ),
    case(
        "join_json_null",
        "POST",
        "/queue/join",
        Some((JSON, "null")),
        BAD,
    ),
    case(
        "join_json_array",
        "POST",
        "/queue/join",
        Some((JSON, "[]")),
        BAD,
    ),
    case(
        "join_empty_json_body",
        "POST",
        "/queue/join",
        Some((JSON, "")),
        BAD,
    ),
    case("join_without_body", "POST", "/queue/join", None, BAD),
    case(
        "join_as_plain_text",
        "POST",
        "/queue/join",
        Some(("text/plain", r#"{"userId":"alice"}"#)),
        BAD,
    ),
    case(
        "ticket_malformed_id",
        "GET",
        "/queue/tickets/not-an-id",
        None,
        BAD,
    ),
    case(
        "ticket_uppercase_id",
        "GET",
        "/queue/tickets/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        None,
        BAD,
    ),
    case(
        "ticket_unknown",
        "GET",
        "/queue/tickets/{unknown}",
        None,
        NOT_FOUND,
    ),
    case(
        "events_unknown_ticket",
        "GET",
        "/queue/tickets/{unknown}/events",
        None,
        NOT_FOUND,
    ),
    case(
        "events_malformed_id",
        "GET",
        "/queue/tickets/not-an-id/events",
        None,
        BAD,
    ),
    case(
        "checkout_quantity_is_a_string",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}","quantity":"1"}"#)),
        BAD,
    ),
    case(
        "checkout_quantity_zero",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}","quantity":0}"#)),
        BAD,
    ),
    case(
        "checkout_quantity_negative",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}","quantity":-1}"#)),
        BAD,
    ),
    case(
        "checkout_quantity_over_the_limit",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}","quantity":3}"#)),
        BAD,
    ),
    case(
        "checkout_quantity_fractional",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}","quantity":1.5}"#)),
        BAD,
    ),
    case(
        "checkout_without_quantity",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}"}"#)),
        BAD,
    ),
    case(
        "checkout_malformed_ticket_id",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"ABC","quantity":1}"#)),
        BAD,
    ),
    case(
        "checkout_unknown_ticket",
        "POST",
        "/checkout",
        Some((JSON, r#"{"ticketId":"{unknown}","quantity":1}"#)),
        NOT_FOUND,
    ),
    case("order_malformed_id", "GET", "/orders/not-an-id", None, BAD),
    case("order_unknown", "GET", "/orders/{unknown}", None, NOT_FOUND),
    case(
        "pay_malformed_id",
        "POST",
        "/orders/not-an-id/pay",
        None,
        BAD,
    ),
    case(
        "pay_unknown_order",
        "POST",
        "/orders/{unknown}/pay",
        None,
        NOT_FOUND,
    ),
];

pub async fn error_case(case: &ErrorCase) {
    let app = TestApp::start().await;
    let path = case.path.replace("{unknown}", UNKNOWN_ID);
    let body = case
        .body
        .map(|(content_type, body)| (content_type, body.replace("{unknown}", UNKNOWN_ID)));

    let response = app
        .send(
            case.method,
            &path,
            body.as_ref().map(|(t, b)| (*t, b.as_str())),
        )
        .await;

    let code = response.error_code(case.expected);
    let expected_code = if case.expected == BAD {
        "invalid_request"
    } else if case.path.starts_with("/orders") {
        "order_not_found"
    } else {
        "ticket_not_found"
    };
    assert_eq!(code, expected_code);
    assert_eq!(response.header("x-served-by"), "test-instance");
}

pub async fn health_names_the_instance() {
    let app = TestApp::start().await;

    let response = app.get("/health").await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.header("x-served-by"), "test-instance");
    let body = response.json();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["instance"], "test-instance");
    assert_eq!(
        body["saleId"].as_str(),
        Some(app.sale.config().sale_id.as_str())
    );
}

pub async fn full_flow_join_wait_checkout_pay() {
    let app = TestApp::start().await;

    let join = app
        .post_json("/queue/join", &json!({ "userId": "alice" }))
        .await;
    let ticket = join.json();
    let ticket_id = ticket["ticketId"].as_str().unwrap().to_owned();
    assert_eq!(join.status, StatusCode::CREATED);
    assert_eq!(
        join.header("location"),
        format!("/queue/tickets/{ticket_id}")
    );
    assert_eq!(ticket["status"], "waiting");
    assert_eq!(ticket["position"], 1);
    assert!(
        ticket.get("orderId").is_none(),
        "null fields are left out: {ticket}"
    );
    let again = app
        .post_json("/queue/join", &json!({ "userId": "alice" }))
        .await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.json()["ticketId"].as_str(), Some(ticket_id.as_str()));

    app.sale.sale.maintenance.run_once().await.unwrap();
    let admitted = app.get(&format!("/queue/tickets/{ticket_id}")).await.json();
    assert_eq!(admitted["status"], "admitted");

    let checkout = app
        .post_json(
            "/checkout",
            &json!({ "ticketId": ticket_id, "quantity": 2 }),
        )
        .await;
    let order = checkout.json();
    let order_id = order["orderId"].as_str().unwrap().to_owned();
    assert_eq!(checkout.status, StatusCode::CREATED);
    assert_eq!(checkout.header("location"), format!("/orders/{order_id}"));
    assert_eq!(order["status"], "pending_payment");

    let repeat = app
        .post_json(
            "/checkout",
            &json!({ "ticketId": ticket_id, "quantity": 2 }),
        )
        .await;
    assert_eq!(repeat.status, StatusCode::OK);
    assert_eq!(repeat.json()["orderId"].as_str(), Some(order_id.as_str()));

    let pay = app.post(&format!("/orders/{order_id}/pay")).await;
    let pay_again = app.post(&format!("/orders/{order_id}/pay")).await;
    assert_eq!(pay.status, StatusCode::CREATED);
    assert_eq!(pay.header("location"), format!("/orders/{order_id}"));
    assert_eq!(pay_again.status, StatusCode::OK);
    let paid = app.get(&format!("/orders/{order_id}")).await.json();
    assert_eq!(paid["status"], "paid");
    assert!(paid["paidAtMs"].is_i64());

    let audit = app.get("/admin/audit").await.json();
    assert_eq!(audit["stock"], 8);
    assert_eq!(audit["oversold"], 0);
    assert_eq!(audit["charges"], 1);
    assert_eq!(audit["stockBalanced"], true);
}

pub async fn checkout_before_admission_is_403() {
    let app = TestApp::start().await;
    let ticket_id = app.join("eager").await;

    let response = app
        .post_json(
            "/checkout",
            &json!({ "ticketId": ticket_id, "quantity": 1 }),
        )
        .await;

    assert_eq!(response.error_code(StatusCode::FORBIDDEN), "not_admitted");
    assert!(
        response.json()["error"]
            .as_str()
            .unwrap()
            .contains("waiting")
    );
}

pub async fn checkout_after_the_admission_window_is_410() {
    let app = TestApp::start().await;
    let ticket_id = app.join("slow").await;
    app.sale.sale.maintenance.run_once().await.unwrap();
    app.sale.advance(Duration::from_secs(60));

    let response = app
        .post_json(
            "/checkout",
            &json!({ "ticketId": ticket_id, "quantity": 1 }),
        )
        .await;

    assert_eq!(response.error_code(StatusCode::GONE), "admission_expired");
}

pub async fn checkout_conflicts_are_reported_with_their_code() {
    let app = TestApp::start_with(|c| Config {
        initial_stock: 3,
        ..c
    })
    .await;
    let first = app.join("first").await;
    let second = app.join("second").await;
    app.sale.sale.maintenance.run_once().await.unwrap();
    app.post_json("/checkout", &json!({ "ticketId": first, "quantity": 2 }))
        .await;

    let changed_mind = app
        .post_json("/checkout", &json!({ "ticketId": first, "quantity": 1 }))
        .await;
    let too_many = app
        .post_json("/checkout", &json!({ "ticketId": second, "quantity": 2 }))
        .await;

    assert_eq!(changed_mind.error_code(StatusCode::CONFLICT), "ticket_used");
    assert_eq!(
        too_many.error_code(StatusCode::CONFLICT),
        "insufficient_stock"
    );
}

pub async fn checkout_when_sold_out_is_409() {
    let app = TestApp::start_with(|c| Config {
        initial_stock: 2,
        ..c
    })
    .await;
    let first = app.join("first").await;
    let second = app.join("second").await;
    app.sale.sale.maintenance.run_once().await.unwrap();
    let order = app
        .post_json("/checkout", &json!({ "ticketId": first, "quantity": 2 }))
        .await
        .json();
    app.post(&format!(
        "/orders/{}/pay",
        order["orderId"].as_str().unwrap()
    ))
    .await;

    let response = app
        .post_json("/checkout", &json!({ "ticketId": second, "quantity": 1 }))
        .await;

    assert_eq!(response.error_code(StatusCode::CONFLICT), "sold_out");
}

pub async fn join_when_sold_out_is_409() {
    let app = TestApp::start_with(|c| Config {
        initial_stock: 0,
        ..c
    })
    .await;

    let response = app
        .post_json("/queue/join", &json!({ "userId": "late" }))
        .await;

    assert_eq!(response.error_code(StatusCode::CONFLICT), "sold_out");
}

pub async fn pay_after_the_deadline_is_410() {
    let app = TestApp::start().await;
    let ticket_id = app.join("late-payer").await;
    app.sale.sale.maintenance.run_once().await.unwrap();
    let order = app
        .post_json(
            "/checkout",
            &json!({ "ticketId": ticket_id, "quantity": 1 }),
        )
        .await
        .json();
    app.sale.advance(Duration::from_secs(120));

    let response = app
        .post(&format!(
            "/orders/{}/pay",
            order["orderId"].as_str().unwrap()
        ))
        .await;

    assert_eq!(response.error_code(StatusCode::GONE), "order_expired");
}

pub async fn field_names_match_case_insensitively_like_the_dotnet_version() {
    let app = TestApp::start().await;

    let response = app
        .send("POST", "/queue/join", Some((JSON, r#"{"UserId":"alice"}"#)))
        .await;

    assert_eq!(response.status, StatusCode::CREATED, "{}", response.text());
}

pub async fn redis_down_is_503_not_a_crash_and_recovers() {
    let mut proxy = Proxy::start().await;
    let app = TestApp::start_at(&proxy.address(), |c| c).await;
    assert_eq!(app.get("/health").await.status, StatusCode::OK);

    proxy.cut();
    let health = app.get("/health").await;
    let join = app
        .post_json("/queue/join", &json!({ "userId": "alice" }))
        .await;
    let board = app.get("/admin/tickets").await;

    assert_eq!(
        health.error_code(StatusCode::SERVICE_UNAVAILABLE),
        "unavailable"
    );
    assert_eq!(
        join.error_code(StatusCode::SERVICE_UNAVAILABLE),
        "unavailable"
    );
    assert_eq!(
        board.error_code(StatusCode::SERVICE_UNAVAILABLE),
        "unavailable"
    );
    assert_eq!(
        join.json()["error"].as_str(),
        Some("storage unavailable, retry shortly")
    );

    proxy.restore().await;
    eventually(|| async {
        match app.get("/health").await.status {
            StatusCode::OK => Ok(()),
            other => Err(other),
        }
    })
    .await;
    let join = app
        .post_json("/queue/join", &json!({ "userId": "alice" }))
        .await;
    assert_eq!(join.status, StatusCode::CREATED, "{}", join.text());
}

pub async fn connecting_to_an_unreachable_redis_fails_fast() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(0));
    let started = Instant::now();

    let result = SaleRedis::connect("redis://127.0.0.1:1", "demo", clock).await;

    assert!(matches!(result, Err(StoreError::Unavailable(_))));
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
}

pub async fn unknown_routes_are_404_and_still_name_the_instance() {
    let app = TestApp::start().await;

    let response = app.get("/nowhere").await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_eq!(response.header("x-served-by"), "test-instance");
}

pub async fn a_reply_the_code_does_not_understand_is_a_500_with_the_error_body() {
    let app = TestApp::start().await;
    let ticket_id = app.join("alice").await;
    let key = format!("cq:{{{}}}:ticket:{ticket_id}", app.sale.config().sale_id);
    let _: () = redis::cmd("HSET")
        .arg(&key)
        .arg("status")
        .arg("bogus")
        .query_async(&mut raw_redis().await)
        .await
        .unwrap();

    let response = app.get(&format!("/queue/tickets/{ticket_id}")).await;

    assert_eq!(
        response.error_code(StatusCode::INTERNAL_SERVER_ERROR),
        "internal"
    );
}
