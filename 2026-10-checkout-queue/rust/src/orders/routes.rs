use std::sync::Arc;

use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use super::{CheckoutResult, OrderView, PayResult};
use crate::app::Sale;
use crate::http::{ApiError, JsonBody, path_id, valid_id};

pub fn routes() -> Router<Arc<Sale>> {
    Router::new()
        .route("/checkout", post(checkout))
        .route("/orders/{order_id}", get(order))
        .route("/orders/{order_id}/pay", post(pay))
}

async fn checkout(State(sale): State<Arc<Sale>>, body: JsonBody) -> Result<Response, ApiError> {
    let ticket_id = valid_id(body.string("ticketId")?, "ticketId")?;
    let max = sale.config.max_per_order;
    let quantity = body
        .integer("quantity")?
        .and_then(|q| u32::try_from(q).ok())
        .filter(|q| (1..=max).contains(q))
        .ok_or_else(|| {
            ApiError::bad_request(format!("quantity must be an integer from 1 to {max}"))
        })?;

    match sale.orders.checkout(&ticket_id, quantity).await? {
        CheckoutResult::Created(order) => Ok(created(order)),
        CheckoutResult::Existing(order) => Ok(Json(order).into_response()),
        CheckoutResult::TicketNotFound => {
            Err(ApiError::not_found("ticket_not_found", "ticket not found"))
        }
        CheckoutResult::NotAdmitted { status } => Err(ApiError::forbidden(
            "not_admitted",
            format!("ticket is not admitted to checkout (status: {status})"),
        )),
        CheckoutResult::AdmissionExpired => Err(ApiError::gone(
            "admission_expired",
            "checkout window has closed; join the queue again",
        )),
        CheckoutResult::TicketUsed { order_id } => Err(ApiError::conflict(
            "ticket_used",
            format!("this ticket already placed order {order_id} with a different quantity"),
        )),
        CheckoutResult::SoldOut => Err(ApiError::conflict("sold_out", "sold out")),
        CheckoutResult::InsufficientStock { left } => Err(ApiError::conflict(
            "insufficient_stock",
            format!("only {left} left right now; stock held by unpaid orders may come back"),
        )),
    }
}

async fn order(
    State(sale): State<Arc<Sale>>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<OrderView>, ApiError> {
    let order_id = path_id(path, "order id")?;
    sale.orders
        .order(&order_id)
        .await?
        .map(Json)
        .ok_or_else(order_not_found)
}

/// 201 when this request charged, 200 when an earlier one already had: retries are safe.
async fn pay(
    State(sale): State<Arc<Sale>>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let order_id = path_id(path, "order id")?;
    match sale.orders.pay(&order_id).await? {
        PayResult::Paid(order) => Ok(created(order)),
        PayResult::AlreadyPaid(order) => Ok(Json(order).into_response()),
        PayResult::OrderExpired(_) => Err(ApiError::gone(
            "order_expired",
            "payment window has closed; the stock was released",
        )),
        PayResult::OrderNotFound => Err(order_not_found()),
    }
}

fn created(order: OrderView) -> Response {
    let location = format!("/orders/{}", order.order_id);
    (
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(order),
    )
        .into_response()
}

fn order_not_found() -> ApiError {
    ApiError::not_found("order_not_found", "order not found")
}
