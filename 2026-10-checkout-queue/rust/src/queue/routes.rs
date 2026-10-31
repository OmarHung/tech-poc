use std::sync::Arc;

use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use super::{JoinResult, TicketView, events};
use crate::app::Sale;
use crate::http::{ApiError, JsonBody, path_id, valid_user_id};

pub fn routes() -> Router<Arc<Sale>> {
    Router::new()
        .route("/queue/join", post(join))
        .route("/queue/tickets/{ticket_id}", get(ticket))
        .route("/queue/tickets/{ticket_id}/events", get(events::stream))
}

async fn join(State(sale): State<Arc<Sale>>, body: JsonBody) -> Result<Response, ApiError> {
    let user_id = valid_user_id(body.string("userId")?)?;
    match sale.queue.join(&user_id).await? {
        JoinResult::Created(view) => {
            let location = format!("/queue/tickets/{}", view.ticket_id);
            Ok((
                StatusCode::CREATED,
                [(header::LOCATION, location)],
                Json(view),
            )
                .into_response())
        }
        JoinResult::Existing(view) => Ok(Json(view).into_response()),
        JoinResult::SoldOut => Err(ApiError::conflict("sold_out", "sold out")),
    }
}

/// Polling doubles as the heartbeat.
async fn ticket(
    State(sale): State<Arc<Sale>>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<TicketView>, ApiError> {
    let ticket_id = path_id(path, "ticket id")?;
    sale.queue
        .ticket(&ticket_id)
        .await?
        .map(Json)
        .ok_or_else(ticket_not_found)
}

pub(super) fn ticket_not_found() -> ApiError {
    ApiError::not_found("ticket_not_found", "ticket not found")
}
