//! `GET /queue/tickets/{ticketId}/events`: the ticket as server-sent events, the push alternative
//! to polling. Each event's data is the same JSON as `GET /queue/tickets/{ticketId}`. The stream
//! ends once the ticket can no longer change.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderName, header};
use axum::response::{IntoResponse, Response};
use futures_util::Stream;
use futures_util::stream;

use super::routes::ticket_not_found;
use super::{TicketStream, TicketView};
use crate::app::Sale;
use crate::http::{ApiError, path_id};

/// Proxies and load balancers drop connections that stay silent for too long.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const KEEP_ALIVE: &[u8] = b": keep-alive\n\n";
const X_ACCEL_BUFFERING: HeaderName = HeaderName::from_static("x-accel-buffering");

pub async fn stream(
    State(sale): State<Arc<Sale>>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let ticket_id = path_id(path, "ticket id")?;
    // Open the stream before reading the ticket, so no change can slip in between the two.
    let updates = sale.streams.open(&ticket_id);
    let first = sale
        .queue
        .ticket(&ticket_id)
        .await?
        .ok_or_else(ticket_not_found)?;
    let headers = [
        (header::CONTENT_TYPE, "text/event-stream"),
        (header::CACHE_CONTROL, "no-cache"),
        (X_ACCEL_BUFFERING, "no"),
    ];
    Ok((headers, Body::from_stream(events(first, updates))).into_response())
}

struct Events {
    last: TicketView,
    updates: TicketStream,
    started: bool,
}

/// The first view at once, then every pushed view that differs from the last one written,
/// with a keep-alive comment after 15 quiet seconds. Dropped (client gone) means unregistered.
fn events(
    first: TicketView,
    updates: TicketStream,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let state = Events {
        last: first,
        updates,
        started: false,
    };
    stream::unfold(state, |mut state| async move {
        if !state.started {
            state.started = true;
            let event = data(&state.last);
            return Some((Ok(event), state));
        }
        if state.last.status.is_final() {
            return None;
        }
        loop {
            match tokio::time::timeout(KEEP_ALIVE_INTERVAL, state.updates.next()).await {
                Err(_quiet) => return Some((Ok(Bytes::from_static(KEEP_ALIVE)), state)),
                Ok(None) => return None,
                Ok(Some(view)) if view == state.last => {}
                Ok(Some(view)) => {
                    let event = data(&view);
                    state.last = view;
                    return Some((Ok(event), state));
                }
            }
        }
    })
}

fn data(view: &TicketView) -> Bytes {
    let json = serde_json::to_string(view).expect("a ticket view always serializes");
    Bytes::from(format!("data: {json}\n\n"))
}
