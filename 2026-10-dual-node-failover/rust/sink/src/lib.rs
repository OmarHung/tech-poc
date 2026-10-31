//! Sink: where results land. Applies each key once and counts every extra copy, so the cost of
//! redundancy is visible.

pub mod result_store;

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use shared::env::{ConfigError, EnvReader};
use shared::error::ApiError;
use shared::http::{read_json, with_served_by};
use shared::work::{ResultRecord, ResultReply};

use crate::result_store::{ResultStatus, ResultStore, SinkSnapshot, SinkSummary};

pub const DEFAULT_PORT: u16 = 8123;

/// The sink's only setting: `PORT`.
pub fn port_from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<u16, ConfigError> {
    EnvReader::new(lookup).port("PORT", DEFAULT_PORT)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SinkHealth {
    status: &'static str,
    service: &'static str,
    summary: SinkSummary,
}

// No auth: the PoC binds to 127.0.0.1 only.
pub fn router(store: Arc<ResultStore>) -> Router {
    let router = Router::new()
        .route("/health", get(health))
        .route("/results", get(snapshot).post(record))
        .with_state(store);
    with_served_by(router, "sink")
}

async fn health(State(store): State<Arc<ResultStore>>) -> Json<SinkHealth> {
    Json(SinkHealth {
        status: "ok",
        service: "sink",
        summary: store.summary(),
    })
}

/// 201 for the first copy, 200 for a duplicate, 409 for a conflict or a stale attempt; the body always
/// says which.
async fn record(
    State(store): State<Arc<ResultStore>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<ResultReply>), ApiError> {
    let request: ResultRecord = read_json(&headers, &body)?;
    let status = store.record(&request)?;
    let code = match status {
        ResultStatus::Applied => StatusCode::CREATED,
        ResultStatus::Duplicate => StatusCode::OK,
        ResultStatus::Conflict | ResultStatus::Stale => StatusCode::CONFLICT,
    };
    let reply = ResultReply {
        status: status.wire_name().to_owned(),
    };
    Ok((code, Json(reply)))
}

async fn snapshot(State(store): State<Arc<ResultStore>>) -> Json<SinkSnapshot> {
    Json(store.snapshot())
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    async fn call(
        app: &Router,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(path);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        let request = request
            .body(Body::from(body.unwrap_or("").to_owned()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        assert_eq!(response.headers()["x-served-by"], "sink");
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn post(node: &str, attempt: i32, value: &str) -> String {
        json!({"key": "event-1", "node": node, "attempt": attempt, "value": value}).to_string()
    }

    #[tokio::test]
    async fn results_answer_with_their_status() {
        let app = router(Arc::new(ResultStore::with_clock(Box::new(|| 42))));

        let applied = call(&app, "POST", "/results", Some(&post("a", 1, "x:1"))).await;
        let duplicate = call(&app, "POST", "/results", Some(&post("b", 1, "x:1"))).await;
        let conflict = call(&app, "POST", "/results", Some(&post("b", 1, "x:2"))).await;
        let stale = call(&app, "POST", "/results", Some(&post("b", 0, "x:1"))).await;
        let (_, snapshot) = call(&app, "GET", "/results", None).await;
        let (_, health) = call(&app, "GET", "/health", None).await;

        assert_eq!(applied, (StatusCode::CREATED, json!({"status": "applied"})));
        assert_eq!(duplicate, (StatusCode::OK, json!({"status": "duplicate"})));
        assert_eq!(
            conflict,
            (StatusCode::CONFLICT, json!({"status": "conflict"}))
        );
        assert_eq!(stale, (StatusCode::CONFLICT, json!({"status": "stale"})));
        let summary =
            json!({"received": 4, "applied": 1, "duplicates": 1, "conflicts": 1, "stale": 1});
        assert_eq!(
            snapshot,
            json!({
                "summary": summary,
                "results": {"event-1": {"value": "x:1", "node": "a", "attempt": 1, "firstAtMs": 42,
                                        "copies": 4, "conflicts": 1, "stale": 1}}
            })
        );
        assert_eq!(
            health,
            json!({"status": "ok", "service": "sink", "summary": summary})
        );
    }

    #[tokio::test]
    async fn bad_results_are_400() {
        let app = router(Arc::new(ResultStore::new()));

        for body in [
            Some(r#"{"key":"bad key","node":"a","attempt":0,"value":"v"}"#),
            Some(r#"{"key":"k","node":"a","attempt":"0","value":"v"}"#),
            Some(r#"{"key":"k","node":"a","attempt":0,"value":5}"#),
            Some("not json"),
            None,
        ] {
            let (status, reply) = call(&app, "POST", "/results", body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
            assert!(reply["error"].is_string(), "{body:?}");
        }
    }

    #[test]
    fn the_port_comes_from_the_environment() {
        assert_eq!(port_from_lookup(|_| None), Ok(DEFAULT_PORT));
        assert_eq!(port_from_lookup(|_| Some("9123".into())), Ok(9123));
        assert!(
            port_from_lookup(|_| Some("x".into()))
                .unwrap_err()
                .0
                .starts_with("PORT")
        );
    }
}
