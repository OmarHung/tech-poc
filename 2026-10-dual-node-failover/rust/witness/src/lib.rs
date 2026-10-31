//! Witness: the third vote that lets two nodes fail over without both becoming primary.

pub mod config;
pub mod lease_table;

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use shared::error::ApiError;
use shared::http::{read_json, with_served_by};
use shared::wire::{LeaseReply, LeaseRequest, SyncReply, SyncRequest};

use crate::lease_table::{LeaseTable, WitnessHealth};

// No auth: the PoC binds to 127.0.0.1 only. A real witness must only accept the two nodes (mTLS or similar).
pub fn router(table: Arc<LeaseTable>) -> Router {
    let router = Router::new()
        .route("/health", get(health))
        .route("/lease", post(lease))
        .route("/sync", post(sync))
        .with_state(table);
    with_served_by(router, "witness")
}

async fn health(State(table): State<Arc<LeaseTable>>) -> Json<WitnessHealth> {
    Json(table.health())
}

async fn lease(
    State(table): State<Arc<LeaseTable>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<LeaseReply>, ApiError> {
    let request: LeaseRequest = read_json(&headers, &body)?;
    Ok(Json(table.acquire(request.node.as_deref())?))
}

async fn sync(
    State(table): State<Arc<LeaseTable>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<SyncReply>, ApiError> {
    let request: SyncRequest = read_json(&headers, &body)?;
    Ok(Json(table.set_standby_in_sync(&request)?))
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::config::WitnessConfig;

    async fn call(
        app: &Router,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> (StatusCode, String, Value) {
        let mut request = Request::builder().method(method).uri(path);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        let request = request
            .body(Body::from(body.unwrap_or("").to_owned()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let served_by = response.headers()["x-served-by"]
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            served_by,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn app() -> (tempfile::TempDir, Router) {
        let dir = tempfile::tempdir().unwrap();
        let table = LeaseTable::open(WitnessConfig {
            data_dir: dir.path().to_owned(),
            ..WitnessConfig::default()
        })
        .unwrap();
        (dir, router(Arc::new(table)))
    }

    #[tokio::test]
    async fn grants_the_lease_and_records_sync_reports_over_http() {
        let (_dir, app) = app();

        let (status, served_by, lease) =
            call(&app, "POST", "/lease", Some(r#"{"node":"a"}"#)).await;
        assert_eq!((status, served_by.as_str()), (StatusCode::OK, "witness"));
        assert_eq!(lease["granted"], json!(true));
        assert_eq!(lease["ttlMs"], json!(3000));

        let body = r#"{"node":"a","epoch":1,"inSync":true,"expectedVersion":1}"#;
        let (status, _, sync) = call(&app, "POST", "/sync", Some(body)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            sync,
            json!({"epoch": 1, "standbyInSync": true, "syncVersion": 2})
        );

        let (_, _, health) = call(&app, "GET", "/health", None).await;
        assert_eq!(health["service"], json!("witness"));
        assert_eq!(health["holder"], json!("a"));
        assert_eq!(health["standbyInSync"], json!(true));
    }

    #[tokio::test]
    async fn refusals_and_bad_input_have_their_status() {
        let (_dir, app) = app();

        let stale = r#"{"node":"b","epoch":1,"inSync":false}"#;
        let (status, _, body) = call(&app, "POST", "/sync", Some(stale)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("not the lease holder")
        );

        for bad in [r#"{"node":"c"}"#, r#"{"node":1}"#, "nope"] {
            let (status, _, body) = call(&app, "POST", "/lease", Some(bad)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
            assert!(body["error"].is_string());
        }
        let (status, _, _) = call(&app, "POST", "/lease", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
