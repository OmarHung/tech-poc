//! The real HTTP pipeline of one node, with election and replication driven by the test.

use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use node::config::{ArbiterMode, NodeConfig};
use node::hosting::Node;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

struct Fixture {
    _dir: TempDir,
    node: Node,
    app: Router,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            data_dir: dir.path().to_owned(),
            arbiter: ArbiterMode::None,
            ..NodeConfig::default()
        };
        let node = Node::open(config).unwrap();
        let app = node.router();
        Self {
            _dir: dir,
            node,
            app,
        }
    }

    /// What the election and replication loops would do on promotion without a witness.
    async fn promote(&self) {
        let state = self.node.state();
        assert!(state.try_promote(state.epoch() + 1, Duration::MAX, false, 0));
        assert!(state.current_term().unwrap().try_go_solo().await);
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        content_type: Option<&str>,
        body: &str,
    ) -> (StatusCode, String, Value) {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = self
            .app
            .clone()
            .oneshot(request.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
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

    async fn put(&self, path: &str, body: &str) -> (StatusCode, String, Value) {
        self.call("PUT", path, Some("application/json"), body).await
    }
}

#[tokio::test]
async fn standby_answers_503_and_says_who_it_is() {
    let f = Fixture::new();

    let (put_status, served_by, put_body) = f.put("/kv/k", r#"{"value":"v"}"#).await;
    let (get_status, _, _) = f.call("GET", "/kv/k", None, "").await;

    assert_eq!(put_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(put_body, json!({"error": "not primary"}));
    assert_eq!(get_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(served_by, "node-a");
}

#[tokio::test]
async fn primary_stores_and_returns_values() {
    let f = Fixture::new();
    f.promote().await;

    let (put_status, _, item) = f.put("/kv/order-1", r#"{"value":"paid"}"#).await;
    let (get_status, _, got) = f.call("GET", "/kv/order-1", None, "").await;

    assert_eq!(put_status, StatusCode::OK);
    assert_eq!(
        item,
        json!({"key": "order-1", "value": "paid", "index": 1, "epoch": 1})
    );
    assert_eq!((get_status, got), (StatusCode::OK, item));
}

#[tokio::test]
async fn missing_key_is_404() {
    let f = Fixture::new();
    f.promote().await;

    let (status, _, body) = f.call("GET", "/kv/nothing", None, "").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error": "key 'nothing' not found"}));
}

#[tokio::test]
async fn bad_writes_are_400() {
    let f = Fixture::new();
    f.promote().await;
    let too_long = json!({"value": "x".repeat(4097)}).to_string();
    let cases = [
        ("/kv/bad%20key", r#"{"value":"v"}"#),
        ("/kv/k", r#"{"value":1}"#),
        ("/kv/k", "{}"),
        ("/kv/k", "not json"),
        ("/kv/k", "null"),
        ("/kv/k", too_long.as_str()),
        ("/kv/%FF", r#"{"value":"v"}"#),
    ];

    for (path, body) in cases {
        let (status, _, reply) = f.put(path, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {body}");
        assert!(reply["error"].is_string(), "{path} {body}: {reply}");
    }
    let (status, _, _) = f
        .call("PUT", "/kv/k", Some("text/plain"), r#"{"value":"v"}"#)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _, _) = f.call("GET", "/kv/bad%20key", None, "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn replicate_applies_entries_and_admin_data_shows_them() {
    let f = Fixture::new();
    let request = json!({"leader": "b", "epoch": 2, "prevIndex": 0, "prevEpoch": 0,
        "entries": [{"index": 1, "epoch": 2, "key": "k", "value": "v"}], "leaderLastIndex": 1})
    .to_string();

    let (status, _, reply) = f
        .call(
            "POST",
            "/internal/replicate",
            Some("application/json"),
            &request,
        )
        .await;
    let (_, _, again) = f
        .call(
            "POST",
            "/internal/replicate",
            Some("application/json"),
            &request,
        )
        .await;
    let (_, _, data) = f.call("GET", "/admin/data", None, "").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        reply,
        json!({"status": "ok", "epoch": 2, "matchIndex": 1, "promiseMs": 3000, "reason": null})
    );
    assert_eq!(again["status"], json!("ok"));
    assert_eq!(
        data,
        json!({"node": "a", "role": "standby", "epoch": 2, "lastIndex": 1, "data": {"k": "v"}})
    );
}

#[tokio::test]
async fn replicate_rejects_malformed_batches() {
    let f = Fixture::new();
    let bad = json!({"epoch": 1, "prevIndex": 0, "entries": [{"index": 2, "epoch": 1, "key": "k", "value": "v"}], "leaderLastIndex": 2});

    let (status, _, reply) = f
        .call(
            "POST",
            "/internal/replicate",
            Some("application/json"),
            &bad.to_string(),
        )
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        reply["error"]
            .as_str()
            .unwrap()
            .contains("does not continue")
    );
}

#[tokio::test]
async fn health_reports_role_and_modes() {
    let f = Fixture::new();

    let (status, served_by, health) = f.call("GET", "/health", None, "").await;

    assert_eq!((status, served_by.as_str()), (StatusCode::OK, "node-a"));
    assert_eq!(health["role"], json!("standby"));
    assert_eq!(health["arbiter"], json!("none"));
    assert_eq!(health["replication"], json!("sync"));
}

#[tokio::test]
async fn unknown_paths_are_404_and_say_who_answered() {
    let f = Fixture::new();

    let (status, served_by, _) = f.call("GET", "/nope", None, "").await;

    assert_eq!(
        (status, served_by.as_str()),
        (StatusCode::NOT_FOUND, "node-a")
    );
}

#[test]
fn a_corrupt_log_stops_the_node_from_starting() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("log.jsonl"), "garbage\n{}\n").unwrap();
    let config = NodeConfig {
        data_dir: dir.path().to_owned(),
        ..NodeConfig::default()
    };

    let error = Node::open(config)
        .err()
        .expect("corrupt log must be refused");

    assert!(error.to_string().contains("line 1 is corrupt"), "{error}");
}
