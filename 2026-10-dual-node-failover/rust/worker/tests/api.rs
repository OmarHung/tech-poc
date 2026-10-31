//! The real HTTP pipeline and startup checks of one worker, without its work loop.

use std::collections::BTreeMap;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;
use worker::config::{WorkerConfig, WorkerMode};
use worker::hosting::{StartupError, Worker};
use worker::hothot::ledger::Ledger;
use worker::status::TaskPhase;

fn config(dir: &TempDir, mode: WorkerMode) -> WorkerConfig {
    WorkerConfig {
        worker_id: "b".into(),
        mode,
        data_dir: dir.path().to_owned(),
        ..WorkerConfig::default()
    }
}

async fn get(app: &Router, path: &str) -> (StatusCode, String, Value) {
    let request = Request::builder().uri(path).body(Body::empty()).unwrap();
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

#[tokio::test]
async fn health_reports_the_current_task_and_says_who_answered() {
    let dir = tempfile::tempdir().unwrap();
    let worker = Worker::open(config(&dir, WorkerMode::Dispatch)).unwrap();
    worker.status().task_phase(4, TaskPhase::Delivering);
    worker.status().sink_replied("applied");

    let (status, served_by, health) = get(&worker.router(), "/health").await;

    assert_eq!((status, served_by.as_str()), (StatusCode::OK, "worker-b"));
    assert_eq!(
        health,
        json!({"status": "ok", "worker": "b", "mode": "dispatch", "offset": 0,
               "sinkReplies": {"applied": 1}, "completed": 0, "abandoned": 0, "staleCompletions": 0,
               "currentTask": 4, "phase": "delivering"})
    );
}

#[tokio::test]
async fn state_is_the_checkpoint_a_hot_hot_worker_starts_from() {
    let dir = tempfile::tempdir().unwrap();
    let saved = Ledger {
        offset: 7,
        balances: BTreeMap::from([("acct-1".into(), 12), ("acct-2".into(), -3)]),
    };
    shared::state_file::save(&dir.path().join("checkpoint.json"), &saved).unwrap();
    let worker = Worker::open(config(&dir, WorkerMode::HotHot)).unwrap();

    let (status, _, state) = get(&worker.router(), "/state").await;
    let (_, _, health) = get(&worker.router(), "/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        state,
        json!({"offset": 7, "balances": {"acct-1": 12, "acct-2": -3}})
    );
    assert_eq!(
        (&health["offset"], &health["mode"], &health["currentTask"]),
        (&json!(7), &json!("hothot"), &Value::Null)
    );
}

#[test]
fn a_hot_hot_worker_refuses_a_checkpoint_it_cannot_read() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("checkpoint.json"), r#"{"offset":"7"}"#).unwrap();

    let error = Worker::open(config(&dir, WorkerMode::HotHot))
        .err()
        .unwrap();

    assert!(matches!(error, StartupError::Checkpoint(_)), "{error}");
    assert!(error.to_string().contains("checkpoint.json"), "{error}");
    // A dispatched worker has no checkpoint to read.
    assert!(Worker::open(config(&dir, WorkerMode::Dispatch)).is_ok());
}

#[test]
fn a_bad_worker_id_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let bad = WorkerConfig {
        worker_id: "Worker A".into(),
        ..config(&dir, WorkerMode::Dispatch)
    };

    let error = Worker::open(bad).err().unwrap();

    assert!(matches!(error, StartupError::Config(_)), "{error}");
    assert!(error.to_string().starts_with("WORKER_ID"), "{error}");
}
