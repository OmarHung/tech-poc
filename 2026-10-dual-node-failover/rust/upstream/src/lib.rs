//! Upstream: where the work comes from. An event log both hot-hot workers read in full, and a task queue
//! that leases each task to one worker. In production this would be Kafka, a message queue or a job
//! table, itself replicated.

pub mod config;
pub mod event_log;
pub mod task_queue;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use shared::error::ApiError;
use shared::http::{read_json, with_served_by};
use shared::supervise::spawn_supervised;
use shared::work::{
    EventPage, LoggedEvent, NewEvent, NewTask, NewTaskReply, TaskAttempt, TaskLeaseRequest,
};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

use crate::config::UpstreamConfig;
use crate::event_log::EventLog;
use crate::task_queue::{TaskQueue, TaskStats};

/// The longest a reader or a lease request may ask to wait for new work.
pub const MAX_WAIT: Duration = Duration::from_secs(10);

/// Events read per request when the reader does not say.
const DEFAULT_PAGE: i32 = 100;

/// The event log and the task queue, plus the loop that requeues expired task leases.
pub struct Upstream {
    config: UpstreamConfig,
    events: Arc<EventLog>,
    tasks: Arc<TaskQueue>,
}

impl Upstream {
    pub fn new(config: UpstreamConfig) -> Self {
        let tasks = Arc::new(TaskQueue::new(config.task_lease_ttl));
        Self {
            config,
            events: Arc::new(EventLog::new()),
            tasks,
        }
    }

    pub fn events(&self) -> &Arc<EventLog> {
        &self.events
    }

    pub fn tasks(&self) -> &Arc<TaskQueue> {
        &self.tasks
    }

    /// Requeues expired task leases even when no worker is asking, so a waiting worker is woken right
    /// away. Leases also expire on every queue call; this covers the quiet moments. Aborting the
    /// handle stops it.
    pub fn spawn_reaper(&self) -> JoinHandle<()> {
        let (tasks, every) = (Arc::clone(&self.tasks), self.config.reap_interval);
        spawn_supervised("lease reaper", move || reap(Arc::clone(&tasks), every))
    }

    // No auth: the PoC binds to 127.0.0.1 only.
    pub fn router(&self) -> Router {
        let app = AppState {
            events: Arc::clone(&self.events),
            tasks: Arc::clone(&self.tasks),
        };
        let router = Router::new()
            .route("/health", get(health))
            .route("/events", post(append_event).get(read_events))
            .route("/tasks", post(add_task))
            .route("/tasks/lease", post(lease_task))
            .route("/tasks/stats", get(task_stats))
            .route("/tasks/{id}/heartbeat", post(heartbeat))
            .route("/tasks/{id}/complete", post(complete))
            .with_state(app);
        with_served_by(router, "upstream")
    }
}

async fn reap(tasks: Arc<TaskQueue>, every: Duration) {
    let mut timer = interval(every);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        tasks.expire_overdue();
    }
}

#[derive(Clone)]
struct AppState {
    events: Arc<EventLog>,
    tasks: Arc<TaskQueue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpstreamHealth {
    status: &'static str,
    service: &'static str,
    last_event: i64,
    tasks: TaskStats,
}

/// Query parameters are read as text and parsed here, so a bad number is a JSON 400 like every other error.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct EventsQuery {
    after: Option<String>,
    limit: Option<String>,
    wait_ms: Option<String>,
}

async fn health(State(app): State<AppState>) -> Json<UpstreamHealth> {
    Json(UpstreamHealth {
        status: "ok",
        service: "upstream",
        last_event: app.events.last(),
        tasks: app.tasks.stats(),
    })
}

async fn append_event(
    State(app): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<LoggedEvent>), ApiError> {
    let request: NewEvent = read_json(&headers, &body)?;
    Ok((StatusCode::CREATED, Json(app.events.append(&request)?)))
}

async fn read_events(
    State(app): State<AppState>,
    query: Result<Query<EventsQuery>, QueryRejection>,
) -> Result<Json<EventPage>, ApiError> {
    let Query(query) =
        query.map_err(|_| ApiError::InvalidInput("query string is not valid".into()))?;
    let after = query_number("after", query.after.as_deref(), 0_i64)?;
    let limit = query_number("limit", query.limit.as_deref(), DEFAULT_PAGE)?;
    let wait_ms = query_number("waitMs", query.wait_ms.as_deref(), 0_i32)?;
    Ok(Json(app.events.read(after, limit, wait(wait_ms)).await?))
}

async fn add_task(
    State(app): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<NewTaskReply>), ApiError> {
    let request: NewTask = read_json(&headers, &body)?;
    let id = app.tasks.add(request.input)?;
    Ok((StatusCode::CREATED, Json(NewTaskReply { id })))
}

async fn lease_task(
    State(app): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request: TaskLeaseRequest = read_json(&headers, &body)?;
    let lease = app
        .tasks
        .lease(request.worker.as_deref(), wait(request.wait_ms))
        .await?;
    Ok(match lease {
        Some(lease) => Json(lease).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

async fn heartbeat(
    State(app): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let id = task_id(&id)?;
    let request: TaskAttempt = read_json(&headers, &body)?;
    if app.tasks.heartbeat(id, &request)? {
        Ok(StatusCode::OK)
    } else {
        Err(ApiError::Conflict("lease lost".into()))
    }
}

async fn complete(
    State(app): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let id = task_id(&id)?;
    let request: TaskAttempt = read_json(&headers, &body)?;
    if app.tasks.complete(id, &request)? {
        Ok(StatusCode::OK)
    } else {
        Err(ApiError::Conflict(
            "lease lost; another attempt owns this task".into(),
        ))
    }
}

async fn task_stats(State(app): State<AppState>) -> Json<TaskStats> {
    Json(app.tasks.stats())
}

/// A path that is not a task number names no task, like a route that does not match.
fn task_id(raw: &str) -> Result<i64, ApiError> {
    raw.parse()
        .map_err(|_| ApiError::NotFound(format!("task {raw} not found")))
}

fn query_number<T: std::str::FromStr>(
    name: &str,
    raw: Option<&str>,
    fallback: T,
) -> Result<T, ApiError> {
    match raw.filter(|raw| !raw.is_empty()) {
        None => Ok(fallback),
        Some(raw) => raw
            .parse()
            .map_err(|_| ApiError::InvalidInput(format!("{name} must be an integer"))),
    }
}

/// A requested wait, clamped to [0, MAX_WAIT].
pub fn wait(wait_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(wait_ms).unwrap_or(0)).min(MAX_WAIT)
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    struct Fixture {
        upstream: Upstream,
        app: Router,
    }

    impl Fixture {
        fn new() -> Self {
            let upstream = Upstream::new(UpstreamConfig::default());
            let app = upstream.router();
            Self { upstream, app }
        }

        async fn call(&self, method: &str, path: &str, body: Option<&str>) -> (StatusCode, Value) {
            let mut request = Request::builder().method(method).uri(path);
            if body.is_some() {
                request = request.header("content-type", "application/json");
            }
            let request = request
                .body(Body::from(body.unwrap_or("").to_owned()))
                .unwrap();
            let response = self.app.clone().oneshot(request).await.unwrap();
            let status = response.status();
            assert_eq!(response.headers()["x-served-by"], "upstream");
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            )
        }
    }

    #[test]
    fn waits_are_clamped() {
        assert_eq!(wait(-5), Duration::ZERO);
        assert_eq!(wait(500), Duration::from_millis(500));
        assert_eq!(wait(i32::MAX), MAX_WAIT);
    }

    #[tokio::test]
    async fn events_can_be_appended_and_read() {
        let f = Fixture::new();

        let created = f
            .call(
                "POST",
                "/events",
                Some(r#"{"account":"acct-1","amount":5}"#),
            )
            .await;
        let page = f.call("GET", "/events?after=0", None).await;
        let empty = f
            .call("GET", "/events?after=1&limit=10&waitMs=0", None)
            .await;

        assert_eq!(
            created,
            (
                StatusCode::CREATED,
                json!({"seq": 1, "account": "acct-1", "amount": 5})
            )
        );
        assert_eq!(
            page,
            (
                StatusCode::OK,
                json!({"events": [{"seq": 1, "account": "acct-1", "amount": 5}], "last": 1})
            )
        );
        assert_eq!(empty.1, json!({"events": [], "last": 1}));
        let (_, health) = f.call("GET", "/health", None).await;
        assert_eq!(
            (&health["service"], &health["lastEvent"]),
            (&json!("upstream"), &json!(1))
        );
    }

    #[tokio::test]
    async fn tasks_go_through_lease_heartbeat_and_complete() {
        let f = Fixture::new();
        let attempt = |worker: &str| format!(r#"{{"worker":"{worker}","attempt":1}}"#);

        let created = f.call("POST", "/tasks", Some(r#"{"input":3}"#)).await;
        let leased = f
            .call("POST", "/tasks/lease", Some(r#"{"worker":"a"}"#))
            .await;
        let empty = f
            .call("POST", "/tasks/lease", Some(r#"{"worker":"b","waitMs":0}"#))
            .await;
        let beat = f
            .call("POST", "/tasks/1/heartbeat", Some(&attempt("a")))
            .await;
        let done = f
            .call("POST", "/tasks/1/complete", Some(&attempt("a")))
            .await;
        let again = f
            .call("POST", "/tasks/1/complete", Some(&attempt("a")))
            .await;
        let other = f
            .call("POST", "/tasks/1/complete", Some(&attempt("b")))
            .await;
        let late_beat = f
            .call("POST", "/tasks/1/heartbeat", Some(&attempt("a")))
            .await;
        let (_, stats) = f.call("GET", "/tasks/stats", None).await;

        assert_eq!(created, (StatusCode::CREATED, json!({"id": 1})));
        assert_eq!(
            leased,
            (
                StatusCode::OK,
                json!({"id": 1, "input": 3, "attempt": 1, "ttlMs": 2000})
            )
        );
        assert_eq!(empty, (StatusCode::NO_CONTENT, Value::Null));
        assert_eq!(beat, (StatusCode::OK, Value::Null));
        assert_eq!(done, (StatusCode::OK, Value::Null));
        assert_eq!(again.0, StatusCode::OK); // the same report twice is fine
        assert_eq!(
            other,
            (
                StatusCode::CONFLICT,
                json!({"error": "lease lost; another attempt owns this task"})
            )
        );
        assert_eq!(
            late_beat,
            (StatusCode::CONFLICT, json!({"error": "lease lost"}))
        );
        assert_eq!(
            stats,
            json!({"total": 1, "queued": 0, "leased": 0, "done": 1, "reassignments": 0,
                   "staleCompletions": 1, "leases": []})
        );
        assert_eq!(f.upstream.tasks().stats().done, 1);
    }

    #[tokio::test]
    async fn bad_requests_are_400_and_unknown_tasks_404() {
        let f = Fixture::new();

        for (method, path, body) in [
            ("POST", "/events", Some(r#"{"account":"","amount":1}"#)),
            ("POST", "/events", Some(r#"{"account":"a","amount":"1"}"#)),
            ("POST", "/events", None),
            ("GET", "/events?after=-1", None),
            ("GET", "/events?after=x", None),
            ("GET", "/events?limit=0", None),
            ("GET", "/events?waitMs=99999999999", None),
            ("POST", "/tasks", Some(r#"{"input":3000000001}"#)),
            ("POST", "/tasks/lease", Some(r#"{"worker":"A B"}"#)),
            ("POST", "/tasks/lease", Some("{}")),
            (
                "POST",
                "/tasks/1/heartbeat",
                Some(r#"{"worker":"a","attempt":"1"}"#),
            ),
        ] {
            let (status, body) = f.call(method, path, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}");
            assert!(body["error"].is_string(), "{method} {path}: {body}");
        }
        for path in ["/tasks/99/complete", "/tasks/x/heartbeat"] {
            let (status, body) = f
                .call("POST", path, Some(r#"{"worker":"a","attempt":1}"#))
                .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert!(body["error"].as_str().unwrap().contains("not found"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_reaper_requeues_expired_leases() {
        let upstream = Upstream::new(UpstreamConfig::default());
        upstream.tasks().add(1).unwrap();
        upstream
            .tasks()
            .lease(Some("a"), Duration::ZERO)
            .await
            .unwrap();
        let reaper = upstream.spawn_reaper();

        tokio::time::sleep(UpstreamConfig::default().task_lease_ttl + Duration::from_millis(150))
            .await;

        let stats = upstream.tasks().stats();
        reaper.abort();
        assert_eq!((stats.queued, stats.reassignments), (1, 1));
    }
}
