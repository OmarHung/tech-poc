use std::time::Duration;

use axum::body::Bytes;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::de::DeserializeOwned;
use shared::work::{
    EventPage, ResultRecord, ResultReply, TaskAttempt, TaskLease, TaskLeaseRequest,
};
use tracing::debug;
use url::Url;

use crate::config::WorkerConfig;

/// How long a pooled connection may sit idle. After a network blip a fresh connection beats a half-dead one.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

/// What came of asking the upstream for a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseReply {
    Leased(TaskLease),
    /// The upstream answered, but no task came up within the poll wait: ask again right away.
    NoTask,
    /// Refused, reset or timed out: wait before asking again.
    Unreachable,
}

/// Calls to the upstream and the sink. Every call is bounded from connect to the last byte of the
/// reply; unreachable means `None`, never an error.
pub struct WorkClient {
    http: Client,
    worker_id: String,
    upstream: Url,
    events_url: Url,
    lease_url: Url,
    results_url: Url,
    sink_health_url: Url,
    rpc_timeout: Duration,
    poll_wait: Duration,
}

impl WorkClient {
    pub fn new(config: &WorkerConfig) -> Result<Self, String> {
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build()
            .map_err(|error| error.to_string())?;
        let join = |base: &Url, name: &str, path: &str| {
            base.join(path).map_err(|error| format!("{name}: {error}"))
        };
        Ok(Self {
            http,
            worker_id: config.worker_id.clone(),
            upstream: config.upstream_url.clone(),
            events_url: join(&config.upstream_url, "UPSTREAM_URL", "events")?,
            lease_url: join(&config.upstream_url, "UPSTREAM_URL", "tasks/lease")?,
            results_url: join(&config.sink_url, "SINK_URL", "results")?,
            sink_health_url: join(&config.sink_url, "SINK_URL", "health")?,
            rpc_timeout: config.rpc_timeout,
            poll_wait: config.poll_wait,
        })
    }

    /// Up to `limit` events after `after`, waiting up to the poll wait for one to arrive.
    pub async fn read_events(&self, after: i64, limit: usize) -> Option<EventPage> {
        let url = self.events_page(after, limit, self.poll_wait);
        let (status, body) = self
            .send(self.http.get(url), self.poll_wait + self.rpc_timeout)
            .await?;
        status.is_success().then(|| parse(&body)).flatten()
    }

    /// The newest event's sequence number.
    pub async fn last_event(&self) -> Option<i64> {
        let url = self.events_page(0, 1, Duration::ZERO);
        let (status, body) = self.send(self.http.get(url), self.rpc_timeout).await?;
        status
            .is_success()
            .then(|| parse::<EventPage>(&body))
            .flatten()
            .map(|page| page.last)
    }

    pub async fn lease_task(&self) -> LeaseReply {
        let body = TaskLeaseRequest {
            worker: Some(self.worker_id.clone()),
            wait_ms: i32::try_from(self.poll_wait.as_millis()).unwrap_or(i32::MAX),
        };
        let request = self.http.post(self.lease_url.clone()).json(&body);
        match self.send(request, self.poll_wait + self.rpc_timeout).await {
            Some((StatusCode::OK, body)) => {
                parse(&body).map_or(LeaseReply::NoTask, LeaseReply::Leased)
            }
            Some((StatusCode::NO_CONTENT, _)) => LeaseReply::NoTask,
            _ => LeaseReply::Unreachable,
        }
    }

    /// `Some(true)` if the lease was extended, `Some(false)` if it is lost, `None` if the upstream could
    /// not be reached within `timeout`.
    pub async fn heartbeat(&self, lease: &TaskLease, timeout: Duration) -> Option<bool> {
        self.task_call(lease, "heartbeat", timeout).await
    }

    /// `Some(true)` if the task is done, `Some(false)` if another attempt owns it now (or the task is
    /// gone), `None` if the upstream could not be reached.
    pub async fn complete(&self, lease: &TaskLease) -> Option<bool> {
        self.task_call(lease, "complete", self.rpc_timeout).await
    }

    /// Whether the sink answers its health check (2xx within RPC_TIMEOUT_MS).
    pub async fn sink_reachable(&self) -> bool {
        let request = self.http.get(self.sink_health_url.clone());
        matches!(self.send(request, self.rpc_timeout).await, Some((status, _)) if status.is_success())
    }

    /// The sink's verdict ("applied", "duplicate", "conflict", "stale"), or `None` if it could not be
    /// reached. `timeout` defaults to RPC_TIMEOUT_MS; a dispatched worker passes what is left of its lease.
    pub async fn deliver(
        &self,
        record: &ResultRecord,
        timeout: Option<Duration>,
    ) -> Option<String> {
        let request = self.http.post(self.results_url.clone()).json(record);
        let (status, body) = self
            .send(request, timeout.unwrap_or(self.rpc_timeout))
            .await?;
        if status.is_server_error() {
            return None;
        }
        parse::<ResultReply>(&body).map(|reply| reply.status)
    }

    fn events_page(&self, after: i64, limit: usize, wait: Duration) -> Url {
        let mut url = self.events_url.clone();
        url.query_pairs_mut()
            .append_pair("after", &after.to_string())
            .append_pair("limit", &limit.to_string())
            .append_pair("waitMs", &wait.as_millis().to_string());
        url
    }

    async fn task_call(&self, lease: &TaskLease, action: &str, timeout: Duration) -> Option<bool> {
        let url = self
            .upstream
            .join(&format!("tasks/{}/{action}", lease.id))
            .ok()?;
        let body = TaskAttempt {
            worker: Some(self.worker_id.clone()),
            attempt: lease.attempt,
        };
        let (status, _) = self.send(self.http.post(url).json(&body), timeout).await?;
        match status {
            StatusCode::OK => Some(true),
            // 404: the upstream no longer knows the task (it restarted); retrying will not bring it back.
            StatusCode::CONFLICT | StatusCode::NOT_FOUND => Some(false),
            _ => None,
        }
    }

    async fn send(
        &self,
        request: RequestBuilder,
        timeout: Duration,
    ) -> Option<(StatusCode, Bytes)> {
        let exchange = async {
            let response = request.send().await?;
            let status = response.status();
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, body))
        };
        match tokio::time::timeout(timeout, exchange).await {
            Ok(Ok(reply)) => Some(reply),
            Ok(Err(error)) => {
                debug!("Call failed: {error}");
                None
            }
            Err(_) => {
                debug!("Call timed out after {} ms", timeout.as_millis());
                None
            }
        }
    }
}

/// The body as `T`, or `None` if it is not that JSON: a garbled reply counts as no reply.
fn parse<T: DeserializeOwned>(body: &[u8]) -> Option<T> {
    serde_json::from_slice(body).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::http::StatusCode as Code;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use serde_json::{Value, json};

    use super::*;

    type Seen = Arc<Mutex<Vec<String>>>;

    /// An upstream and a sink in one server whose replies the test picks.
    async fn fake(
        task_status: Code,
        sink_status: Code,
        sink_body: &'static str,
    ) -> (WorkClient, Seen) {
        let seen: Seen = Arc::default();
        let (events, tasks, results) = (seen.clone(), seen.clone(), seen.clone());
        let app = Router::new()
            .route(
                "/events",
                get(move |uri: axum::http::Uri| async move {
                    events.lock().unwrap().push(uri.to_string());
                    axum::Json(
                        json!({"events": [{"seq": 4, "account": "x", "amount": 1}], "last": 9}),
                    )
                }),
            )
            .route(
                "/tasks/lease",
                post(|| async {
                    axum::Json(json!({"id": 3, "input": 5, "attempt": 2, "ttlMs": 2000}))
                }),
            )
            .route(
                "/tasks/{id}/{action}",
                post(move |uri: axum::http::Uri, body: String| async move {
                    tasks.lock().unwrap().push(format!("{uri} {body}"));
                    task_status
                }),
            )
            .route("/health", get(move || async move { sink_status }))
            .route(
                "/results",
                post(move |body: String| async move {
                    results.lock().unwrap().push(body);
                    (
                        sink_status,
                        [("content-type", "application/json")],
                        sink_body,
                    )
                        .into_response()
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        let url = Url::parse(&format!("http://{address}")).unwrap();
        let config = WorkerConfig {
            worker_id: "b".into(),
            upstream_url: url.clone(),
            sink_url: url,
            ..WorkerConfig::default()
        };
        (WorkClient::new(&config).unwrap(), seen)
    }

    fn lease() -> TaskLease {
        TaskLease {
            id: 3,
            input: 5,
            attempt: 2,
            ttl_ms: 2000,
        }
    }

    fn record() -> ResultRecord {
        ResultRecord {
            key: Some("task-3".into()),
            node: Some("b".into()),
            attempt: 2,
            value: Some("5^2=25".into()),
        }
    }

    #[tokio::test]
    async fn reads_events_and_leases_tasks() {
        let (client, seen) = fake(Code::OK, Code::CREATED, r#"{"status":"applied"}"#).await;

        let page = client.read_events(3, 100).await.unwrap();
        let last = client.last_event().await;
        let leased = client.lease_task().await;

        assert_eq!((page.events[0].seq, page.last, last), (4, 9, Some(9)));
        assert_eq!(leased, LeaseReply::Leased(lease()));
        assert_eq!(
            *seen.lock().unwrap(),
            [
                "/events?after=3&limit=100&waitMs=500",
                "/events?after=0&limit=1&waitMs=0"
            ]
        );
    }

    #[tokio::test]
    async fn task_calls_say_whether_the_lease_still_holds() {
        for (status, expected) in [
            (Code::OK, Some(true)),
            (Code::CONFLICT, Some(false)),
            (Code::NOT_FOUND, Some(false)),
            (Code::INTERNAL_SERVER_ERROR, None),
        ] {
            let (client, seen) = fake(status, Code::CREATED, "{}").await;

            assert_eq!(
                client.heartbeat(&lease(), Duration::from_secs(1)).await,
                expected,
                "{status}"
            );
            assert_eq!(client.complete(&lease()).await, expected, "{status}");
            let calls = seen.lock().unwrap().clone();
            assert_eq!(calls.len(), 2);
            assert!(calls[0].starts_with("/tasks/3/heartbeat "), "{calls:?}");
            assert!(calls[1].starts_with("/tasks/3/complete "), "{calls:?}");
            let body: Value = serde_json::from_str(calls[1].split_once(' ').unwrap().1).unwrap();
            assert_eq!(body, json!({"worker": "b", "attempt": 2}));
        }
    }

    #[tokio::test]
    async fn deliveries_return_the_sinks_verdict() {
        let cases = [
            (Code::CREATED, r#"{"status":"applied"}"#, Some("applied")),
            (Code::CONFLICT, r#"{"status":"stale"}"#, Some("stale")),
            (Code::BAD_REQUEST, r#"{"error":"bad"}"#, None),
            (Code::SERVICE_UNAVAILABLE, r#"{"status":"applied"}"#, None),
        ];
        for (status, body, expected) in cases {
            let (client, seen) = fake(Code::OK, status, body).await;

            let reply = client.deliver(&record(), None).await;

            assert_eq!(reply.as_deref(), expected, "{status}");
            assert_eq!(
                client.sink_reachable().await,
                status.is_success(),
                "{status}"
            );
            let sent: Value = serde_json::from_str(&seen.lock().unwrap()[0]).unwrap();
            assert_eq!(
                sent,
                json!({"key": "task-3", "node": "b", "attempt": 2, "value": "5^2=25"})
            );
        }
    }

    #[tokio::test]
    async fn unreachable_or_slow_targets_are_no_answer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        drop(listener);
        let slow = Router::new().fallback(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            "late"
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let slow_url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move { axum::serve(listener, slow).await });

        for url in [dead, slow_url] {
            let config = WorkerConfig {
                upstream_url: url.clone(),
                sink_url: url,
                rpc_timeout: Duration::from_millis(100),
                poll_wait: Duration::from_millis(50),
                ..WorkerConfig::default()
            };
            let client = WorkClient::new(&config).unwrap();

            assert_eq!(client.read_events(0, 10).await, None);
            assert_eq!(client.last_event().await, None);
            assert_eq!(client.lease_task().await, LeaseReply::Unreachable);
            assert_eq!(
                client.heartbeat(&lease(), Duration::from_millis(50)).await,
                None
            );
            assert_eq!(client.complete(&lease()).await, None);
            assert_eq!(
                client
                    .deliver(&record(), Some(Duration::from_millis(50)))
                    .await,
                None
            );
            assert!(!client.sink_reachable().await);
        }
    }
}
