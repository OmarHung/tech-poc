//! Shared test plumbing: one throwaway Redis for the whole run, a fresh sale per test (unique
//! SALE_ID, so tests never see each other's keys), a manual clock, and the real router driven
//! in-process.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::Response;
use checkout_queue::admin::AuditReport;
use checkout_queue::app::{self, BackgroundTasks, Sale};
use checkout_queue::clock::{Clock, ManualClock};
use checkout_queue::config::Config;
use checkout_queue::ids;
use checkout_queue::orders::{CheckoutResult, OrderView};
use checkout_queue::queue::TicketView;
use checkout_queue::sale_redis::SaleRedis;
use http_body_util::BodyExt;
use serde_json::Value;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tower::ServiceExt;

/// 2026-10-30T00:00:00Z, where every test's clock starts.
pub const START_MS: i64 = 1_793_318_400_000;
/// How long a test waits for an event before calling it missing.
pub const EVENT_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------- the run's Redis

struct SharedRedis {
    runtime: tokio::runtime::Runtime,
    container: ContainerAsync<Redis>,
    address: String,
}

static SHARED: Mutex<Option<SharedRedis>> = Mutex::new(None);

/// `host:port` of the run's Redis, started on first use. The harness calls this before each
/// test's runtime starts, so inside a test it only reads the cached address.
pub fn redis_address() -> String {
    let mut shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(redis) = shared.as_ref() {
        return redis.address.clone();
    }
    let runtime = tokio::runtime::Runtime::new().expect("a Tokio runtime for the container");
    let (container, address) = runtime.block_on(async {
        let container = Redis::default()
            .with_tag("7.4-alpine")
            .start()
            .await
            .expect("start Redis in Docker (is Docker running?)");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("Redis port");
        (container, format!("127.0.0.1:{port}"))
    });
    *shared = Some(SharedRedis {
        runtime,
        container,
        address: address.clone(),
    });
    address
}

/// Removes the run's Redis container, if a test started it.
pub fn remove_redis() {
    let shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(SharedRedis {
        runtime, container, ..
    }) = shared
        && let Err(error) = runtime.block_on(container.rm())
    {
        eprintln!("could not remove the Redis container: {error}");
    }
}

/// A plain connection for poking at keys directly.
pub async fn raw_redis() -> redis::aio::MultiplexedConnection {
    redis::Client::open(format!("redis://{}", redis_address()))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

// ---------------------------------------------------------------- one sale

/// Small numbers so every rule is easy to hit: 10 in stock, 3 at a time, up to 2 each.
pub fn new_config() -> Config {
    Config {
        sale_id: format!("t{}", ids::new_id()),
        initial_stock: 10,
        max_per_order: 2,
        max_active: 3,
        admission_window_ms: 60_000,
        payment_window_ms: 120_000,
        heartbeat_timeout_ms: 15_000,
        poll_interval_ms: 1_000,
        ..Config::default()
    }
}

/// A fresh sale on the shared Redis, wired the same way as the app and driven by a manual clock.
pub struct TestSale {
    pub sale: Arc<Sale>,
    pub clock: Arc<ManualClock>,
}

impl TestSale {
    pub async fn start() -> Self {
        Self::start_with(|c| c).await
    }

    pub async fn start_with(tweak: impl FnOnce(Config) -> Config) -> Self {
        Self::start_config(tweak(new_config())).await
    }

    pub async fn start_config(config: Config) -> Self {
        Self::start_at(&redis_address(), config).await
    }

    /// Against Redis at `address` (a [`Proxy`] in the outage tests).
    pub async fn start_at(address: &str, config: Config) -> Self {
        let clock = Arc::new(ManualClock::new(START_MS));
        let shared: Arc<dyn Clock> = clock.clone();
        let redis = SaleRedis::connect(&format!("redis://{address}"), &config.sale_id, shared)
            .await
            .expect("connect to the test Redis");
        let initial_stock = config.initial_stock;
        let sale = Sale::new(config, redis).expect("a valid test config");
        sale.redis
            .initialize(initial_stock)
            .await
            .expect("init.lua");
        Self { sale, clock }
    }

    pub fn config(&self) -> &Config {
        &self.sale.config
    }

    pub fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    pub fn advance(&self, by: Duration) {
        self.clock.advance(by);
    }

    /// Joins and returns the ticket (new or existing).
    pub async fn join(&self, user_id: &str) -> TicketView {
        self.sale
            .queue
            .join(user_id)
            .await
            .unwrap()
            .ticket()
            .cloned()
            .unwrap_or_else(|| panic!("{user_id} could not join: sold out"))
    }

    pub async fn ticket(&self, ticket_id: &str) -> TicketView {
        self.sale
            .queue
            .ticket(ticket_id)
            .await
            .unwrap()
            .expect("ticket exists")
    }

    /// Joins every user, runs one admission pass, and returns their tickets after it.
    pub async fn join_and_admit(&self, user_ids: &[&str]) -> Vec<TicketView> {
        for user_id in user_ids {
            self.join(user_id).await;
        }
        self.sale.queue.admit().await.unwrap();
        let mut tickets = Vec::with_capacity(user_ids.len());
        for user_id in user_ids {
            tickets.push(self.join(user_id).await);
        }
        tickets
    }

    /// Puts one user through the line and returns their pending order.
    pub async fn place_order(&self, user_id: &str, quantity: u32) -> OrderView {
        let ticket = self.join_and_admit(&[user_id]).await.remove(0);
        match self
            .sale
            .orders
            .checkout(&ticket.ticket_id, quantity)
            .await
            .unwrap()
        {
            CheckoutResult::Created(order) => order,
            other => panic!("expected a new order, got {other:?}"),
        }
    }

    pub async fn audit(&self) -> AuditReport {
        self.sale.audit.get().await.unwrap()
    }
}

// ---------------------------------------------------------------- the HTTP app

/// The real router, called in-process; the admission tick is driven by the test.
pub struct TestApp {
    pub sale: TestSale,
    pub background: BackgroundTasks,
    router: Router,
}

impl TestApp {
    pub async fn start() -> Self {
        Self::start_with(|c| c).await
    }

    pub async fn start_with(tweak: impl FnOnce(Config) -> Config) -> Self {
        Self::start_at(&redis_address(), tweak).await
    }

    pub async fn start_at(address: &str, tweak: impl FnOnce(Config) -> Config) -> Self {
        let config = Config {
            instance_name: "test-instance".to_owned(),
            ..tweak(new_config())
        };
        Self::from_sale(TestSale::start_at(address, config).await).await
    }

    pub async fn from_sale(sale: TestSale) -> Self {
        let background = app::start_background(&sale.sale, false).await;
        let router = app::router(Arc::clone(&sale.sale));
        Self {
            sale,
            background,
            router,
        }
    }

    pub async fn open(&self, method: &str, path: &str, body: Option<(&str, &str)>) -> Response {
        let builder = Request::builder().method(method).uri(path);
        let request = match body {
            Some((content_type, body)) => builder
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(body.to_owned())),
            None => builder.body(Body::empty()),
        };
        self.router.clone().oneshot(request.unwrap()).await.unwrap()
    }

    pub async fn send(&self, method: &str, path: &str, body: Option<(&str, &str)>) -> TestResponse {
        let response = self.open(method, path, body).await;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        TestResponse {
            status,
            headers,
            body,
        }
    }

    pub async fn get(&self, path: &str) -> TestResponse {
        self.send("GET", path, None).await
    }

    pub async fn post(&self, path: &str) -> TestResponse {
        self.send("POST", path, None).await
    }

    pub async fn post_json(&self, path: &str, body: &Value) -> TestResponse {
        self.send("POST", path, Some(("application/json", &body.to_string())))
            .await
    }

    /// Joins over HTTP and returns the ticket id.
    pub async fn join(&self, user_id: &str) -> String {
        let response = self
            .post_json("/queue/join", &serde_json::json!({ "userId": user_id }))
            .await;
        assert!(response.status.is_success(), "join: {}", response.text());
        response.json()["ticketId"].as_str().unwrap().to_owned()
    }

    pub async fn events(&self, ticket_id: &str) -> Events {
        let response = self
            .open("GET", &format!("/queue/tickets/{ticket_id}/events"), None)
            .await;
        Events::new(response)
    }
}

pub struct TestResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl TestResponse {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", self.text()))
    }

    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .unwrap_or_else(|| panic!("no {name} header"))
            .to_str()
            .unwrap()
    }

    /// Asserts the status and the `{"error": ..., "code": ...}` shape; returns the code.
    pub fn error_code(&self, expected: StatusCode) -> String {
        assert_eq!(self.status, expected, "body: {}", self.text());
        assert_eq!(self.header("content-type"), "application/json");
        let body = self.json();
        let error = body["error"].as_str().unwrap_or_default();
        assert!(!error.trim().is_empty(), "no error message: {body}");
        body["code"].as_str().expect("an error code").to_owned()
    }
}

/// Reads a server-sent event stream line by line.
pub struct Events {
    pub status: StatusCode,
    pub headers: HeaderMap,
    body: Body,
    buffer: String,
}

impl Events {
    pub fn new(response: Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        Self {
            status,
            headers,
            body: response.into_body(),
            buffer: String::new(),
        }
    }

    /// The next event's data as JSON. Skips keep-alive comments.
    pub async fn next(&mut self) -> Value {
        let mut data = None;
        loop {
            let line = self.line().await.expect("the stream ended");
            if let Some(json) = line.strip_prefix("data: ") {
                data = Some(json.to_owned());
            } else if line.is_empty()
                && let Some(json) = data.take()
            {
                return serde_json::from_str(&json).unwrap();
            }
        }
    }

    /// True if the server ends the stream without sending anything else.
    pub async fn ended(&mut self) -> bool {
        self.line().await.is_none()
    }

    async fn line(&mut self) -> Option<String> {
        loop {
            if let Some(end) = self.buffer.find('\n') {
                let line = self.buffer[..end].to_owned();
                self.buffer.drain(..=end);
                return Some(line);
            }
            let frame = tokio::time::timeout(EVENT_TIMEOUT, self.body.frame())
                .await
                .expect("no event within the timeout")?;
            if let Ok(data) = frame.expect("stream error").into_data() {
                self.buffer.push_str(std::str::from_utf8(&data).unwrap());
            }
        }
    }
}

// ---------------------------------------------------------------- outages

/// A TCP proxy in front of the run's Redis that a test can cut and restore, to see what
/// requests do while Redis is unreachable.
pub struct Proxy {
    address: SocketAddr,
    accepting: Option<JoinHandle<()>>,
}

impl Proxy {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        Self {
            address,
            accepting: Some(tokio::spawn(forward(listener))),
        }
    }

    pub fn address(&self) -> String {
        self.address.to_string()
    }

    /// Drops every proxied connection and stops listening: Redis is "down".
    pub fn cut(&mut self) {
        if let Some(accepting) = self.accepting.take() {
            accepting.abort();
        }
    }

    /// Listens again on the same port: Redis is "back".
    pub async fn restore(&mut self) {
        let listener = TcpListener::bind(self.address).await.unwrap();
        self.accepting = Some(tokio::spawn(forward(listener)));
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.cut();
    }
}

/// Aborting this task drops the JoinSet, which aborts and closes every forwarded connection.
async fn forward(listener: TcpListener) {
    let upstream = redis_address();
    let mut connections = JoinSet::new();
    while let Ok((mut client, _)) = listener.accept().await {
        let upstream = upstream.clone();
        connections.spawn(async move {
            if let Ok(mut server) = TcpStream::connect(upstream).await {
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            }
        });
    }
}

/// Retries `attempt` until it succeeds or `EVENT_TIMEOUT` passes (for after a reconnect).
pub async fn eventually<T, E: std::fmt::Debug, F: Future<Output = Result<T, E>>>(
    mut attempt: impl FnMut() -> F,
) -> T {
    let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
    loop {
        match attempt().await {
            Ok(value) => return value,
            Err(error) if tokio::time::Instant::now() > deadline => {
                panic!("still failing after {EVENT_TIMEOUT:?}: {error:?}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

pub fn users(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("user-{i}")).collect()
}
