//! Gateway: the clients' single entry point. Forwards everything to whichever node is primary right now.

pub mod config;
pub mod tracker;

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::header::{CONTENT_LENGTH, HOST};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use shared::error::ErrorBody;
use shared::http::{MAX_BODY_BYTES, SERVED_BY};
use tracing::debug;

use crate::config::{GatewayConfig, UPSTREAM_TIMEOUT};
use crate::tracker::PrimaryTracker;

pub struct Gateway {
    tracker: Arc<PrimaryTracker>,
    http: reqwest::Client,
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(UPSTREAM_TIMEOUT)
            .build()?;
        let tracker = Arc::new(PrimaryTracker::new(config, http.clone()));
        Ok(Self { tracker, http })
    }

    pub fn tracker(&self) -> &Arc<PrimaryTracker> {
        &self.tracker
    }
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    service: &'static str,
    primary: Option<String>,
    epoch: Option<i64>,
}

pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        // Other methods on /health are forwarded like any other path.
        .route("/health", get(health).fallback(forward))
        .fallback(forward)
        .with_state(gateway)
}

/// Gateway-owned responses are tagged so callers can tell them from forwarded ones.
fn own_response(status: StatusCode, body: impl Serialize) -> Response {
    (status, [(SERVED_BY, "gateway")], Json(body)).into_response()
}

fn error_response(status: StatusCode, message: &str) -> Response {
    own_response(
        status,
        ErrorBody {
            error: message.to_owned(),
        },
    )
}

async fn health(State(gateway): State<Arc<Gateway>>) -> Response {
    let current = gateway.tracker.current();
    own_response(
        StatusCode::OK,
        Health {
            status: "ok",
            service: "gateway",
            primary: current.as_ref().map(|c| c.target.id.clone()),
            epoch: current.map(|c| c.epoch),
        },
    )
}

async fn forward(State(gateway): State<Arc<Gateway>>, request: Request) -> Response {
    let Some(primary) = gateway.tracker.current() else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "no primary available");
    };
    let (parts, body) = request.into_parts();
    let Ok(body) = to_bytes(body, MAX_BODY_BYTES).await else {
        return error_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    };
    let path_and_query = parts.uri.path_and_query().map_or("/", |pq| pq.as_str());
    let url = format!(
        "{}{path_and_query}",
        primary.target.url.as_str().trim_end_matches('/')
    );
    let sent = gateway
        .http
        .request(parts.method, url)
        .headers(forwardable(&parts.headers))
        .body(body)
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(error) => return unavailable(&gateway, &primary.target.id, &error),
    };
    let status = response.status();
    let headers = forwardable(response.headers());
    match response.bytes().await {
        Ok(body) => {
            if status == StatusCode::SERVICE_UNAVAILABLE {
                gateway.tracker.nudge(); // the node says it is not primary (any more)
            }
            (status, headers, Body::from(body)).into_response()
        }
        Err(error) => unavailable(&gateway, &primary.target.id, &error),
    }
}

fn unavailable(gateway: &Gateway, node: &str, error: &reqwest::Error) -> Response {
    gateway.tracker.nudge();
    debug!("Forwarding to {node} failed: {error}");
    error_response(StatusCode::BAD_GATEWAY, "primary unavailable")
}

/// Drops hop-by-hop headers plus Host and Content-Length, which the HTTP client recomputes.
fn forwardable(headers: &HeaderMap) -> HeaderMap {
    headers
        .iter()
        .filter(|(name, _)| !is_hop_by_hop(name) && *name != HOST && *name != CONTENT_LENGTH)
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    use axum::http::Method;
    use axum::routing::any;
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::config::NodeTarget;
    use crate::tracker::PrimaryChoice;

    type HealthBodies = Arc<Mutex<HashMap<String, String>>>;

    /// A node whose /health body the test sets, and which echoes everything else.
    async fn fake_node(id: &'static str, bodies: HealthBodies) -> NodeTarget {
        let app = Router::new()
            .route(
                "/health",
                get(move || async move {
                    let body = bodies.lock().unwrap().get(id).cloned().unwrap_or_else(|| r#"{"role":"standby","epoch":0}"#.into());
                    ([("content-type", "application/json")], body)
                }),
            )
            .fallback(any(move |request: Request| async move {
                let (parts, body) = request.into_parts();
                let body = String::from_utf8(to_bytes(body, usize::MAX).await.unwrap().to_vec()).unwrap();
                let echo = json!({"node": id, "method": parts.method.as_str(), "uri": parts.uri.to_string(), "body": body});
                let status = if parts.uri.path() == "/not-primary" { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK };
                (status, [(SERVED_BY, format!("node-{id}"))], Json(echo))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        NodeTarget {
            id: id.into(),
            url: Url::parse(&format!("http://{address}")).unwrap(),
        }
    }

    struct Fixture {
        app: Router,
        bodies: HealthBodies,
    }

    impl Fixture {
        async fn new() -> Self {
            let bodies: HealthBodies = Arc::default();
            let nodes = vec![
                fake_node("a", bodies.clone()).await,
                fake_node("b", bodies.clone()).await,
            ];
            let config = GatewayConfig {
                nodes,
                poll_interval: Duration::from_millis(20),
                ..GatewayConfig::default()
            };
            let gateway = Arc::new(Gateway::new(config).unwrap());
            tokio::spawn(Arc::clone(gateway.tracker()).run());
            Self {
                app: router(gateway),
                bodies,
            }
        }

        fn set(&self, node: &str, body: &str) {
            self.bodies.lock().unwrap().insert(node.into(), body.into());
        }

        async fn call(&self, method: Method, uri: &str, body: &str) -> (StatusCode, String, Value) {
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap();
            let response = self.app.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let served_by = response
                .headers()
                .get(SERVED_BY)
                .map(|v| v.to_str().unwrap().to_owned())
                .unwrap_or_default();
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (
                status,
                served_by,
                serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            )
        }

        async fn wait_for_primary(&self, expected: Option<&str>) {
            for _ in 0..500 {
                let (_, _, health) = self.call(Method::GET, "/health", "").await;
                if health["primary"].as_str() == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("gateway never settled on {expected:?}");
        }
    }

    #[tokio::test]
    async fn no_primary_is_503_from_the_gateway() {
        let f = Fixture::new().await;
        f.set("a", r#"{"role":"primary","epoch":1}"#);
        f.wait_for_primary(Some("a")).await;
        f.set("a", r#"{"role":"standby","epoch":1}"#);
        f.wait_for_primary(None).await;

        let (status, served_by, body) = f.call(Method::PUT, "/kv/k", r#"{"value":"v"}"#).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, json!({"error": "no primary available"}));
        assert_eq!(served_by, "gateway");
    }

    #[tokio::test]
    async fn requests_go_to_the_primary_with_method_path_query_and_body() {
        let f = Fixture::new().await;
        f.set("b", r#"{"role":"primary","epoch":2}"#);
        f.wait_for_primary(Some("b")).await;

        let (status, served_by, echo) = f.call(Method::PUT, "/kv/k?x=1", r#"{"value":"v"}"#).await;
        let (_, gateway_served, health) = f.call(Method::GET, "/health", "").await;
        let (posted_status, posted_served_by, _) = f.call(Method::POST, "/health", "{}").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(served_by, "node-b");
        assert_eq!(
            echo,
            json!({"node": "b", "method": "PUT", "uri": "/kv/k?x=1", "body": r#"{"value":"v"}"#})
        );
        assert_eq!(gateway_served, "gateway");
        assert_eq!(
            health,
            json!({"status": "ok", "service": "gateway", "primary": "b", "epoch": 2})
        );
        // Only GET /health belongs to the gateway; the fake node itself answers POST /health with 405.
        assert_eq!(
            (posted_status, posted_served_by.as_str()),
            (StatusCode::METHOD_NOT_ALLOWED, "")
        );
    }

    #[tokio::test]
    async fn requests_follow_a_failover_and_garbage_health_is_ignored() {
        let f = Fixture::new().await;
        f.set("a", r#"{"role":"primary","epoch":1}"#);
        f.wait_for_primary(Some("a")).await;

        f.set("a", "this is not json");
        f.set("b", r#"{"role":"primary","epoch":2}"#);
        f.wait_for_primary(Some("b")).await;
        let (_, served_by, _) = f.call(Method::GET, "/kv/k", "").await;

        assert_eq!(served_by, "node-b");
        f.set("b", r#"{"role":"primary","epoch":"2"}"#);
        f.wait_for_primary(None).await;
    }

    #[tokio::test]
    async fn a_node_saying_503_is_passed_through_and_triggers_a_poll() {
        let f = Fixture::new().await;
        f.set("a", r#"{"role":"primary","epoch":1}"#);
        f.wait_for_primary(Some("a")).await;

        let (status, served_by, _) = f.call(Method::GET, "/not-primary", "").await;

        assert_eq!(
            (status, served_by.as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, "node-a")
        );
    }

    #[tokio::test]
    async fn unreachable_primary_is_502() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        drop(listener);
        // No polling: the tracker still believes in a primary that has just died.
        let gateway = Arc::new(Gateway::new(GatewayConfig::default()).unwrap());
        gateway.tracker.set_current(Some(PrimaryChoice {
            target: NodeTarget {
                id: "a".into(),
                url: dead,
            },
            epoch: 1,
        }));

        let request = Request::builder().uri("/kv/k").body(Body::empty()).unwrap();
        let response = router(gateway).oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response.headers()[SERVED_BY], "gateway");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"error": "primary unavailable"})
        );
    }

    #[test]
    fn hop_by_hop_headers_are_not_forwarded() {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("connection", "close"),
            ("host", "x"),
            ("content-length", "3"),
            ("x-served-by", "node-a"),
            ("content-type", "application/json"),
        ] {
            headers.insert(HeaderName::from_static(name), value.parse().unwrap());
        }

        let kept: Vec<String> = forwardable(&headers)
            .keys()
            .map(|k| k.to_string())
            .collect();

        assert_eq!(kept.len(), 2);
        assert!(
            kept.contains(&"x-served-by".to_owned()) && kept.contains(&"content-type".to_owned())
        );
    }
}
