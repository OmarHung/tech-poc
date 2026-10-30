//! Strangler Fig facade: one entry point whose catalog route can be switched at runtime.

pub mod routing;

use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Request, State};
use axum::http::header::{CONTENT_LENGTH, HOST};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::routing::{CatalogRoute, Upstreams, resolve};

pub const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BODY_BYTES: usize = 1024 * 1024;
const SERVED_BY: HeaderName = HeaderName::from_static("x-served-by");
const POISONED: &str = "route lock poisoned";

struct GatewayState {
    http: reqwest::Client,
    upstreams: Upstreams,
    catalog_route: RwLock<CatalogRoute>,
}

impl GatewayState {
    fn route(&self) -> CatalogRoute {
        *self.catalog_route.read().expect(POISONED)
    }
}

#[derive(Serialize)]
struct Routes {
    catalog: CatalogRoute,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    service: &'static str,
    routes: Routes,
}

#[derive(Deserialize)]
struct RoutesUpdate {
    catalog: String,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub fn build_app(
    upstreams: Upstreams,
    initial_route: CatalogRoute,
) -> Result<Router, reqwest::Error> {
    let http = reqwest::Client::builder()
        .timeout(UPSTREAM_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let state = Arc::new(GatewayState {
        http,
        upstreams,
        catalog_route: RwLock::new(initial_route),
    });
    Ok(Router::new()
        .route("/health", get(health))
        .route("/admin/routes", get(get_routes).put(put_routes))
        .fallback(proxy)
        .with_state(state))
}

/// Gateway-owned responses are tagged so callers can tell them from proxied ones.
fn own_response<T: Serialize>(status: StatusCode, body: T) -> Response {
    (status, [(SERVED_BY, "gateway")], Json(body)).into_response()
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    own_response(
        status,
        ErrorBody {
            error: message.into(),
        },
    )
}

async fn health(State(state): State<Arc<GatewayState>>) -> Response {
    let routes = Routes {
        catalog: state.route(),
    };
    own_response(
        StatusCode::OK,
        Health {
            status: "ok",
            service: "gateway",
            routes,
        },
    )
}

async fn get_routes(State(state): State<Arc<GatewayState>>) -> Response {
    own_response(
        StatusCode::OK,
        Routes {
            catalog: state.route(),
        },
    )
}

/// No auth: the PoC binds to 127.0.0.1 only. A real gateway must protect this.
async fn put_routes(
    State(state): State<Arc<GatewayState>>,
    payload: Result<Json<RoutesUpdate>, JsonRejection>,
) -> Response {
    let update = match payload {
        Ok(Json(update)) => update,
        Err(rejection) => return error_response(StatusCode::BAD_REQUEST, rejection.body_text()),
    };
    let route = match update.catalog.parse::<CatalogRoute>() {
        Ok(route) => route,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    *state.catalog_route.write().expect(POISONED) = route;
    own_response(StatusCode::OK, Routes { catalog: route })
}

async fn proxy(State(state): State<Arc<GatewayState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let upstream = resolve(parts.uri.path(), state.route(), &state.upstreams);
    let path_and_query = parts.uri.path_and_query().map_or("/", |pq| pq.as_str());
    let Ok(body) = to_bytes(body, MAX_BODY_BYTES).await else {
        return error_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    };
    let sent = state
        .http
        .request(parts.method, format!("{upstream}{path_and_query}"))
        .headers(forwardable(&parts.headers))
        .body(body)
        .send()
        .await;
    match sent {
        Ok(response) => relay(response).await,
        Err(error) => {
            eprintln!("upstream {upstream} failed: {error}");
            error_response(StatusCode::BAD_GATEWAY, "upstream unavailable")
        }
    }
}

async fn relay(response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = forwardable(response.headers());
    match response.bytes().await {
        Ok(body) => (status, headers, Body::from(body)).into_response(),
        Err(error) => {
            eprintln!("reading upstream response failed: {error}");
            error_response(StatusCode::BAD_GATEWAY, "upstream unavailable")
        }
    }
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
    use axum::http::header::CONTENT_TYPE;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;

    /// Upstream that answers every request with its own name.
    async fn spawn_upstream(name: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(move || async move { name });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    async fn gateway() -> Router {
        let upstreams = Upstreams {
            monolith: spawn_upstream("monolith").await,
            catalog: spawn_upstream("catalog").await,
        };
        build_app(upstreams, CatalogRoute::Monolith).unwrap()
    }

    async fn send(app: &Router, method: &str, uri: &str, body: &str) -> (StatusCode, String) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn switching_the_route_moves_catalog_traffic_only() {
        let app = gateway().await;
        assert_eq!(send(&app, "GET", "/products/1", "").await.1, "monolith");

        let (status, _) = send(&app, "PUT", "/admin/routes", r#"{"catalog":"service"}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(send(&app, "GET", "/products/1", "").await.1, "catalog");
        assert_eq!(send(&app, "POST", "/orders", "{}").await.1, "monolith");
    }

    #[tokio::test]
    async fn rejects_unknown_route_names() {
        let app = gateway().await;
        let (status, body) = send(&app, "PUT", "/admin/routes", r#"{"catalog":"nope"}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("catalog must be"), "{body}");
    }

    #[tokio::test]
    async fn unreachable_upstream_returns_502() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let app = build_app(
            Upstreams {
                monolith: dead.clone(),
                catalog: dead,
            },
            CatalogRoute::Monolith,
        )
        .unwrap();
        let (status, body) = send(&app, "GET", "/orders/1", "").await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(body.contains("upstream unavailable"), "{body}");
    }
}
