//! HTTP plumbing shared by every process.

use std::io;
use std::net::Ipv4Addr;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::response::Response;
use axum::serve::ListenerExt;
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;

use crate::error::ApiError;

pub const SERVED_BY: HeaderName = HeaderName::from_static("x-served-by");

/// Largest request body accepted, the same as Kestrel's default. A full replication batch fits easily.
pub const MAX_BODY_BYTES: usize = 30_000_000;

/// Tags every response with `X-Served-By` so callers can tell who answered, and caps request bodies.
pub fn with_served_by<S>(router: Router<S>, name: &str) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let value = HeaderValue::from_str(name).unwrap_or_else(|_| HeaderValue::from_static("unknown"));
    router
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(axum::middleware::map_response(
            move |mut response: Response| {
                let value = value.clone();
                async move {
                    response.headers_mut().insert(SERVED_BY, value);
                    response
                }
            },
        ))
}

/// Parses a JSON request body strictly: a number where a string is expected (or the reverse) is a 400,
/// and so is a missing or non-JSON Content-Type. Unknown fields are ignored.
pub fn read_json<T: DeserializeOwned>(headers: &HeaderMap, body: &[u8]) -> Result<T, ApiError> {
    if !has_json_content_type(headers) {
        return Err(ApiError::InvalidInput("request body must be JSON".into()));
    }
    match serde_json::from_slice::<Option<T>>(body) {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(ApiError::InvalidInput("request body is required".into())),
        // The serde message names Rust types; the position is enough for the caller.
        Err(error) => Err(ApiError::InvalidInput(format!(
            "invalid JSON body at line {} column {}",
            error.line(),
            error.column()
        ))),
    }
}

/// `application/json` or any `application/*+json`, with or without parameters such as charset.
pub fn has_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let media_type = value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || (media_type.starts_with("application/") && media_type.ends_with("+json"))
}

/// Binds 127.0.0.1 only: nothing here has authentication (see the README's scope section).
pub async fn bind(port: u16) -> io::Result<TcpListener> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await
}

/// Serves until the process is killed. Small request/response pairs go out without Nagle delays.
pub async fn serve(listener: TcpListener, router: Router) -> io::Result<()> {
    let listener = listener.tap_io(|tcp| {
        if let Err(error) = tcp.set_nodelay(true) {
            tracing::debug!("could not set TCP_NODELAY: {error}");
        }
    });
    axum::serve(listener, router).await
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use serde::Deserialize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tower::ServiceExt;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Body1 {
        value: Option<String>,
    }

    fn json_headers(content_type: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
        headers
    }

    #[test]
    fn reads_strict_json() {
        let headers = json_headers("application/json; charset=utf-8");

        assert_eq!(
            read_json::<Body1>(&headers, br#"{"value":"v","extra":1}"#),
            Ok(Body1 {
                value: Some("v".into())
            })
        );
        assert_eq!(
            read_json::<Body1>(&headers, b"{}"),
            Ok(Body1 { value: None })
        );
        for bad in [&br#"{"value":1}"#[..], b"not json", b"", b"[1]"] {
            let error = read_json::<Body1>(&headers, bad).unwrap_err();
            assert!(error.message().starts_with("invalid JSON body"), "{error}");
        }
        assert_eq!(
            read_json::<Body1>(&headers, b"null"),
            Err(ApiError::InvalidInput("request body is required".into()))
        );
    }

    #[test]
    fn requires_a_json_content_type() {
        assert!(has_json_content_type(&json_headers(
            "application/problem+json"
        )));
        assert!(!has_json_content_type(&json_headers("text/plain")));
        assert!(!has_json_content_type(&HeaderMap::new()));
        assert_eq!(
            read_json::<Body1>(&json_headers("text/plain"), b"{}"),
            Err(ApiError::InvalidInput("request body must be JSON".into()))
        );
    }

    #[tokio::test]
    async fn every_response_says_who_served_it() {
        let app = with_served_by(Router::new().route("/ok", get(|| async { "ok" })), "node-a");

        for path in ["/ok", "/missing"] {
            let request = Request::builder().uri(path).body(Body::empty()).unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.headers()[SERVED_BY], "node-a", "{path}");
        }
    }

    #[tokio::test]
    async fn serves_over_tcp_on_loopback() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        assert!(address.ip().is_loopback());
        let app = Router::new().route("/ping", get(|| async { "pong" }));
        let server = tokio::spawn(serve(listener, app));

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /ping HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        server.abort();

        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
        assert!(reply.ends_with("pong"), "{reply}");
    }
}
