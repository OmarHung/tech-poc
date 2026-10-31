use std::time::Duration;

use axum::http::StatusCode;
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::debug;
use url::Url;

/// How long a pooled connection may sit idle. After a network blip a fresh connection beats a half-dead one.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

/// A response that was read to the end.
#[derive(Debug, Clone)]
pub struct RpcResponse {
    pub status: StatusCode,
    pub body: Vec<u8>,
}

impl RpcResponse {
    /// The body as `T`, or `None` if it is not that JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Option<T> {
        serde_json::from_slice(&self.body).ok()
    }
}

/// JSON POSTs to the peer or the witness, each bounded by RENEW_INTERVAL_MS from connect to the last
/// byte of the response. Unreachable means `None`, never an error: it is the normal case in a partition.
#[derive(Debug, Clone)]
pub struct RpcClient {
    http: Client,
    timeout: Duration,
}

impl RpcClient {
    pub fn new(timeout: Duration) -> Result<Self, reqwest::Error> {
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build()?;
        Ok(Self { http, timeout })
    }

    /// The response (any status) or `None` if the target could not be reached in time.
    pub async fn post<T: Serialize + ?Sized>(&self, url: &Url, body: &T) -> Option<RpcResponse> {
        let exchange = async {
            let response = self.http.post(url.clone()).json(body).send().await?;
            let status = response.status();
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>(RpcResponse {
                status,
                body: body.to_vec(),
            })
        };
        match tokio::time::timeout(self.timeout, exchange).await {
            Ok(Ok(response)) => Some(response),
            Ok(Err(error)) => {
                debug!("{url} unreachable: {error}");
                None
            }
            Err(_) => {
                debug!(
                    "{url} did not answer within {} ms",
                    self.timeout.as_millis()
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::routing::post;
    use serde_json::{Value, json};

    use super::*;

    async fn spawn(app: Router) -> Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        Url::parse(&format!("http://{address}/echo")).unwrap()
    }

    #[tokio::test]
    async fn posts_json_and_reads_the_whole_reply() {
        let url =
            spawn(Router::new().route("/echo", post(|body: String| async move { body }))).await;
        let rpc = RpcClient::new(Duration::from_secs(2)).unwrap();

        let response = rpc.post(&url, &json!({"a": 1})).await.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.json::<Value>(), Some(json!({"a": 1})));
        assert_eq!(response.json::<Vec<i32>>(), None);
    }

    #[tokio::test]
    async fn unreachable_or_slow_targets_are_none() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        drop(listener);
        let slow = spawn(Router::new().route(
            "/echo",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                "late"
            }),
        ))
        .await;
        let rpc = RpcClient::new(Duration::from_millis(100)).unwrap();

        assert!(rpc.post(&dead, &json!({})).await.is_none());
        assert!(rpc.post(&slow, &json!({})).await.is_none());
    }
}
