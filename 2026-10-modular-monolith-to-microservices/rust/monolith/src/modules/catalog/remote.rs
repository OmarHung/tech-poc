//! `CatalogPort` over HTTP: what the monolith uses once the catalog is extracted.

use std::time::Duration;

use super::{CatalogError, CatalogPort, ErrorBody, Product, ReserveRequest};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

pub struct HttpCatalogClient {
    http: reqwest::Client,
    base_url: String,
}

impl HttpCatalogClient {
    pub fn new(base_url: &str) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

#[async_trait]
impl CatalogPort for HttpCatalogClient {
    async fn list(&self) -> Result<Vec<Product>, CatalogError> {
        decode(self.http.get(self.url("/products")).send().await).await
    }

    async fn get(&self, id: u64) -> Result<Product, CatalogError> {
        decode(
            self.http
                .get(self.url(&format!("/products/{id}")))
                .send()
                .await,
        )
        .await
    }

    async fn reserve(&self, id: u64, quantity: u64) -> Result<Product, CatalogError> {
        let request = self.http.post(self.url(&format!("/products/{id}/reserve")));
        decode(request.json(&ReserveRequest { quantity }).send().await).await
    }
}

/// Maps the catalog-service response back into the same `CatalogError` the
/// in-process catalog would have returned, so callers cannot tell the difference.
async fn decode<T: DeserializeOwned>(
    sent: Result<reqwest::Response, reqwest::Error>,
) -> Result<T, CatalogError> {
    let response = sent.map_err(unavailable)?;
    let status = response.status();
    if status.is_success() {
        return response.json::<T>().await.map_err(unavailable);
    }
    let message = match response.json::<ErrorBody>().await {
        Ok(body) => body.error,
        Err(_) => format!("catalog-service returned {status}"),
    };
    Err(match status {
        StatusCode::BAD_REQUEST => CatalogError::InvalidInput(message),
        StatusCode::NOT_FOUND => CatalogError::NotFound(message),
        StatusCode::CONFLICT => CatalogError::InsufficientStock(message),
        _ => {
            eprintln!("catalog-service error {status}: {message}");
            CatalogError::Unavailable("catalog unavailable".into())
        }
    })
}

/// Logs the detail server-side; the client only learns that the catalog is unavailable.
fn unavailable(error: reqwest::Error) -> CatalogError {
    eprintln!("catalog-service request failed: {error}");
    CatalogError::Unavailable("catalog unavailable".into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::{InMemoryCatalog, router};
    use tokio::net::TcpListener;

    use super::*;

    /// Starts the real catalog routes on a random port, standing in for catalog-service.
    async fn spawn_catalog_service() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = router(Arc::new(InMemoryCatalog::seeded()));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn round_trips_successful_calls() {
        let client = HttpCatalogClient::new(&spawn_catalog_service().await).unwrap();
        assert_eq!(client.list().await.unwrap().len(), 3);
        assert_eq!(client.reserve(1, 2).await.unwrap().stock, 48);
        assert_eq!(client.get(1).await.unwrap().stock, 48);
    }

    #[tokio::test]
    async fn maps_error_statuses_back_to_catalog_errors() {
        let client = HttpCatalogClient::new(&spawn_catalog_service().await).unwrap();
        assert_eq!(
            client.get(99).await,
            Err(CatalogError::product_not_found(99))
        );
        assert_eq!(
            client.reserve(1, 99).await,
            Err(CatalogError::insufficient_stock(1, 99, 50))
        );
        assert!(matches!(
            client.reserve(1, 0).await,
            Err(CatalogError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn unreachable_service_is_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client = HttpCatalogClient::new(&format!("http://{address}")).unwrap();
        assert_eq!(
            client.list().await,
            Err(CatalogError::Unavailable("catalog unavailable".into()))
        );
    }
}
