use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};

use super::{CatalogError, Product, ReserveRequest, SharedCatalog, parse_id};

/// Catalog HTTP routes, identical whether hosted by the monolith or by catalog-service.
pub fn router(catalog: SharedCatalog) -> Router {
    Router::new()
        .route("/products", get(list_products))
        .route("/products/{id}", get(get_product))
        .route("/products/{id}/reserve", post(reserve_product))
        .with_state(catalog)
}

async fn list_products(
    State(catalog): State<SharedCatalog>,
) -> Result<Json<Vec<Product>>, CatalogError> {
    catalog.list().await.map(Json)
}

async fn get_product(
    State(catalog): State<SharedCatalog>,
    Path(raw_id): Path<String>,
) -> Result<Json<Product>, CatalogError> {
    let id = parse_id(&raw_id, "product id")?;
    catalog.get(id).await.map(Json)
}

async fn reserve_product(
    State(catalog): State<SharedCatalog>,
    Path(raw_id): Path<String>,
    payload: Result<Json<ReserveRequest>, JsonRejection>,
) -> Result<Json<Product>, CatalogError> {
    let id = parse_id(&raw_id, "product id")?;
    let Json(request) =
        payload.map_err(|rejection| CatalogError::InvalidInput(rejection.body_text()))?;
    catalog.reserve(id, request.quantity).await.map(Json)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    use super::super::InMemoryCatalog;
    use super::*;

    async fn call(method: &str, uri: &str, body: &str) -> (StatusCode, serde_json::Value) {
        let app = router(Arc::new(InMemoryCatalog::seeded()));
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn get_product_returns_json() {
        let (status, body) = call("GET", "/products/1", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "Mechanical Keyboard");
    }

    #[tokio::test]
    async fn errors_use_status_code_and_error_body() {
        let cases = [
            ("GET", "/products/999", "", StatusCode::NOT_FOUND),
            ("GET", "/products/abc", "", StatusCode::BAD_REQUEST),
            (
                "POST",
                "/products/1/reserve",
                "{not json",
                StatusCode::BAD_REQUEST,
            ),
            (
                "POST",
                "/products/1/reserve",
                r#"{"quantity":-1}"#,
                StatusCode::BAD_REQUEST,
            ),
            (
                "POST",
                "/products/1/reserve",
                r#"{"quantity":99}"#,
                StatusCode::CONFLICT,
            ),
        ];
        for (method, uri, body, expected) in cases {
            let (status, json) = call(method, uri, body).await;
            assert_eq!(status, expected, "{method} {uri} {body}");
            assert!(json["error"].is_string(), "{method} {uri} {body}: {json}");
        }
    }
}
