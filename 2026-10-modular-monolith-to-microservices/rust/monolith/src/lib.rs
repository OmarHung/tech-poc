//! The modular monolith: `modules::orders` stays here, `modules::catalog` has been
//! migrated to catalog-service and is kept in place for comparison and rollback.

pub mod config;
pub mod modules;

use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue};
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::config::CatalogMode;
use crate::modules::catalog::{self, SharedCatalog};
use crate::modules::orders::{self, OrderService};

const SERVED_BY: HeaderName = HeaderName::from_static("x-served-by");

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct Health {
    status: &'static str,
    service: &'static str,
    catalog_mode: &'static str,
}

/// Catalog routes and orders both go through `catalog`, so in remote mode the
/// monolith delegates everything catalog-related to catalog-service and never
/// holds a second copy of the data.
pub fn build_app(catalog: SharedCatalog, mode: CatalogMode) -> Router {
    let orders = Arc::new(OrderService::new(Arc::clone(&catalog)));
    let health = Health {
        status: "ok",
        service: "monolith",
        catalog_mode: mode.as_str(),
    };
    Router::new()
        .route("/health", get(move || async move { Json(health) }))
        .merge(catalog::router(catalog))
        .merge(orders::router(orders))
        .layer(map_response(served_by))
}

async fn served_by(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(SERVED_BY, HeaderValue::from_static("monolith"));
    response
}
