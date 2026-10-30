//! The catalog module extracted into its own process. Same routes, same data model.

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue};
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use catalog_service::catalog::{self, InMemoryCatalog};
use serde::Serialize;
use tokio::net::TcpListener;

const DEFAULT_PORT: &str = "8082";
const SERVED_BY: HeaderName = HeaderName::from_static("x-served-by");

#[derive(Serialize)]
struct Health {
    status: &'static str,
    service: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let raw_port = env::var("PORT").unwrap_or_else(|_| DEFAULT_PORT.to_owned());
    let port: u16 = raw_port
        .parse()
        .map_err(|_| format!("PORT must be between 1 and 65535, got '{raw_port}'"))?;

    let app = Router::new()
        .route(
            "/health",
            get(|| async {
                Json(Health {
                    status: "ok",
                    service: "catalog-service",
                })
            }),
        )
        .merge(catalog::router(Arc::new(InMemoryCatalog::seeded())))
        .layer(map_response(served_by));

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(address).await?;
    println!("catalog-service listening on http://{address}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn served_by(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(SERVED_BY, HeaderValue::from_static("catalog-service"));
    response
}
