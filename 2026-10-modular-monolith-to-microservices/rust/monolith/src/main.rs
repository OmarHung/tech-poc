use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use monolith::build_app;
use monolith::config::{CatalogMode, Config};
use monolith::modules::catalog::{HttpCatalogClient, InMemoryCatalog, SharedCatalog};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    let catalog: SharedCatalog = match config.catalog_mode {
        CatalogMode::InProcess => Arc::new(InMemoryCatalog::seeded()),
        CatalogMode::Remote => Arc::new(HttpCatalogClient::new(&config.catalog_url)?),
    };
    let address = SocketAddr::from(([127, 0, 0, 1], config.port));
    let listener = TcpListener::bind(address).await?;
    println!(
        "monolith listening on http://{address} (catalog mode: {})",
        config.catalog_mode.as_str()
    );
    axum::serve(listener, build_app(catalog, config.catalog_mode)).await?;
    Ok(())
}
