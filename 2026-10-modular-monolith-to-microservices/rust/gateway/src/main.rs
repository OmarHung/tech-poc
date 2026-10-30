use std::env;
use std::error::Error;
use std::net::SocketAddr;

use gateway::build_app;
use gateway::routing::{CatalogRoute, Upstreams};
use tokio::net::TcpListener;

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let raw_port = env_or("PORT", "8080");
    let port: u16 = raw_port
        .parse()
        .map_err(|_| format!("PORT must be between 1 and 65535, got '{raw_port}'"))?;
    let upstreams = Upstreams {
        monolith: env_or("MONOLITH_URL", "http://127.0.0.1:8081"),
        catalog: env_or("CATALOG_URL", "http://127.0.0.1:8082"),
    };
    let initial_route: CatalogRoute = env_or("CATALOG_ROUTE", "monolith").parse()?;

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(address).await?;
    println!("gateway listening on http://{address} (catalog route: {initial_route:?})");
    axum::serve(listener, build_app(upstreams, initial_route)?).await?;
    Ok(())
}
