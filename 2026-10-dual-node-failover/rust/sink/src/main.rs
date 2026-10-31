//! Sink: where results land. Applies each key once and counts every extra copy.

use std::process::ExitCode;
use std::sync::Arc;

use sink::result_store::ResultStore;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    shared::logging::init();
    let port = match sink::port_from_lookup(|name| std::env::var(name).ok()) {
        Ok(port) => port,
        Err(error) => {
            error!("Cannot start the sink: {error}");
            return ExitCode::FAILURE;
        }
    };
    let listener = match shared::http::bind(port).await {
        Ok(listener) => listener,
        Err(error) => {
            error!("Cannot listen on 127.0.0.1:{port}: {error}");
            return ExitCode::FAILURE;
        }
    };
    info!("Sink on http://127.0.0.1:{port}");
    let store = Arc::new(ResultStore::new());
    if let Err(error) = shared::http::serve(listener, sink::router(store)).await {
        error!("Sink stopped serving: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
