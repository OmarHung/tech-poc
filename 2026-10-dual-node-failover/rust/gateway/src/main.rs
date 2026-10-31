//! Gateway: the clients' single entry point. Forwards everything to whichever node is primary right now.

use std::process::ExitCode;
use std::sync::Arc;

use gateway::Gateway;
use gateway::config::GatewayConfig;
use shared::supervise::spawn_supervised;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    shared::logging::init();
    let config = match GatewayConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            error!("Cannot start the gateway: {error}");
            return ExitCode::FAILURE;
        }
    };
    let gateway = match Gateway::new(config.clone()) {
        Ok(gateway) => Arc::new(gateway),
        Err(error) => {
            error!("Cannot set up the gateway's HTTP client: {error}");
            return ExitCode::FAILURE;
        }
    };
    let listener = match shared::http::bind(config.port).await {
        Ok(listener) => listener,
        Err(error) => {
            error!("Cannot listen on 127.0.0.1:{}: {error}", config.port);
            return ExitCode::FAILURE;
        }
    };
    let tracker = Arc::clone(gateway.tracker());
    let _poller = spawn_supervised("primary tracker", move || Arc::clone(&tracker).run());
    let nodes: Vec<String> = config
        .nodes
        .iter()
        .map(|n| format!("{}={}", n.id, n.url))
        .collect();
    info!(
        "Gateway on http://127.0.0.1:{} for {}, polling every {} ms",
        config.port,
        nodes.join(", "),
        config.poll_interval.as_millis()
    );
    if let Err(error) = shared::http::serve(listener, gateway::router(gateway)).await {
        error!("Gateway stopped serving: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
