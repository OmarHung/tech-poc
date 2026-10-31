//! Witness: the third vote that lets two nodes fail over without both becoming primary.

use std::process::ExitCode;
use std::sync::Arc;

use tracing::{error, info};
use witness::config::WitnessConfig;
use witness::lease_table::LeaseTable;

#[tokio::main]
async fn main() -> ExitCode {
    shared::logging::init();
    let config = match WitnessConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            error!("Cannot start the witness: {error}");
            return ExitCode::FAILURE;
        }
    };
    let table = match LeaseTable::open(config.clone()) {
        Ok(table) => Arc::new(table),
        Err(error) => {
            error!("Cannot start the witness: {error}");
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
    let state = table.state();
    info!(
        "Witness for nodes {} on http://127.0.0.1:{}, lease {} ms; holder {}, epoch {}",
        config.nodes.join(", "),
        config.port,
        config.lease_ttl.as_millis(),
        state.holder.as_deref().unwrap_or("none"),
        state.epoch
    );
    if let Err(error) = shared::http::serve(listener, witness::router(table)).await {
        error!("Witness stopped serving: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
