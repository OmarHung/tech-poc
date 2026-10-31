//! Upstream: where the work comes from. An event log both hot-hot workers read in full, and a task queue
//! that leases each task to one worker.

use std::process::ExitCode;

use tracing::{error, info};
use upstream::Upstream;
use upstream::config::UpstreamConfig;

#[tokio::main]
async fn main() -> ExitCode {
    shared::logging::init();
    let config = match UpstreamConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            error!("Cannot start the upstream: {error}");
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
    let upstream = Upstream::new(config.clone());
    let _reaper = upstream.spawn_reaper();
    info!(
        "Upstream on http://127.0.0.1:{}, task lease {} ms",
        config.port,
        config.task_lease_ttl.as_millis()
    );
    if let Err(error) = shared::http::serve(listener, upstream.router()).await {
        error!("Upstream stopped serving: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
