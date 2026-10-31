//! Worker: one of two identical processes that never talk to each other. MODE=hothot reads and
//! processes every event (like the other worker does); MODE=dispatch takes tasks the upstream leases to it.

use std::path::Path;
use std::process::ExitCode;

use tracing::{error, info};
use worker::config::WorkerConfig;
use worker::hosting::Worker;

#[tokio::main]
async fn main() -> ExitCode {
    shared::logging::init();
    let config = match WorkerConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            error!("Cannot start the worker: {error}");
            return ExitCode::FAILURE;
        }
    };
    let worker = match Worker::open(config.clone()) {
        Ok(worker) => worker,
        Err(error) => {
            error!("Cannot start worker {}: {error}", config.worker_id);
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
    let _work = worker.spawn_loop();
    info!(
        "Worker {} ({}) on http://127.0.0.1:{}, upstream {}, sink {}, data {}",
        config.worker_id,
        config.mode.wire_name(),
        config.port,
        config.upstream_url,
        config.sink_url,
        absolute(&config.data_dir)
    );
    if let Err(error) = shared::http::serve(listener, worker.router()).await {
        error!("Worker {} stopped serving: {error}", config.worker_id);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn absolute(path: &Path) -> String {
    std::path::absolute(path)
        .unwrap_or_else(|_| path.to_owned())
        .display()
        .to_string()
}
