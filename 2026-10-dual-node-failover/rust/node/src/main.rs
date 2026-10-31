//! One of the two data nodes. Which one is primary is decided at runtime (see cluster/election.rs).

use std::path::Path;
use std::process::ExitCode;

use node::config::NodeConfig;
use node::hosting::Node;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    shared::logging::init();
    let config = match NodeConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            error!("Cannot start the node: {error}");
            return ExitCode::FAILURE;
        }
    };
    let node = match Node::open(config.clone()) {
        Ok(node) => node,
        Err(error) => {
            error!("Cannot start node {}: {error}", config.node_id);
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
    let _workers = node.spawn_workers();
    info!(
        "Node {} on http://127.0.0.1:{}: arbiter {}, replication {}, peer {}, data {}",
        config.node_id,
        config.port,
        config.arbiter.wire_name(),
        config.replication.wire_name(),
        config.peer_url,
        absolute(&config.data_dir)
    );
    if let Err(error) = shared::http::serve(listener, node.router()).await {
        error!("Node {} stopped serving: {error}", config.node_id);
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
