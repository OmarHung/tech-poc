//! One instance of the checkout queue. Start several against the same Redis and SALE_ID and they
//! serve one sale together; configuration comes from the environment (see `config.rs`).

use std::error::Error;
use std::io::IsTerminal;
use std::net::Ipv4Addr;
use std::process::ExitCode;
use std::sync::Arc;

use checkout_queue::app::{self, Sale};
use checkout_queue::clock::{Clock, SystemClock};
use checkout_queue::config::Config;
use checkout_queue::limits;
use checkout_queue::sale_redis::SaleRedis;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_target(false)
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    match limits::raise_open_file_limit() {
        Ok(open_files) if open_files < limits::WANTED_OPEN_FILES => {
            tracing::warn!("can only open {open_files} files; a large crowd may run out of sockets")
        }
        Ok(_) => {}
        Err(error) => tracing::warn!("cannot raise the open-file limit: {error}"),
    }
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let redis = SaleRedis::connect(&config.redis_connection_url(), &config.sale_id, clock)
        .await
        .map_err(|error| format!("cannot connect to Redis at {}: {error}", config.redis_url))?;
    let sale = Sale::new(config, redis)?;
    let config = Arc::clone(&sale.config);

    let started_with = sale.redis.initialize(config.initial_stock).await?;
    if started_with != i64::from(config.initial_stock) {
        tracing::warn!(
            "sale {} already started with {started_with} in stock; ignoring INITIAL_STOCK={}",
            config.sale_id,
            config.initial_stock
        );
    }

    let mut background = app::start_background(&sale, true).await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, config.port)).await?;
    tracing::info!(
        "{} serving sale {} on http://127.0.0.1:{} (max active {})",
        config.instance_name,
        config.sale_id,
        config.port,
        config.max_active
    );

    tokio::select! {
        served = axum::serve(listener, app::router(sale)) => Ok(served?),
        Some(stopped) = background.join_next() => match stopped {
            Ok(Ok(())) => Err("a background loop stopped".into()),
            Ok(Err(bug)) => Err(format!("a background loop failed: {bug}").into()),
            Err(panicked) => Err(format!("a background loop panicked: {panicked}").into()),
        },
    }
}
