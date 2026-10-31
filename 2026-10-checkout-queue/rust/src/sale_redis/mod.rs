//! All Redis access for one sale. Every state change is one Lua script (see `scripts`), so each
//! is atomic on its own and safe to call from any number of instances.

mod reply;
mod scripts;

use std::fmt::{self, Display};

/// One ARGV value after the prefix. `Sync` so request futures stay `Send`.
pub type ScriptArg<'a> = &'a (dyn Display + Sync);
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig, PubSubSink, PubSubStream};
use redis::{Client, RedisError, Value};

pub use reply::ScriptReply;
pub use scripts::{SCRIPTS, SaleScript, SaleScripts};

use crate::clock::Clock;

/// How long one command may take before the request gets a 503 instead.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long one attempt to open a connection may take.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);
/// Reconnect attempts after a dropped connection, before the waiting requests fail.
/// Kept small: a request should hear "unavailable" quickly, and the next one tries again.
const RECONNECT_RETRIES: usize = 2;
const RECONNECT_MIN_DELAY: Duration = Duration::from_millis(100);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum StoreError {
    /// Redis could not be reached or did not answer in time. Retrying later may work.
    Unavailable(RedisError),
    /// A script replied with something this code does not understand: a bug, not an outage.
    Unexpected(String),
}

impl Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(error) => write!(f, "redis unavailable: {error}"),
            Self::Unexpected(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<RedisError> for StoreError {
    fn from(error: RedisError) -> Self {
        Self::Unavailable(error)
    }
}

/// A live subscription to the admitted channel. Keep it whole: dropping it unsubscribes.
pub struct AdmittedSubscription {
    _sink: PubSubSink,
    pub messages: PubSubStream,
}

#[derive(Clone)]
pub struct SaleRedis {
    client: Client,
    connection: ConnectionManager,
    prefix: Arc<str>,
    clock: Arc<dyn Clock>,
}

impl SaleRedis {
    /// Connects (failing fast if Redis is unreachable) and keeps reconnecting after that.
    pub async fn connect(
        url: &str,
        sale_id: &str,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StoreError> {
        let client = Client::open(url)?;
        let config = ConnectionManagerConfig::new()
            .set_response_timeout(Some(RESPONSE_TIMEOUT))
            .set_connection_timeout(Some(CONNECTION_TIMEOUT))
            .set_number_of_retries(RECONNECT_RETRIES)
            .set_min_delay(RECONNECT_MIN_DELAY)
            .set_max_delay(RECONNECT_MAX_DELAY);
        let connection = ConnectionManager::new_with_config(client.clone(), config).await?;
        Ok(Self {
            client,
            connection,
            // The {braces} are a Redis Cluster hash tag: every key of one sale on one slot.
            prefix: format!("cq:{{{sale_id}}}:").into(),
            clock,
        })
    }

    pub fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// `cq:{<sale>}:`, ARGV[1] of every script.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// admit.lua publishes the ticket ids it just admitted here, separated by spaces.
    pub fn admitted_channel(&self) -> String {
        format!("{}admitted", self.prefix)
    }

    pub async fn ping(&self) -> Result<(), StoreError> {
        let mut connection = self.connection.clone();
        let _: Value = redis::cmd("PING").query_async(&mut connection).await?;
        Ok(())
    }

    /// Sets the stock if this sale has never started. Returns the stock the sale actually started with.
    pub async fn initialize(&self, initial_stock: u32) -> Result<i64, StoreError> {
        self.run(&SCRIPTS.init, &[&initial_stock])
            .await?
            .int("initialStock")
    }

    /// Runs a script with the sale prefix as ARGV[1], followed by `args`.
    pub async fn run(
        &self,
        script: &SaleScript,
        args: &[ScriptArg<'_>],
    ) -> Result<ScriptReply, StoreError> {
        ScriptReply::parse(script.name, &self.invoke(script, args).await?)
    }

    /// For scripts that return a list of replies, each shaped like a single script reply.
    pub async fn run_rows(
        &self,
        script: &SaleScript,
        args: &[ScriptArg<'_>],
    ) -> Result<Vec<ScriptReply>, StoreError> {
        ScriptReply::parse_rows(script.name, &self.invoke(script, args).await?)
    }

    /// A dedicated pub/sub connection listening to [`Self::admitted_channel`].
    pub async fn subscribe_admitted(&self) -> Result<AdmittedSubscription, StoreError> {
        let subscribe = async {
            let mut pubsub = self.client.get_async_pubsub().await?;
            pubsub.subscribe(self.admitted_channel()).await?;
            let (sink, messages) = pubsub.split();
            Ok::<_, RedisError>(AdmittedSubscription {
                _sink: sink,
                messages,
            })
        };
        match tokio::time::timeout(CONNECTION_TIMEOUT + RESPONSE_TIMEOUT, subscribe).await {
            Ok(result) => Ok(result?),
            Err(_) => Err(StoreError::Unavailable(RedisError::from(
                std::io::Error::from(std::io::ErrorKind::TimedOut),
            ))),
        }
    }

    async fn invoke(
        &self,
        script: &SaleScript,
        args: &[ScriptArg<'_>],
    ) -> Result<Value, StoreError> {
        let mut invocation = script.script().prepare_invoke();
        invocation.arg(&*self.prefix);
        for arg in args {
            invocation.arg(arg.to_string());
        }
        let mut connection = self.connection.clone();
        Ok(invocation.invoke_async(&mut connection).await?)
    }
}
