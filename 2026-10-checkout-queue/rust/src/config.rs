//! Settings for one process. Every instance pointed at the same Redis and SALE_ID serves the
//! same sale, so scaling out is just starting more processes.

use std::fmt;
use std::time::Duration;

use axum::http::HeaderValue;

/// The longest admission, payment, heartbeat, poll or tick setting: one day.
const MAX_DURATION_MS: u64 = 86_400_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub port: u16,
    /// `host:port` (what verify/run.sh passes) or a full `redis://` URL.
    pub redis_url: String,
    pub instance_name: String,
    pub sale_id: String,
    pub initial_stock: u32,
    pub max_per_order: u32,
    /// How many admitted users may be in checkout at once. This is the knob that flattens the spike.
    pub max_active: u32,
    /// How long an admitted user has to place the order before the slot goes to the next in line.
    pub admission_window_ms: u64,
    /// How long an order holds its stock waiting for payment.
    pub payment_window_ms: u64,
    /// A waiting user who has not polled for this long is skipped when their turn comes.
    pub heartbeat_timeout_ms: u64,
    /// How often clients are told to poll; also how often event streams refresh positions.
    pub poll_interval_ms: u64,
    /// How often each instance releases unpaid orders and admits the next users.
    pub tick_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 8090,
            redis_url: "127.0.0.1:6379".to_owned(),
            instance_name: "checkout-queue".to_owned(),
            sale_id: "demo".to_owned(),
            initial_stock: 100,
            max_per_order: 2,
            max_active: 50,
            admission_window_ms: 120_000,
            payment_window_ms: 300_000,
            heartbeat_timeout_ms: 15_000,
            poll_interval_ms: 1_000,
            tick_ms: 200,
        }
    }
}

/// A setting that cannot be used. The message names the variable to fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Reads every variable through `env`; unset ones keep their default.
    pub fn from_lookup(env: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let d = Self::default();
        let config = Self {
            port: int(&env, "PORT", d.port, 1, u16::MAX)?,
            redis_url: env("REDIS_URL").unwrap_or(d.redis_url),
            instance_name: env("INSTANCE_NAME").unwrap_or(d.instance_name),
            sale_id: env("SALE_ID").unwrap_or(d.sale_id),
            initial_stock: int(&env, "INITIAL_STOCK", d.initial_stock, 0, 10_000_000)?,
            max_per_order: int(&env, "MAX_PER_ORDER", d.max_per_order, 1, 100)?,
            max_active: int(&env, "MAX_ACTIVE", d.max_active, 1, 1_000_000)?,
            admission_window_ms: millis(&env, "ADMISSION_WINDOW_MS", d.admission_window_ms)?,
            payment_window_ms: millis(&env, "PAYMENT_WINDOW_MS", d.payment_window_ms)?,
            heartbeat_timeout_ms: millis(&env, "HEARTBEAT_TIMEOUT_MS", d.heartbeat_timeout_ms)?,
            poll_interval_ms: millis(&env, "POLL_INTERVAL_MS", d.poll_interval_ms)?,
            tick_ms: millis(&env, "TICK_MS", d.tick_ms)?,
        };
        config.validate()?;
        Ok(config)
    }

    /// Checks the rules that span several settings, or that hand-built configs could break.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.redis_url.trim().is_empty() {
            return Err(ConfigError("REDIS_URL must not be empty".to_owned()));
        }
        if !is_sale_id(&self.sale_id) {
            return Err(ConfigError(format!(
                "SALE_ID must be 1-64 letters, digits, '-' or '_', got '{}'",
                self.sale_id
            )));
        }
        // It goes out in the X-Served-By header of every response.
        if HeaderValue::from_str(&self.instance_name).is_err() {
            return Err(ConfigError(format!(
                "INSTANCE_NAME must be printable text, got '{}'",
                self.instance_name.escape_debug()
            )));
        }
        // A client that misses a poll or two (GC pause, flaky mobile network) must not lose its place.
        if self.heartbeat_timeout_ms < self.poll_interval_ms.saturating_mul(3) {
            return Err(ConfigError(format!(
                "HEARTBEAT_TIMEOUT_MS ({}) must be at least 3 x POLL_INTERVAL_MS ({})",
                self.heartbeat_timeout_ms, self.poll_interval_ms
            )));
        }
        Ok(())
    }

    /// The URL the redis crate understands: `host:port` becomes `redis://host:port`.
    pub fn redis_connection_url(&self) -> String {
        let url = self.redis_url.trim();
        if url.contains("://") {
            url.to_owned()
        } else {
            format!("redis://{url}")
        }
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub fn tick_interval(&self) -> Duration {
        Duration::from_millis(self.tick_ms)
    }
}

/// `^[A-Za-z0-9_-]{1,64}$`: the sale id goes inside a `{...}` hash tag in every key.
fn is_sale_id(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Plain digits only (no sign, no decimals, no spaces), within `min..=max`.
fn int<T>(
    env: &impl Fn(&str) -> Option<String>,
    name: &str,
    fallback: T,
    min: T,
    max: T,
) -> Result<T, ConfigError>
where
    T: Copy + PartialOrd + fmt::Display + std::str::FromStr,
{
    let Some(raw) = env(name) else {
        return Ok(fallback);
    };
    let digits_only = !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit());
    match raw.parse::<T>() {
        Ok(value) if digits_only && value >= min && value <= max => Ok(value),
        _ => Err(ConfigError(format!(
            "{name} must be an integer between {min} and {max}, got '{raw}'"
        ))),
    }
}

fn millis(
    env: &impl Fn(&str) -> Option<String>,
    name: &str,
    fallback: u64,
) -> Result<u64, ConfigError> {
    int(env, name, fallback, 1, MAX_DURATION_MS)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn uses_defaults_when_nothing_is_set() {
        let config = Config::from_lookup(|_| None).unwrap();

        assert_eq!(config, Config::default());
    }

    #[test]
    fn reads_every_variable() {
        let env = lookup(&[
            ("PORT", "9001"),
            ("REDIS_URL", "redis.local:6380"),
            ("INSTANCE_NAME", "node-a"),
            ("SALE_ID", "black-friday"),
            ("INITIAL_STOCK", "500"),
            ("MAX_PER_ORDER", "3"),
            ("MAX_ACTIVE", "80"),
            ("ADMISSION_WINDOW_MS", "30000"),
            ("PAYMENT_WINDOW_MS", "60000"),
            ("HEARTBEAT_TIMEOUT_MS", "9000"),
            ("POLL_INTERVAL_MS", "2000"),
            ("TICK_MS", "50"),
        ]);

        let config = Config::from_lookup(env).unwrap();

        assert_eq!(
            config,
            Config {
                port: 9001,
                redis_url: "redis.local:6380".to_owned(),
                instance_name: "node-a".to_owned(),
                sale_id: "black-friday".to_owned(),
                initial_stock: 500,
                max_per_order: 3,
                max_active: 80,
                admission_window_ms: 30_000,
                payment_window_ms: 60_000,
                heartbeat_timeout_ms: 9_000,
                poll_interval_ms: 2_000,
                tick_ms: 50,
            }
        );
        assert_eq!(config.poll_interval(), Duration::from_secs(2));
        assert_eq!(config.tick_interval(), Duration::from_millis(50));
    }

    #[test]
    fn rejects_invalid_values_naming_the_variable() {
        let cases = [
            ("PORT", "0"),
            ("PORT", "http"),
            ("PORT", "65536"),
            ("PORT", ""),
            ("INITIAL_STOCK", "-1"),
            ("INITIAL_STOCK", "+5"),
            ("INITIAL_STOCK", " 5"),
            ("INITIAL_STOCK", "10000001"),
            ("MAX_PER_ORDER", "0"),
            ("MAX_PER_ORDER", "101"),
            ("MAX_ACTIVE", "0"),
            ("ADMISSION_WINDOW_MS", "0"),
            ("ADMISSION_WINDOW_MS", "86400001"),
            ("PAYMENT_WINDOW_MS", "1.5"),
            ("TICK_MS", "0"),
            ("TICK_MS", "99999999999999999999999"),
            ("SALE_ID", "has space"),
            ("SALE_ID", "brace}breaks{the-hash-tag"),
            ("SALE_ID", ""),
            ("REDIS_URL", " "),
            ("INSTANCE_NAME", "line\nbreak"),
            ("HEARTBEAT_TIMEOUT_MS", "2999"),
        ];
        for (name, value) in cases {
            let error = Config::from_lookup(lookup(&[(name, value)])).unwrap_err();

            assert!(
                error.to_string().contains(name),
                "{name}={value:?}: {error}"
            );
        }
    }

    #[test]
    fn accepts_the_edges_of_each_range() {
        let env = lookup(&[
            ("PORT", "65535"),
            ("INITIAL_STOCK", "0"),
            ("MAX_ACTIVE", "1000000"),
            ("SALE_ID", &"a".repeat(64)),
            ("HEARTBEAT_TIMEOUT_MS", "3000"),
            ("ADMISSION_WINDOW_MS", "86400000"),
        ]);

        let config = Config::from_lookup(env).unwrap();

        assert_eq!(
            (config.port, config.initial_stock, config.max_active),
            (65535, 0, 1_000_000)
        );
    }

    #[test]
    fn redis_url_accepts_host_port_or_a_full_url() {
        let plain = Config {
            redis_url: "127.0.0.1:6392".to_owned(),
            ..Config::default()
        };
        let full = Config {
            redis_url: "redis://user:secret@cache:6379/0".to_owned(),
            ..Config::default()
        };

        assert_eq!(plain.redis_connection_url(), "redis://127.0.0.1:6392");
        assert_eq!(
            full.redis_connection_url(),
            "redis://user:secret@cache:6379/0"
        );
    }
}
