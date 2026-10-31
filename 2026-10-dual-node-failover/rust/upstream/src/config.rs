use std::time::Duration;

use shared::env::{ConfigError, EnvReader};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamConfig {
    pub port: u16,
    /// How long a worker holds a task without a heartbeat before it is handed to someone else.
    pub task_lease_ttl: Duration,
    /// How often expired task leases are looked for.
    pub reap_interval: Duration,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            port: 8120,
            task_lease_ttl: Duration::from_secs(2),
            reap_interval: Duration::from_millis(100),
        }
    }
}

impl UpstreamConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let env = EnvReader::new(lookup);
        let d = Self::default();
        Ok(Self {
            port: env.port("PORT", d.port)?,
            task_lease_ttl: env.millis("TASK_LEASE_TTL_MS", d.task_lease_ttl, 100, 60_000)?,
            ..d
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn from_pairs(pairs: &[(&str, &str)]) -> Result<UpstreamConfig, ConfigError> {
        let map: HashMap<&str, &str> = pairs.iter().copied().collect();
        UpstreamConfig::from_lookup(|name| map.get(name).map(|v| (*v).to_owned()))
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        let config = from_pairs(&[]).unwrap();

        assert_eq!(config, UpstreamConfig::default());
        assert_eq!(
            (config.port, config.task_lease_ttl, config.reap_interval),
            (8120, Duration::from_secs(2), Duration::from_millis(100))
        );
    }

    #[test]
    fn settings_are_read_from_the_environment() {
        let config = from_pairs(&[("PORT", "9120"), ("TASK_LEASE_TTL_MS", "500")]).unwrap();

        assert_eq!(config.port, 9120);
        assert_eq!(config.task_lease_ttl, Duration::from_millis(500));
    }

    #[test]
    fn bad_settings_name_the_variable() {
        for (name, value) in [
            ("PORT", "0"),
            ("TASK_LEASE_TTL_MS", "99"),
            ("TASK_LEASE_TTL_MS", "60001"),
            ("TASK_LEASE_TTL_MS", "2s"),
        ] {
            let error = from_pairs(&[(name, value)]).unwrap_err();
            assert!(error.0.starts_with(name), "{name}={value}: {error}");
        }
    }
}
