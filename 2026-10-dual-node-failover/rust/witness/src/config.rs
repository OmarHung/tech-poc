use std::path::PathBuf;
use std::time::Duration;

use shared::env::{ConfigError, EnvReader};
use shared::wire::is_valid_node_id;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub lease_ttl: Duration,
    /// The two node ids allowed to hold the lease.
    pub nodes: Vec<String>,
}

impl Default for WitnessConfig {
    fn default() -> Self {
        Self {
            port: 8103,
            data_dir: PathBuf::from("data/witness"),
            lease_ttl: Duration::from_secs(3),
            nodes: vec!["a".into(), "b".into()],
        }
    }
}

impl WitnessConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let env = EnvReader::new(lookup);
        let d = Self::default();
        let config = Self {
            port: env.port("PORT", d.port)?,
            data_dir: PathBuf::from(env.string("DATA_DIR", &d.data_dir.to_string_lossy())),
            lease_ttl: env.millis("LEASE_TTL_MS", d.lease_ttl, 100, 60_000)?,
            nodes: env
                .string("NODES", &d.nodes.join(","))
                .split(',')
                .map(|id| id.trim().to_owned())
                .collect(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let valid = self.nodes.len() == 2
            && self.nodes[0] != self.nodes[1]
            && self.nodes.iter().all(|id| is_valid_node_id(id));
        if !valid {
            return Err(ConfigError(format!(
                "NODES must be two different ids of lowercase letters, digits or '-', got '{}'",
                self.nodes.join(",")
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn from_pairs(pairs: &[(&str, &str)]) -> Result<WitnessConfig, ConfigError> {
        let map: HashMap<&str, &str> = pairs.iter().copied().collect();
        WitnessConfig::from_lookup(|name| map.get(name).map(|v| (*v).to_owned()))
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        assert_eq!(from_pairs(&[]).unwrap(), WitnessConfig::default());
    }

    #[test]
    fn settings_are_read_from_the_environment() {
        let config = from_pairs(&[
            ("PORT", "9000"),
            ("NODES", "x, y"),
            ("LEASE_TTL_MS", "1500"),
            ("DATA_DIR", "/tmp/w"),
        ])
        .unwrap();

        assert_eq!(config.port, 9000);
        assert_eq!(config.nodes, ["x", "y"]);
        assert_eq!(config.lease_ttl, Duration::from_millis(1500));
        assert_eq!(config.data_dir, PathBuf::from("/tmp/w"));
    }

    #[test]
    fn nodes_must_be_two_distinct_valid_ids() {
        for nodes in ["a", "a,a", "a,b,c", "a,B", "a,"] {
            let error = from_pairs(&[("NODES", nodes)]).unwrap_err();
            assert!(error.0.contains("NODES"), "{nodes}: {error}");
        }
    }

    #[test]
    fn bad_numbers_name_the_variable() {
        for (name, value) in [("PORT", "0"), ("LEASE_TTL_MS", "abc")] {
            let error = from_pairs(&[(name, value)]).unwrap_err();
            assert!(error.0.contains(name), "{error}");
        }
    }
}
