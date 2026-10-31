use std::path::PathBuf;
use std::time::Duration;

use shared::env::{ConfigError, EnvReader};
use shared::wire::is_valid_node_id;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbiterMode {
    /// A third process hands out the primary lease (the design under test).
    Witness,
    /// The standby promotes itself when the primary goes quiet. The classic setup that can
    /// split-brain; kept as the control group.
    None,
}

impl ArbiterMode {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Witness => "witness",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// A write is acknowledged once it is on the standby too (or the witness agreed the standby is out).
    Sync,
    /// A write is acknowledged once it is on this node; the standby catches up in the background.
    Async,
}

impl ReplicationMode {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Async => "async",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub port: u16,
    pub node_id: String,
    pub peer_url: Url,
    pub witness_url: Url,
    pub data_dir: PathBuf,
    pub arbiter: ArbiterMode,
    pub replication: ReplicationMode,
    /// How long a lease lasts, and how long a standby must hear nothing from the primary before it
    /// tries to take over.
    pub lease_ttl: Duration,
    /// How often the primary renews its lease and heartbeats the standby. Also the timeout of every
    /// call to them.
    pub renew_interval: Duration,
    /// How long a sync write waits for the standby before asking the witness to go on without it.
    pub replication_timeout: Duration,
    /// Extra wait before this node tries to become primary. Without a witness it plays the part of
    /// keepalived's priority.
    pub election_delay: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            port: 8101,
            node_id: "a".into(),
            peer_url: Url::parse("http://127.0.0.1:8102").expect("valid literal URL"),
            witness_url: Url::parse("http://127.0.0.1:8103").expect("valid literal URL"),
            data_dir: PathBuf::from("data/a"),
            arbiter: ArbiterMode::Witness,
            replication: ReplicationMode::Sync,
            lease_ttl: Duration::from_secs(3),
            renew_interval: Duration::from_millis(500),
            replication_timeout: Duration::from_secs(1),
            election_delay: Duration::ZERO,
        }
    }
}

impl NodeConfig {
    /// The primary treats its leases as ending this much earlier than the other side does, to absorb clock drift.
    pub fn lease_safety_margin(&self) -> Duration {
        self.lease_ttl / 10
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let env = EnvReader::new(lookup);
        let d = Self::default();
        let node_id = env.string("NODE_ID", &d.node_id);
        let config = Self {
            port: env.port("PORT", d.port)?,
            peer_url: env.url("PEER_URL", &d.peer_url)?,
            witness_url: env.url("WITNESS_URL", &d.witness_url)?,
            data_dir: PathBuf::from(env.string("DATA_DIR", &format!("data/{node_id}"))),
            arbiter: env.choice(
                "ARBITER",
                d.arbiter,
                &[
                    ("witness", ArbiterMode::Witness),
                    ("none", ArbiterMode::None),
                ],
            )?,
            replication: env.choice(
                "REPLICATION",
                d.replication,
                &[
                    ("sync", ReplicationMode::Sync),
                    ("async", ReplicationMode::Async),
                ],
            )?,
            lease_ttl: env.millis("LEASE_TTL_MS", d.lease_ttl, 100, 60_000)?,
            renew_interval: env.millis("RENEW_INTERVAL_MS", d.renew_interval, 10, 20_000)?,
            replication_timeout: env.millis(
                "REPLICATION_TIMEOUT_MS",
                d.replication_timeout,
                10,
                30_000,
            )?,
            election_delay: env.millis("ELECTION_DELAY_MS", d.election_delay, 0, 60_000)?,
            node_id,
        };
        config.validate()?;
        Ok(config)
    }

    /// Checks the rules that span several settings. The error names the variable to fix.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !is_valid_node_id(&self.node_id) {
            return Err(ConfigError(format!(
                "NODE_ID must be 1-32 lowercase letters, digits or '-', got '{}'",
                self.node_id
            )));
        }
        // One slow round trip must not cost the primary its lease: it gets several renewals per lease.
        if self.lease_ttl < self.renew_interval * 3 {
            return Err(ConfigError(format!(
                "LEASE_TTL_MS ({}) must be at least 3 x RENEW_INTERVAL_MS ({})",
                self.lease_ttl.as_millis(),
                self.renew_interval.as_millis()
            )));
        }
        // A write stuck on the standby must still have lease left when it asks the witness to go on alone.
        if self.replication_timeout * 2 > self.lease_ttl {
            return Err(ConfigError(format!(
                "REPLICATION_TIMEOUT_MS ({}) must be at most half of LEASE_TTL_MS ({})",
                self.replication_timeout.as_millis(),
                self.lease_ttl.as_millis()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn from_pairs(pairs: &[(&str, &str)]) -> Result<NodeConfig, ConfigError> {
        let map: HashMap<&str, &str> = pairs.iter().copied().collect();
        NodeConfig::from_lookup(|name| map.get(name).map(|v| (*v).to_owned()))
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        let config = from_pairs(&[]).unwrap();

        assert_eq!(config.port, 8101);
        assert_eq!(config.node_id, "a");
        assert_eq!(config.data_dir, PathBuf::from("data/a"));
        assert_eq!(config.arbiter, ArbiterMode::Witness);
        assert_eq!(config.replication, ReplicationMode::Sync);
        assert_eq!(config.lease_safety_margin(), Duration::from_millis(300));
        assert_eq!(config.peer_url.as_str(), "http://127.0.0.1:8102/");
    }

    #[test]
    fn settings_are_read_from_the_environment() {
        let config = from_pairs(&[
            ("NODE_ID", "b"),
            ("PORT", "9102"),
            ("PEER_URL", "http://127.0.0.1:9101"),
            ("WITNESS_URL", "http://127.0.0.1:9103"),
            ("ARBITER", "none"),
            ("REPLICATION", "async"),
            ("LEASE_TTL_MS", "2000"),
            ("RENEW_INTERVAL_MS", "200"),
            ("REPLICATION_TIMEOUT_MS", "400"),
            ("ELECTION_DELAY_MS", "1500"),
        ])
        .unwrap();

        assert_eq!(config.node_id, "b");
        assert_eq!(config.data_dir, PathBuf::from("data/b"));
        assert_eq!(config.port, 9102);
        assert_eq!(config.peer_url.as_str(), "http://127.0.0.1:9101/");
        assert_eq!(config.witness_url.as_str(), "http://127.0.0.1:9103/");
        assert_eq!(config.arbiter, ArbiterMode::None);
        assert_eq!(config.replication, ReplicationMode::Async);
        assert_eq!(config.lease_ttl, Duration::from_secs(2));
        assert_eq!(config.renew_interval, Duration::from_millis(200));
        assert_eq!(config.replication_timeout, Duration::from_millis(400));
        assert_eq!(config.election_delay, Duration::from_millis(1500));
        assert_eq!(
            (config.arbiter.wire_name(), config.replication.wire_name()),
            ("none", "async")
        );
    }

    #[test]
    fn data_dir_can_be_set() {
        let config = from_pairs(&[("DATA_DIR", "/tmp/x")]).unwrap();

        assert_eq!(config.data_dir, PathBuf::from("/tmp/x"));
    }

    #[test]
    fn bad_settings_name_the_variable_to_fix() {
        let cases = [
            ("NODE_ID", "Node A", "NODE_ID"),
            ("ARBITER", "zookeeper", "ARBITER"),
            ("REPLICATION", "semi", "REPLICATION"),
            ("PEER_URL", "not a url", "PEER_URL"),
            ("PEER_URL", "ftp://host", "PEER_URL"),
            ("RENEW_INTERVAL_MS", "1500", "LEASE_TTL_MS"),
            ("REPLICATION_TIMEOUT_MS", "2000", "REPLICATION_TIMEOUT_MS"),
            ("PORT", "70000", "PORT"),
            ("LEASE_TTL_MS", "abc", "LEASE_TTL_MS"),
        ];
        for (name, value, expected) in cases {
            let error = from_pairs(&[(name, value)]).unwrap_err();
            assert!(error.0.contains(expected), "{name}={value}: {error}");
        }
    }
}
