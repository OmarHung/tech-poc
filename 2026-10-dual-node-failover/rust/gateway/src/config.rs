use std::time::Duration;

use shared::env::{ConfigError, EnvReader, parse_url};
use shared::wire::is_valid_node_id;
use url::Url;

/// How long a forwarded request may take before the gateway answers 502.
pub const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTarget {
    pub id: String,
    pub url: Url,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    pub port: u16,
    pub nodes: Vec<NodeTarget>,
    /// How often every node's /health is polled to find the primary. Part of the failover time.
    pub poll_interval: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            port: 8100,
            nodes: parse_nodes("a=http://127.0.0.1:8101,b=http://127.0.0.1:8102")
                .expect("valid literal"),
            poll_interval: Duration::from_millis(100),
        }
    }
}

impl GatewayConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let env = EnvReader::new(lookup);
        let d = Self::default();
        let raw_nodes = env.string("NODE_URLS", "");
        Ok(Self {
            port: env.port("PORT", d.port)?,
            nodes: if raw_nodes.is_empty() {
                d.nodes
            } else {
                parse_nodes(&raw_nodes)?
            },
            poll_interval: env.millis("POLL_INTERVAL_MS", d.poll_interval, 10, 10_000)?,
        })
    }

    /// The timeout of one /health poll: three poll intervals, at least 300 ms.
    pub fn health_timeout(&self) -> Duration {
        (self.poll_interval * 3).max(Duration::from_millis(300))
    }
}

/// Parses `a=http://host:port,b=http://host:port`.
pub fn parse_nodes(raw: &str) -> Result<Vec<NodeTarget>, ConfigError> {
    let nodes = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| match part.split_once('=') {
            Some((id, url)) if is_valid_node_id(id) => Ok(NodeTarget {
                id: id.to_owned(),
                url: parse_url("NODE_URLS", url)?,
            }),
            _ => Err(ConfigError(format!(
                "NODE_URLS entries must look like a=http://127.0.0.1:8101, got '{part}'"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let distinct = nodes
        .iter()
        .enumerate()
        .all(|(i, node)| nodes[..i].iter().all(|other| other.id != node.id));
    if nodes.is_empty() || !distinct {
        return Err(ConfigError(format!(
            "NODE_URLS must list distinct node ids, got '{raw}'"
        )));
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn defaults_point_at_the_two_local_nodes() {
        let config = GatewayConfig::from_lookup(|_| None).unwrap();

        assert_eq!(config.port, 8100);
        assert_eq!(
            config
                .nodes
                .iter()
                .map(|n| n.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert_eq!(config.health_timeout(), Duration::from_millis(300));
    }

    #[test]
    fn settings_are_read_from_the_environment() {
        let env: HashMap<&str, &str> = [
            ("PORT", "9100"),
            ("NODE_URLS", "x=http://127.0.0.1:9001"),
            ("POLL_INTERVAL_MS", "200"),
        ]
        .into();

        let config =
            GatewayConfig::from_lookup(|name| env.get(name).map(|v| (*v).to_owned())).unwrap();

        assert_eq!(config.port, 9100);
        assert_eq!(config.nodes[0].id, "x");
        assert_eq!(config.health_timeout(), Duration::from_millis(600));
        assert!(
            GatewayConfig::from_lookup(|n| (n == "POLL_INTERVAL_MS").then(|| "5".into())).is_err()
        );
    }

    #[test]
    fn node_urls_are_parsed() {
        let nodes = parse_nodes("x=http://127.0.0.1:9001, y=http://127.0.0.1:9002").unwrap();

        assert_eq!(
            nodes,
            vec![
                NodeTarget {
                    id: "x".into(),
                    url: Url::parse("http://127.0.0.1:9001").unwrap()
                },
                NodeTarget {
                    id: "y".into(),
                    url: Url::parse("http://127.0.0.1:9002").unwrap()
                },
            ]
        );
    }

    #[test]
    fn bad_node_urls_are_rejected() {
        for raw in [
            "a",
            "a=notaurl",
            "A=http://127.0.0.1:1",
            "a=http://127.0.0.1:1,a=http://127.0.0.1:2",
            ",",
        ] {
            let error = parse_nodes(raw).unwrap_err();
            assert!(error.0.contains("NODE_URLS"), "{raw}: {error}");
        }
    }
}
