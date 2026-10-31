use std::path::PathBuf;
use std::time::Duration;

use shared::env::{ConfigError, EnvReader};
use shared::wire::is_valid_node_id;
use url::Url;

/// How long to wait before calling the upstream or the sink again after it could not be reached.
pub const RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerMode {
    /// Hot-hot redundancy: read every event from the upstream log and process all of them, like the
    /// other worker does.
    HotHot,
    /// Dispatched worker: take one task at a time from the upstream queue, under a lease.
    Dispatch,
}

impl WorkerMode {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::HotHot => "hothot",
            Self::Dispatch => "dispatch",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    /// Resume from the saved position and replay what was missed while down.
    Checkpoint,
    /// Jump to the newest event, skipping whatever happened while down. The naive way to rejoin; kept
    /// as a control group.
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub port: u16,
    pub worker_id: String,
    pub mode: WorkerMode,
    pub upstream_url: Url,
    pub sink_url: Url,
    pub data_dir: PathBuf,
    pub start_from: StartFrom,
    /// When false, every result carries this worker's clock, so the two hot-hot workers never agree.
    /// Shows why processing must be deterministic.
    pub deterministic: bool,
    /// When false, a dispatched worker writes its result even after losing the lease. Control group
    /// for the frozen-worker case.
    pub lease_check: bool,
    /// How long one dispatched task takes.
    pub work_time: Duration,
    /// Timeout of every call to the upstream or the sink, on top of any long-poll wait.
    pub rpc_timeout: Duration,
    /// How long a read of the event log or a task lease request waits for new work. Not an
    /// environment variable; tests shorten it.
    pub poll_wait: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            port: 8121,
            worker_id: "a".into(),
            mode: WorkerMode::HotHot,
            upstream_url: Url::parse("http://127.0.0.1:8120").expect("valid literal URL"),
            sink_url: Url::parse("http://127.0.0.1:8123").expect("valid literal URL"),
            data_dir: PathBuf::from("data/worker-a"),
            start_from: StartFrom::Checkpoint,
            deterministic: true,
            lease_check: true,
            work_time: Duration::from_millis(300),
            rpc_timeout: Duration::from_secs(1),
            poll_wait: Duration::from_millis(500),
        }
    }
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let env = EnvReader::new(lookup);
        let d = Self::default();
        let worker_id = env.string("WORKER_ID", &d.worker_id);
        let config = Self {
            port: env.port("PORT", d.port)?,
            mode: env.choice(
                "MODE",
                d.mode,
                &[
                    ("hothot", WorkerMode::HotHot),
                    ("dispatch", WorkerMode::Dispatch),
                ],
            )?,
            upstream_url: env.url("UPSTREAM_URL", &d.upstream_url)?,
            sink_url: env.url("SINK_URL", &d.sink_url)?,
            data_dir: PathBuf::from(env.string("DATA_DIR", &format!("data/worker-{worker_id}"))),
            start_from: env.choice(
                "START_FROM",
                d.start_from,
                &[
                    ("checkpoint", StartFrom::Checkpoint),
                    ("latest", StartFrom::Latest),
                ],
            )?,
            deterministic: env.choice(
                "DETERMINISTIC",
                d.deterministic,
                &[("true", true), ("false", false)],
            )?,
            lease_check: env.choice(
                "LEASE_CHECK",
                d.lease_check,
                &[("on", true), ("off", false)],
            )?,
            work_time: env.millis("WORK_MS", d.work_time, 0, 60_000)?,
            rpc_timeout: env.millis("RPC_TIMEOUT_MS", d.rpc_timeout, 10, 30_000)?,
            worker_id,
            ..d
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !is_valid_node_id(&self.worker_id) {
            return Err(ConfigError(format!(
                "WORKER_ID must be 1-32 lowercase letters, digits or '-', got '{}'",
                self.worker_id
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn from_pairs(pairs: &[(&str, &str)]) -> Result<WorkerConfig, ConfigError> {
        let map: HashMap<&str, &str> = pairs.iter().copied().collect();
        WorkerConfig::from_lookup(|name| map.get(name).map(|v| (*v).to_owned()))
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        let config = from_pairs(&[]).unwrap();

        assert_eq!(config, WorkerConfig::default());
        assert_eq!(config.mode, WorkerMode::HotHot);
        assert_eq!(config.data_dir, PathBuf::from("data/worker-a"));
        assert_eq!(config.start_from, StartFrom::Checkpoint);
        assert!(config.deterministic && config.lease_check);
        assert_eq!(
            (config.port, config.work_time, config.rpc_timeout),
            (8121, Duration::from_millis(300), Duration::from_secs(1))
        );
        assert_eq!(config.poll_wait, Duration::from_millis(500));
        assert_eq!(config.upstream_url.as_str(), "http://127.0.0.1:8120/");
        assert_eq!(config.sink_url.as_str(), "http://127.0.0.1:8123/");
    }

    #[test]
    fn settings_are_read_from_the_environment() {
        let config = from_pairs(&[
            ("WORKER_ID", "b"),
            ("PORT", "9122"),
            ("MODE", "dispatch"),
            ("START_FROM", "latest"),
            ("DETERMINISTIC", "false"),
            ("LEASE_CHECK", "off"),
            ("WORK_MS", "50"),
            ("RPC_TIMEOUT_MS", "250"),
            ("UPSTREAM_URL", "http://127.0.0.1:9120"),
            ("SINK_URL", "http://127.0.0.1:9123"),
        ])
        .unwrap();

        assert_eq!(
            (config.worker_id.as_str(), &config.data_dir),
            ("b", &PathBuf::from("data/worker-b"))
        );
        assert_eq!(config.port, 9122);
        assert_eq!(config.mode, WorkerMode::Dispatch);
        assert_eq!(config.start_from, StartFrom::Latest);
        assert!(!config.deterministic && !config.lease_check);
        assert_eq!(config.work_time, Duration::from_millis(50));
        assert_eq!(config.rpc_timeout, Duration::from_millis(250));
        assert_eq!(config.upstream_url.as_str(), "http://127.0.0.1:9120/");
        assert_eq!(config.sink_url.as_str(), "http://127.0.0.1:9123/");
        assert_eq!(
            (config.mode.wire_name(), WorkerMode::HotHot.wire_name()),
            ("dispatch", "hothot")
        );
    }

    #[test]
    fn data_dir_can_be_set_and_work_can_take_no_time() {
        let config = from_pairs(&[("DATA_DIR", "/tmp/w"), ("WORK_MS", "0")]).unwrap();

        assert_eq!(config.data_dir, PathBuf::from("/tmp/w"));
        assert_eq!(config.work_time, Duration::ZERO);
    }

    #[test]
    fn bad_settings_name_the_variable() {
        for (name, value) in [
            ("WORKER_ID", "Worker A"),
            ("MODE", "both"),
            ("START_FROM", "beginning"),
            ("DETERMINISTIC", "yes"),
            ("LEASE_CHECK", "yes"),
            ("WORK_MS", "60001"),
            ("RPC_TIMEOUT_MS", "9"),
            ("UPSTREAM_URL", "not a url"),
            ("SINK_URL", "ftp://host"),
            ("PORT", "70000"),
        ] {
            let error = from_pairs(&[(name, value)]).unwrap_err();
            assert!(error.0.starts_with(name), "{name}={value}: {error}");
        }
        assert_eq!(
            from_pairs(&[("MODE", "both")]).unwrap_err().0,
            "MODE must be one of hothot, dispatch, got 'both'"
        );
    }
}
