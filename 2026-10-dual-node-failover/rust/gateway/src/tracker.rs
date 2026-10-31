use std::sync::{Arc, PoisonError, RwLock};

use serde::Deserialize;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::warn;

use crate::config::{GatewayConfig, NodeTarget};

/// The part of a node's /health the gateway needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NodeStatus {
    pub role: Option<String>,
    pub epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryChoice {
    pub target: NodeTarget,
    pub epoch: i64,
}

/// Polls every node's /health and remembers which one is primary, the way HAProxy is pointed at
/// Patroni's /primary check. If two nodes claim it (only possible without a witness), the newer epoch wins.
pub struct PrimaryTracker {
    config: GatewayConfig,
    http: reqwest::Client,
    current: RwLock<Option<PrimaryChoice>>,
    nudge: Notify,
}

impl PrimaryTracker {
    pub fn new(config: GatewayConfig, http: reqwest::Client) -> Self {
        Self {
            config,
            http,
            current: RwLock::new(None),
            nudge: Notify::new(),
        }
    }

    pub fn current(&self) -> Option<PrimaryChoice> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the current choice, e.g. in tests that stop polling.
    pub fn set_current(&self, choice: Option<PrimaryChoice>) {
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = choice;
    }

    /// Polls again right away, e.g. after the primary failed a request. Several nudges before the
    /// next poll collapse into one.
    pub fn nudge(&self) {
        self.nudge.notify_one();
    }

    /// The node that says it is primary with the highest epoch; the first listed wins a tie.
    pub fn choose(polled: &[(NodeTarget, Option<NodeStatus>)]) -> Option<PrimaryChoice> {
        polled
            .iter()
            .filter_map(|(target, status)| {
                status
                    .as_ref()
                    .filter(|s| s.role.as_deref() == Some("primary"))
                    .map(|s| PrimaryChoice {
                        target: target.clone(),
                        epoch: s.epoch,
                    })
            })
            .fold(None, |best: Option<PrimaryChoice>, choice| match best {
                Some(best) if best.epoch >= choice.epoch => Some(best),
                _ => Some(choice),
            })
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            self.poll_once().await;
            tokio::select! {
                () = self.nudge.notified() => {}
                () = tokio::time::sleep(self.config.poll_interval) => {}
            }
        }
    }

    /// Polls every node once and updates the current primary.
    pub async fn poll_once(&self) {
        let choice = Self::choose(&self.poll_all().await);
        let previous = std::mem::replace(
            &mut *self.current.write().unwrap_or_else(PoisonError::into_inner),
            choice.clone(),
        );
        let key = |c: &Option<PrimaryChoice>| c.as_ref().map(|c| (c.target.id.clone(), c.epoch));
        if key(&choice) != key(&previous) {
            match &choice {
                Some(choice) => warn!(
                    "Primary is now {} (epoch {})",
                    choice.target.id, choice.epoch
                ),
                None => warn!("Primary is now none"),
            }
        }
    }

    async fn poll_all(&self) -> Vec<(NodeTarget, Option<NodeStatus>)> {
        let mut polls = JoinSet::new();
        for (i, node) in self.config.nodes.iter().enumerate() {
            let (http, node, timeout) = (
                self.http.clone(),
                node.clone(),
                self.config.health_timeout(),
            );
            polls.spawn(async move {
                let status = poll(&http, &node, timeout).await;
                (i, node, status)
            });
        }
        let mut polled: Vec<(usize, NodeTarget, Option<NodeStatus>)> = polls.join_all().await;
        polled.sort_by_key(|(i, _, _)| *i);
        polled
            .into_iter()
            .map(|(_, node, status)| (node, status))
            .collect()
    }
}

/// The node's status, or `None` if it is unreachable, slow, or answers anything but a JSON status.
/// A node answering nonsense is a node to skip, not a reason to stop polling.
async fn poll(
    http: &reqwest::Client,
    node: &NodeTarget,
    timeout: std::time::Duration,
) -> Option<NodeStatus> {
    let url = node.url.join("health").ok()?;
    let response = http.get(url).timeout(timeout).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.bytes().await.ok()?;
    serde_json::from_slice(&body).ok()
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;

    fn target(id: &str) -> NodeTarget {
        NodeTarget {
            id: id.into(),
            url: Url::parse(&format!("http://{id}/")).unwrap(),
        }
    }

    fn status(role: &str, epoch: i64) -> Option<NodeStatus> {
        Some(NodeStatus {
            role: Some(role.into()),
            epoch,
        })
    }

    #[test]
    fn choose_picks_the_node_that_says_primary() {
        let choice = PrimaryTracker::choose(&[
            (target("a"), status("standby", 3)),
            (target("b"), status("primary", 3)),
        ]);

        assert_eq!(
            choice,
            Some(PrimaryChoice {
                target: target("b"),
                epoch: 3
            })
        );
    }

    #[test]
    fn choose_prefers_the_newer_epoch_when_both_claim_it() {
        let newer = PrimaryTracker::choose(&[
            (target("a"), status("primary", 4)),
            (target("b"), status("primary", 5)),
        ]);
        let tie = PrimaryTracker::choose(&[
            (target("a"), status("primary", 5)),
            (target("b"), status("primary", 5)),
        ]);

        assert_eq!(newer.map(|c| c.target.id), Some("b".into()));
        assert_eq!(tie.map(|c| c.target.id), Some("a".into()));
    }

    #[test]
    fn choose_returns_nothing_without_a_primary() {
        assert_eq!(
            PrimaryTracker::choose(&[(target("a"), None), (target("b"), status("standby", 2))]),
            None
        );
        assert_eq!(PrimaryTracker::choose(&[]), None);
    }

    #[test]
    fn status_parsing_is_strict_about_numbers() {
        assert_eq!(
            serde_json::from_str::<NodeStatus>(r#"{"role":"primary","epoch":3,"extra":true}"#).ok(),
            status("primary", 3)
        );
        assert!(serde_json::from_str::<NodeStatus>(r#"{"role":"primary","epoch":"3"}"#).is_err());
    }
}
