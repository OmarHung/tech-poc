use async_trait::async_trait;
use axum::http::StatusCode;
use shared::error::ErrorBody;
use shared::wire::{LeaseReply, LeaseRequest, SyncReply, SyncRequest};
use tracing::warn;
use url::Url;

use super::primary_term::Arbiter;
use super::rpc::RpcClient;

/// Where the lease comes from. The election loop only needs this, so tests can stand in for the witness.
#[async_trait]
pub trait LeaseSource: Send + Sync {
    /// Acquires or renews the lease. `None` if the witness could not be reached.
    async fn acquire(&self) -> Option<LeaseReply>;
}

/// The witness's HTTP API, as seen from one node.
pub struct WitnessClient {
    rpc: RpcClient,
    node_id: String,
    lease_url: Url,
    sync_url: Url,
}

impl WitnessClient {
    pub fn new(rpc: RpcClient, node_id: &str, witness_url: &Url) -> Result<Self, url::ParseError> {
        Ok(Self {
            rpc,
            node_id: node_id.to_owned(),
            lease_url: witness_url.join("lease")?,
            sync_url: witness_url.join("sync")?,
        })
    }
}

#[async_trait]
impl LeaseSource for WitnessClient {
    async fn acquire(&self) -> Option<LeaseReply> {
        let request = LeaseRequest {
            node: Some(self.node_id.clone()),
        };
        let response = self.rpc.post(&self.lease_url, &request).await?;
        if !response.status.is_success() {
            return None;
        }
        response.json()
    }
}

#[async_trait]
impl Arbiter for WitnessClient {
    async fn set_standby_in_sync(
        &self,
        epoch: i64,
        in_sync: bool,
        expected_version: i64,
    ) -> Option<SyncReply> {
        let request = SyncRequest {
            node: Some(self.node_id.clone()),
            epoch,
            in_sync,
            expected_version,
        };
        let response = self.rpc.post(&self.sync_url, &request).await?;
        if response.status == StatusCode::CONFLICT {
            let reason = response
                .json::<ErrorBody>()
                .map(|body| body.error)
                .unwrap_or_default();
            warn!("Witness refused standby in sync = {in_sync} for epoch {epoch}: {reason}");
            return None;
        }
        if !response.status.is_success() {
            return None;
        }
        response.json()
    }
}

/// No witness: nobody to ask, so the primary may always go on alone. This is what lets both sides keep writing.
pub struct NoArbiter;

#[async_trait]
impl Arbiter for NoArbiter {
    async fn set_standby_in_sync(
        &self,
        epoch: i64,
        in_sync: bool,
        expected_version: i64,
    ) -> Option<SyncReply> {
        Some(SyncReply {
            epoch,
            standby_in_sync: in_sync,
            sync_version: expected_version + 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::json;

    use super::*;

    type Seen = Arc<Mutex<Vec<serde_json::Value>>>;

    async fn fake_witness(sync_status: StatusCode) -> (Url, Seen) {
        let seen: Seen = Arc::default();
        let app = Router::new()
            .route(
                "/lease",
                post(|State(seen): State<Seen>, Json(body): Json<serde_json::Value>| async move {
                    seen.lock().unwrap().push(body);
                    Json(json!({"granted": true, "holder": "a", "epoch": 3, "ttlMs": 3000, "standbyInSync": true, "syncVersion": 9, "reason": null}))
                }),
            )
            .route(
                "/sync",
                post(move |State(seen): State<Seen>, Json(body): Json<serde_json::Value>| async move {
                    seen.lock().unwrap().push(body);
                    let reply = if sync_status == StatusCode::OK {
                        Json(json!({"epoch": 3, "standbyInSync": false, "syncVersion": 10}))
                    } else {
                        Json(json!({"error": "not the lease holder"}))
                    };
                    (sync_status, reply).into_response()
                }),
            )
            .with_state(Arc::clone(&seen));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        (Url::parse(&format!("http://{address}")).unwrap(), seen)
    }

    fn client(url: &Url) -> WitnessClient {
        WitnessClient::new(RpcClient::new(Duration::from_secs(2)).unwrap(), "a", url).unwrap()
    }

    #[tokio::test]
    async fn acquires_the_lease_and_reports_sync_state() {
        let (url, seen) = fake_witness(StatusCode::OK).await;
        let witness = client(&url);

        let lease = witness.acquire().await.unwrap();
        let sync = witness.set_standby_in_sync(3, false, 9).await.unwrap();

        assert_eq!(
            (lease.granted, lease.epoch, lease.sync_version),
            (true, 3, 9)
        );
        assert_eq!(
            sync,
            SyncReply {
                epoch: 3,
                standby_in_sync: false,
                sync_version: 10
            }
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                json!({"node": "a"}),
                json!({"node": "a", "epoch": 3, "inSync": false, "expectedVersion": 9})
            ]
        );
    }

    #[tokio::test]
    async fn refused_or_failed_reports_are_none() {
        for status in [StatusCode::CONFLICT, StatusCode::INTERNAL_SERVER_ERROR] {
            let (url, _) = fake_witness(status).await;
            assert_eq!(
                client(&url).set_standby_in_sync(3, true, 9).await,
                None,
                "{status}"
            );
        }
    }

    #[tokio::test]
    async fn no_arbiter_accepts_everything() {
        assert_eq!(
            NoArbiter.set_standby_in_sync(2, true, 4).await,
            Some(SyncReply {
                epoch: 2,
                standby_in_sync: true,
                sync_version: 5
            })
        );
    }
}
