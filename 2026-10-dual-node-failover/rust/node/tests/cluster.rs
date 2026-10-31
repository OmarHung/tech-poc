//! Whole-cluster behaviour in one process: a witness and two nodes with the real election and
//! replication loops on a short lease, talking over loopback through TCP links the test can cut.
//! verify/scenario.py covers the same ground with real processes.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use node::config::NodeConfig;
use node::hosting::Node;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{AbortHandle, JoinHandle};
use url::Url;
use witness::config::WitnessConfig;
use witness::lease_table::LeaseTable;

const LEASE_TTL: Duration = Duration::from_millis(600);
const NODE_IDS: [&str; 2] = ["a", "b"];

/// A TCP link between two processes. Blocking it drops open connections and refuses new ones.
struct Link {
    port: u16,
    blocked: Arc<AtomicBool>,
    connections: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Link {
    async fn start(target: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let blocked = Arc::new(AtomicBool::new(false));
        let connections: Arc<Mutex<Vec<AbortHandle>>> = Arc::default();
        let (is_blocked, open) = (Arc::clone(&blocked), Arc::clone(&connections));
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                if is_blocked.load(Ordering::SeqCst) {
                    continue; // dropped: the caller sees the connection close
                }
                let pipe = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(("127.0.0.1", target)).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                open.lock().unwrap().push(pipe.abort_handle());
            }
        });
        Self {
            port,
            blocked,
            connections,
        }
    }

    fn block(&self) {
        self.blocked.store(true, Ordering::SeqCst);
        self.connections
            .lock()
            .unwrap()
            .drain(..)
            .for_each(|pipe| pipe.abort());
    }

    fn heal(&self) {
        self.blocked.store(false, Ordering::SeqCst);
    }
}

struct Process {
    port: u16,
    tasks: Vec<JoinHandle<()>>,
}

struct Cluster {
    _dir: TempDir,
    processes: Mutex<HashMap<&'static str, Process>>,
    /// (from, to) -> link; "w" is the witness.
    links: HashMap<(&'static str, &'static str), Link>,
    http: reqwest::Client,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let witness_listener = shared::http::bind(0).await.unwrap();
        let node_listeners = [
            shared::http::bind(0).await.unwrap(),
            shared::http::bind(0).await.unwrap(),
        ];
        let port_of = |listener: &TcpListener| listener.local_addr().unwrap().port();
        let witness_port = port_of(&witness_listener);
        let node_ports = [port_of(&node_listeners[0]), port_of(&node_listeners[1])];
        let mut links = HashMap::new();
        links.insert(("a", "b"), Link::start(node_ports[1]).await);
        links.insert(("b", "a"), Link::start(node_ports[0]).await);
        links.insert(("a", "w"), Link::start(witness_port).await);
        links.insert(("b", "w"), Link::start(witness_port).await);

        let mut processes = HashMap::new();
        let witness_config = WitnessConfig {
            data_dir: dir.path().join("witness"),
            lease_ttl: LEASE_TTL,
            ..WitnessConfig::default()
        };
        let table = Arc::new(LeaseTable::open(witness_config).unwrap());
        let server = tokio::spawn(serve(witness_listener, witness::router(table)));
        processes.insert(
            "witness",
            Process {
                port: witness_port,
                tasks: vec![server],
            },
        );

        for (i, (id, listener)) in NODE_IDS.into_iter().zip(node_listeners).enumerate() {
            let peer = other(id);
            let config = NodeConfig {
                node_id: id.into(),
                port: node_ports[i],
                peer_url: local_url(links[&(id, peer)].port),
                witness_url: local_url(links[&(id, "w")].port),
                data_dir: dir.path().join(id),
                lease_ttl: LEASE_TTL,
                renew_interval: LEASE_TTL / 6,
                replication_timeout: LEASE_TTL / 3,
                ..NodeConfig::default()
            };
            let node = Node::open(config).unwrap();
            let mut tasks = node.spawn_workers();
            tasks.push(tokio::spawn(serve(listener, node.router())));
            processes.insert(
                id,
                Process {
                    port: node_ports[i],
                    tasks,
                },
            );
        }
        Self {
            _dir: dir,
            processes: Mutex::new(processes),
            links,
            http: reqwest::Client::new(),
        }
    }

    fn port(&self, name: &str) -> u16 {
        self.processes.lock().unwrap()[name].port
    }

    fn is_up(&self, name: &str) -> bool {
        self.processes.lock().unwrap().contains_key(name)
    }

    /// Stops the process and cuts every link to and from it, like kill -9 plus a dead NIC.
    fn kill(&self, name: &str) {
        if let Some(process) = self.processes.lock().unwrap().remove(name) {
            process.tasks.iter().for_each(JoinHandle::abort);
        }
        let short = if name == "witness" { "w" } else { name };
        self.links
            .iter()
            .filter(|((from, to), _)| *from == short || *to == short)
            .for_each(|(_, link)| link.block());
    }

    /// Cuts every link between `node` and the rest of the cluster, both ways. Clients still reach it.
    fn isolate(&self, node: &str) {
        self.links
            .iter()
            .filter(|((from, to), _)| *from == node || *to == node)
            .for_each(|(_, link)| link.block());
    }

    fn heal(&self) {
        self.links.values().for_each(Link::heal);
    }

    async fn get(&self, node: &str, path: &str) -> Value {
        let url = format!("http://127.0.0.1:{}{path}", self.port(node));
        self.http
            .get(url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn health(&self, node: &str) -> Value {
        self.get(node, "/health").await
    }

    async fn data(&self, node: &str) -> Value {
        self.get(node, "/admin/data").await["data"].clone()
    }

    async fn put(&self, node: &str, key: &str) -> reqwest::StatusCode {
        let url = format!("http://127.0.0.1:{}/kv/{key}", self.port(node));
        self.http
            .put(url)
            .json(&json!({"value": key}))
            .send()
            .await
            .unwrap()
            .status()
    }

    /// Writes keys (value = key) and asserts each one was acknowledged.
    async fn write(&self, node: &str, prefix: &str, count: usize) -> Vec<String> {
        let mut keys = Vec::new();
        for i in 0..count {
            let key = format!("{prefix}-{i}");
            let status = self.put(node, &key).await;
            assert!(status.is_success(), "{key} on {node}: {status}");
            keys.push(key);
        }
        keys
    }

    /// Waits for a primary whose standby is in sync and returns (primary, standby).
    async fn wait_for_stable_primary(&self) -> (&'static str, &'static str) {
        for _ in 0..wait_steps() {
            for id in NODE_IDS.into_iter().filter(|id| self.is_up(id)) {
                let health = self.health(id).await;
                if health["role"] == "primary" && health["peer"] == "in-sync" {
                    return (id, other(id));
                }
            }
            tokio::time::sleep(STEP).await;
        }
        panic!("timed out waiting for a primary with an in-sync standby");
    }

    async fn wait_for_role(&self, node: &str, role: &str) {
        wait_until(&format!("{node} to be {role}"), || async {
            self.health(node).await["role"] == role
        })
        .await;
    }
}

const STEP: Duration = Duration::from_millis(20);

fn wait_steps() -> u32 {
    (LEASE_TTL * 10).as_millis() as u32 / STEP.as_millis() as u32
}

async fn wait_until<F, Fut>(what: &str, condition: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..wait_steps() {
        if condition().await {
            return;
        }
        tokio::time::sleep(STEP).await;
    }
    panic!("timed out waiting for {what}");
}

async fn serve(listener: TcpListener, router: axum::Router) {
    let _ = shared::http::serve(listener, router).await;
}

fn local_url(port: u16) -> Url {
    Url::parse(&format!("http://127.0.0.1:{port}")).unwrap()
}

fn other(node: &str) -> &'static str {
    if node == "a" { "b" } else { "a" }
}

fn holds(data: &Value, keys: &[String]) -> bool {
    keys.iter().all(|key| data[key] == json!(key))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_primary_replicates_every_acknowledged_write() {
    let cluster = Cluster::start().await;
    let (primary, standby) = cluster.wait_for_stable_primary().await;

    let keys = cluster.write(primary, "k", 20).await;

    assert_eq!(cluster.health(standby).await["role"], "standby");
    assert!(holds(&cluster.data(standby).await, &keys));
    assert_eq!(
        cluster.put(standby, "nope").await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_takes_over_when_the_primary_dies_with_every_acknowledged_write() {
    let cluster = Cluster::start().await;
    let (primary, standby) = cluster.wait_for_stable_primary().await;
    let old_epoch = cluster.health(primary).await["epoch"].as_i64().unwrap();
    let keys = cluster.write(primary, "before", 20).await;

    cluster.kill(primary);
    cluster.wait_for_role(standby, "primary").await;

    let health = cluster.health(standby).await;
    assert!(health["epoch"].as_i64().unwrap() > old_epoch, "{health}");
    wait_until("the new primary to go solo", || async {
        cluster.health(standby).await["peer"] == "solo"
    })
    .await;
    assert!(holds(&cluster.data(standby).await, &keys));
    assert_eq!(cluster.put(standby, "after").await, reqwest::StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_primary_stops_acknowledging_and_rejoins_as_standby() {
    let cluster = Cluster::start().await;
    let (primary, standby) = cluster.wait_for_stable_primary().await;
    let keys = cluster.write(primary, "before", 10).await;

    cluster.isolate(primary);
    let cut_off = cluster.put(primary, "cut-off").await;
    cluster.wait_for_role(standby, "primary").await;
    let accepted = cluster.write(standby, "after", 10).await;
    cluster.heal();
    cluster.wait_for_role(primary, "standby").await;
    wait_until("the old primary to catch up", || async {
        cluster.health(standby).await["peer"] == "in-sync"
    })
    .await;

    assert_eq!(cut_off, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let new_primary_data = cluster.data(standby).await;
    assert_eq!(new_primary_data, cluster.data(primary).await);
    assert!(holds(&new_primary_data, &keys) && holds(&new_primary_data, &accepted));
    assert!(new_primary_data.get("cut-off").is_none());
    assert!(
        cluster.health(primary).await["truncatedEntries"]
            .as_i64()
            .unwrap()
            >= 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_keeps_serving_while_the_witness_is_down() {
    let cluster = Cluster::start().await;
    let (primary, _) = cluster.wait_for_stable_primary().await;
    let epoch = cluster.health(primary).await["epoch"].clone();

    cluster.kill("witness");
    tokio::time::sleep(LEASE_TTL * 3).await;
    let keys = cluster.write(primary, "no-witness", 10).await;

    let health = cluster.health(primary).await;
    assert_eq!(
        (&health["role"], &health["epoch"]),
        (&json!("primary"), &epoch)
    );
    assert_eq!(keys.len(), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_goes_solo_when_the_standby_dies() {
    let cluster = Cluster::start().await;
    let (primary, standby) = cluster.wait_for_stable_primary().await;

    cluster.kill(standby);
    let keys = cluster.write(primary, "alone", 5).await;

    assert_eq!(cluster.health(primary).await["peer"], "solo");
    assert_eq!(keys.len(), 5);
}
