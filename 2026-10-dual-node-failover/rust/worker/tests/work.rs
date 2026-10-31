//! Both architectures end to end in one process: an upstream, a sink and two workers running their real
//! loops on short timings, each worker reaching the upstream and the sink over loopback through TCP
//! links the test can cut. verify/independent.py covers the same ground with real processes.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shared::work::NewEvent;
use sink::result_store::ResultStore;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{AbortHandle, JoinHandle};
use upstream::Upstream;
use upstream::config::UpstreamConfig;
use url::Url;
use worker::config::{WorkerConfig, WorkerMode};
use worker::hosting::Worker;
use worker::hothot::ledger::Ledger;
use worker::status::WorkerStatus;

const TASK_LEASE_TTL: Duration = Duration::from_millis(400);
const WORKERS: [&str; 2] = ["a", "b"];

/// A TCP link from a worker to the upstream or the sink. Blocking it drops open connections and refuses new ones.
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

    fn url(&self) -> Url {
        Url::parse(&format!("http://127.0.0.1:{}", self.port)).unwrap()
    }
}

struct Running {
    status: Arc<WorkerStatus>,
    work: JoinHandle<()>,
}

struct Work {
    dir: TempDir,
    mode: WorkerMode,
    work_time: Duration,
    upstream: Upstream,
    results: Arc<ResultStore>,
    /// (worker, "upstream" | "sink") -> link
    links: HashMap<(&'static str, &'static str), Link>,
    workers: Mutex<HashMap<&'static str, Running>>,
    background: Vec<JoinHandle<()>>,
}

impl Work {
    /// Upstream, sink and the links, but no workers yet.
    async fn new(mode: WorkerMode, work_ms: u64) -> Self {
        let upstream = Upstream::new(UpstreamConfig {
            task_lease_ttl: TASK_LEASE_TTL,
            reap_interval: Duration::from_millis(20),
            ..UpstreamConfig::default()
        });
        let results = Arc::new(ResultStore::new());
        let upstream_listener = shared::http::bind(0).await.unwrap();
        let sink_listener = shared::http::bind(0).await.unwrap();
        let upstream_port = upstream_listener.local_addr().unwrap().port();
        let sink_port = sink_listener.local_addr().unwrap().port();
        let background = vec![
            upstream.spawn_reaper(),
            tokio::spawn(serve(upstream_listener, upstream.router())),
            tokio::spawn(serve(sink_listener, sink::router(Arc::clone(&results)))),
        ];
        let mut links = HashMap::new();
        for id in WORKERS {
            links.insert((id, "upstream"), Link::start(upstream_port).await);
            links.insert((id, "sink"), Link::start(sink_port).await);
        }
        Self {
            dir: tempfile::tempdir().unwrap(),
            mode,
            work_time: Duration::from_millis(work_ms),
            upstream,
            results,
            links,
            workers: Mutex::default(),
            background,
        }
    }

    /// Upstream, sink and both workers running.
    async fn start(mode: WorkerMode, work_ms: u64) -> Self {
        let work = Self::new(mode, work_ms).await;
        WORKERS.into_iter().for_each(|id| work.start_worker(id));
        work
    }

    fn data_dir(&self, id: &str) -> std::path::PathBuf {
        self.dir.path().join(id)
    }

    /// Starts (or restarts) a worker with its links healed, from whatever checkpoint it has on disk.
    fn start_worker(&self, id: &'static str) {
        self.links[&(id, "upstream")].heal();
        self.links[&(id, "sink")].heal();
        let config = WorkerConfig {
            worker_id: id.into(),
            mode: self.mode,
            upstream_url: self.links[&(id, "upstream")].url(),
            sink_url: self.links[&(id, "sink")].url(),
            data_dir: self.data_dir(id),
            work_time: self.work_time,
            poll_wait: Duration::from_millis(100),
            rpc_timeout: Duration::from_millis(500),
            ..WorkerConfig::default()
        };
        let worker = Worker::open(config).unwrap();
        let running = Running {
            status: Arc::clone(worker.status()),
            work: worker.spawn_loop(),
        };
        self.workers.lock().unwrap().insert(id, running);
    }

    fn status(&self, id: &str) -> Arc<WorkerStatus> {
        Arc::clone(&self.workers.lock().unwrap()[id].status)
    }

    /// Stops the worker and cuts its links, like kill -9.
    fn kill(&self, id: &str) {
        if let Some(running) = self.workers.lock().unwrap().remove(id) {
            running.work.abort();
        }
        self.cut(id, "upstream");
        self.cut(id, "sink");
    }

    /// Cuts the link from `worker` to `to` ("upstream" or "sink") only.
    fn cut(&self, worker: &str, to: &str) {
        self.links
            .iter()
            .filter(|((from, target), _)| *from == worker && *target == to)
            .for_each(|(_, link)| link.block());
    }

    fn heal(&self, worker: &str, to: &str) {
        self.links
            .iter()
            .filter(|((from, target), _)| *from == worker && *target == to)
            .for_each(|(_, link)| link.heal());
    }

    fn append(&self, expected: &mut BTreeMap<String, i64>, from: i64, to: i64) {
        for i in from..=to {
            let account = format!("acct-{}", i % 3);
            let event = NewEvent {
                account: Some(account.clone()),
                amount: i,
            };
            self.upstream.events().append(&event).unwrap();
            *expected.entry(account).or_default() += i;
        }
    }

    fn holder(&self) -> String {
        self.upstream.tasks().stats().leases[0].worker.clone()
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        for running in self.workers.lock().unwrap().values() {
            running.work.abort();
        }
        self.background.iter().for_each(JoinHandle::abort);
    }
}

async fn serve(listener: TcpListener, router: axum::Router) {
    let _ = shared::http::serve(listener, router).await;
}

async fn wait_until<F, Fut>(what: &str, condition: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..500 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_hot_both_workers_process_every_event_and_losing_one_loses_nothing() {
    let work = Work::start(WorkerMode::HotHot, 0).await;
    let mut expected = BTreeMap::new();
    work.append(&mut expected, 1, 30);
    wait_until("both workers to deliver", || async {
        work.results.summary().received == 60
    })
    .await;

    work.kill("a");
    work.append(&mut expected, 31, 60);
    wait_until("b to deliver the rest", || async {
        work.results.summary().applied == 60
    })
    .await;
    wait_until("b to save its checkpoint", || async {
        work.status("b").ledger().offset == 60
    })
    .await;

    assert_eq!(work.results.summary().conflicts, 0);
    assert_eq!(work.status("b").ledger().balances, expected);
    let saved = std::fs::read_to_string(work.data_dir("b").join("checkpoint.json")).unwrap();
    assert_eq!(
        serde_json::from_str::<Ledger>(&saved).unwrap(),
        work.status("b").ledger()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_hot_a_restarted_worker_replays_from_its_checkpoint_and_agrees() {
    let work = Work::start(WorkerMode::HotHot, 0).await;
    let mut expected = BTreeMap::new();
    work.append(&mut expected, 1, 20);
    wait_until("a to checkpoint", || async {
        work.status("a").ledger().offset == 20
    })
    .await;

    work.kill("a");
    work.append(&mut expected, 21, 50);
    work.start_worker("a");
    wait_until("a to catch up", || async {
        work.status("a").ledger().offset == 50
    })
    .await;

    assert_eq!(work.status("a").ledger().balances, expected);
    let summary = work.results.summary();
    assert_eq!((summary.applied, summary.conflicts), (50, 0));
    let replies = work.status("a").health().sink_replies;
    assert!(replies.values().sum::<i64>() >= 30, "{replies:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_hot_a_worker_ahead_of_the_upstream_refuses_to_process() {
    let work = Work::new(WorkerMode::HotHot, 0).await;
    let ahead = Ledger {
        offset: 10,
        balances: BTreeMap::from([("acct-1".into(), 5)]),
    };
    shared::state_file::save(&work.data_dir("a").join("checkpoint.json"), &ahead).unwrap();
    work.start_worker("a");
    work.append(&mut BTreeMap::new(), 1, 3);

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(work.results.summary().received, 0);
    assert_eq!(work.status("a").ledger(), ahead);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_a_task_held_by_a_dead_worker_is_done_by_the_other() {
    let work = Work::start(WorkerMode::Dispatch, 150).await;
    work.upstream.tasks().add(7).unwrap();
    wait_until("a worker to take the task", || async {
        work.upstream.tasks().stats().leases.len() == 1
    })
    .await;
    let holder = work.holder();

    work.kill(&holder);
    wait_until("the task to be redone", || async {
        work.upstream.tasks().stats().done == 1
    })
    .await;

    let result = work.results.snapshot().results["task-1"].clone();
    assert_eq!(work.upstream.tasks().stats().reassignments, 1);
    assert_ne!(result.node, holder);
    assert_eq!((result.value.as_str(), result.attempt), ("7^2=49", 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_a_task_longer_than_the_lease_is_kept_alive_by_heartbeats() {
    let work = Work::start(WorkerMode::Dispatch, 1_000).await; // the lease is 400 ms
    work.upstream.tasks().add(3).unwrap();

    wait_until("the long task", || async {
        work.upstream.tasks().stats().done == 1
    })
    .await;

    assert_eq!(work.upstream.tasks().stats().reassignments, 0);
    assert_eq!(work.results.summary().received, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_a_worker_that_cannot_reach_the_sink_lets_the_task_go() {
    let work = Work::start(WorkerMode::Dispatch, 150).await;
    work.upstream.tasks().add(5).unwrap();
    wait_until("a worker to take the task", || async {
        work.upstream.tasks().stats().leases.len() == 1
    })
    .await;
    let holder = work.holder();

    work.cut(&holder, "sink");
    wait_until("the other worker to finish it", || async {
        work.upstream.tasks().stats().done == 1
    })
    .await;

    // A single task and no backlog: without the circuit breaker the cut-off worker could lease the
    // same task straight back and stall it again.
    assert_ne!(work.results.snapshot().results["task-1"].node, holder);
    assert_eq!(work.upstream.tasks().stats().reassignments, 1);
    assert_eq!(work.status(&holder).health().abandoned, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_a_worker_that_gave_up_for_the_sink_waits_until_it_is_back() {
    let work = Work::start(WorkerMode::Dispatch, 150).await;
    work.upstream.tasks().add(5).unwrap();
    wait_until("a worker to take the task", || async {
        work.upstream.tasks().stats().leases.len() == 1
    })
    .await;
    let holder = work.holder();
    work.cut(&holder, "sink");
    wait_until("the holder to give the task up", || async {
        work.status(&holder).health().abandoned == 1
    })
    .await;

    for i in 2..=5 {
        work.upstream.tasks().add(i).unwrap();
    }
    wait_until("the other worker to do all five", || async {
        work.upstream.tasks().stats().done == 5
    })
    .await;
    let while_cut = work.status(&holder).health();
    work.heal(&holder, "sink");
    for i in 6..=15 {
        work.upstream.tasks().add(i).unwrap();
    }
    wait_until("the holder to take tasks again", || async {
        work.status(&holder).health().completed >= 1
    })
    .await;

    // While the sink was cut off the holder leased nothing: no second task stalled.
    assert_eq!((while_cut.abandoned, while_cut.completed), (1, 0));
    assert_eq!(work.upstream.tasks().stats().reassignments, 1);
    let snapshot = work.results.snapshot();
    assert!((1..=5).all(|i| snapshot.results[&format!("task-{i}")].node != holder));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_a_lost_completion_report_means_the_task_runs_twice() {
    let work = Work::start(WorkerMode::Dispatch, 150).await;
    work.upstream.tasks().add(4).unwrap();
    wait_until("a worker to take the task", || async {
        work.upstream.tasks().stats().leases.len() == 1
    })
    .await;
    let holder = work.holder();

    work.cut(&holder, "upstream");
    wait_until("the other worker to finish it", || async {
        work.upstream.tasks().stats().done == 1
    })
    .await;
    work.heal(&holder, "upstream");
    wait_until("the first worker's late report", || async {
        work.upstream.tasks().stats().stale_completions == 1
    })
    .await;

    let summary = work.results.summary();
    assert_eq!(
        (summary.received, summary.applied, summary.conflicts),
        (2, 1, 0)
    );
    assert_eq!(work.status(&holder).health().stale_completions, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_spreads_tasks_over_both_workers() {
    let work = Work::start(WorkerMode::Dispatch, 150).await;
    for i in 1..=6 {
        work.upstream.tasks().add(i).unwrap();
    }

    wait_until("all tasks", || async {
        work.upstream.tasks().stats().done == 6
    })
    .await;

    let snapshot = work.results.snapshot();
    let mut nodes: Vec<&str> = snapshot.results.values().map(|r| r.node.as_str()).collect();
    nodes.sort_unstable();
    nodes.dedup();
    assert_eq!(nodes, ["a", "b"]);
    assert_eq!(snapshot.summary.received, 6);
}
