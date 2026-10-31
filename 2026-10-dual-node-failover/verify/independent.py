"""Scenarios for the two architectures where worker a and worker b never talk to each other.

hot-hot:  the upstream keeps an event log. Both workers read all of it, keep running balances per account and send
          one result per event to the sink. Nothing fails over: when one worker dies, the other is already doing
          the same work. The price is doing everything twice, and the two copies have to agree.
dispatch: the upstream leases each task to one worker. A worker that stops heartbeating loses the lease and the task
          goes to the other one. The price is at-least-once: some tasks run twice.

The sink applies the first result per key and counts every other copy, so duplicates and disagreements show up.
This script produces the events and tasks itself, so it knows the right answer for every key.
"""

from __future__ import annotations

import random
import shutil
import statistics
import tempfile
import threading
import time
from dataclasses import dataclass

from chaos import ChaosProxy
from harness import PORT_BASE, Checks, Client, ProcessGroup, Scenario, ScenarioError, get_json, wait_until

UPSTREAM_PORT = PORT_BASE + 20
WORKER_PORTS = {"a": PORT_BASE + 21, "b": PORT_BASE + 22}
SINK_PORT = PORT_BASE + 23
# Each worker reaches the upstream and the sink only through these proxies.
LINKS = {
    "a->up": (PORT_BASE + 31, UPSTREAM_PORT),
    "b->up": (PORT_BASE + 32, UPSTREAM_PORT),
    "a->sink": (PORT_BASE + 33, SINK_PORT),
    "b->sink": (PORT_BASE + 34, SINK_PORT),
}
PORTS = [UPSTREAM_PORT, SINK_PORT, *WORKER_PORTS.values(), *(port for port, _ in LINKS.values())]

TASK_LEASE_TTL_S = 2.0
WORK_MS = 300
EVENTS_PER_S = 200
TASKS_PER_S = 5  # two workers at 300 ms per task manage about 6.7 per second
LONG_WORK_MS = 3000  # longer than the 2 s lease
LONG_TASKS_PER_S = 0.5
ACCOUNTS = 10
SETTLE_TIMEOUT_S = 30.0
MAX_OUTPUT_GAP_MS = 1000


# --------------------------------------------------------------------------- cluster


class WorkCluster:
    """Upstream, sink and the two workers of one scenario, with the chaos proxy between workers and the rest."""

    def __init__(self, ctx: Checks) -> None:
        self._env = ctx.scenario.env
        self._group = ProcessGroup(ctx.impl, ctx.scenario.name)
        self.data_root = tempfile.mkdtemp(prefix=f"dnf-{ctx.scenario.name}-")
        self.proxy = ChaosProxy(LINKS)

    def __enter__(self) -> WorkCluster:
        self.proxy.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._group.stop_all()
        self.proxy.stop()
        shutil.rmtree(self.data_root, ignore_errors=True)

    def start_all(self) -> None:
        for name in ("upstream", "sink", "a", "b"):
            self.start(name)

    def start(self, name: str, **overrides: str) -> None:
        if name == "upstream":
            env = {"PORT": str(UPSTREAM_PORT), "TASK_LEASE_TTL_MS": str(int(TASK_LEASE_TTL_S * 1000))}
            self._group.start(name, "upstream", env, UPSTREAM_PORT)
            return
        if name == "sink":
            self._group.start(name, "sink", {"PORT": str(SINK_PORT)}, SINK_PORT)
            return
        env = {
            "WORK_MS": str(WORK_MS),
            **self._env,
            "PORT": str(WORKER_PORTS[name]),
            "WORKER_ID": name,
            "UPSTREAM_URL": f"http://127.0.0.1:{LINKS[f'{name}->up'][0]}",
            "SINK_URL": f"http://127.0.0.1:{LINKS[f'{name}->sink'][0]}",
            "DATA_DIR": f"{self.data_root}/{name}",
            **overrides,
        }
        self._group.start(name, "worker", env, WORKER_PORTS[name])

    def kill(self, name: str) -> None:
        self._group.kill(name)

    def pause(self, name: str) -> None:
        self._group.pause(name)

    def resume(self, name: str) -> None:
        self._group.resume(name)


def worker_health(name: str) -> dict:
    return get_json(WORKER_PORTS[name], "/health") or {}


def worker_offset(name: str) -> int:
    return int(worker_health(name).get("offset", -1))


def task_stats() -> dict:
    return get_json(UPSTREAM_PORT, "/tasks/stats") or {}


def sink_snapshot() -> tuple[dict, dict]:
    body = get_json(SINK_PORT, "/results", timeout=10.0)
    if body is None:
        raise ScenarioError("sink did not return /results")
    return body["summary"], body["results"]


def wait_for_task_on(name: str) -> int:
    """Waits until the worker is working on a task (not yet writing its result), so freezing or cutting it off
    catches the task before its result reaches the sink."""
    found: dict[str, int] = {}

    def busy() -> bool:
        health = worker_health(name)
        working = health.get("phase") == "working" and health.get("currentTask") is not None
        if working:
            found["task"] = int(health["currentTask"])
        return working

    wait_until(busy, 10.0, f"worker {name} to hold a task")
    return found["task"]


# --------------------------------------------------------------------------- load


@dataclass(frozen=True)
class Produced:
    key: str  # the sink key for its result
    expected: str  # the right result
    at_ms: int  # wall clock just before it was sent upstream; the sink stamps results with the same clock


class Producer:
    """One thread feeding the upstream at a steady rate and computing the right result for everything it sends."""

    def __init__(self, kind: str, rate: float) -> None:
        self._kind = kind
        self._interval = 1.0 / rate
        self._rng = random.Random(7)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._lock = threading.Lock()
        self.produced: list[Produced] = []
        self.balances: dict[str, int] = {}

    def start(self) -> Producer:
        self._thread.start()
        return self

    def stop(self) -> list[Produced]:
        self._stop.set()
        self._thread.join()
        return list(self.produced)

    def last_seq(self) -> int:
        with self._lock:
            return len(self.produced)

    def _run(self) -> None:
        client = Client(UPSTREAM_PORT)
        seq = 0
        while not self._stop.is_set():
            seq += 1
            item = self._event(client) if self._kind == "events" else self._task(client, seq)
            if item is not None:
                with self._lock:
                    self.produced.append(item)
            time.sleep(self._interval)
        client.close()

    def _event(self, client: Client) -> Produced | None:
        account = f"acct-{self._rng.randint(1, ACCOUNTS)}"
        amount = self._rng.randint(-50, 100)
        sent_ms = _now_ms()
        reply = client.request("POST", "/events", {"account": account, "amount": amount})
        if reply.status != 201:
            return None
        # One producer thread, so sequence numbers come back in the order the balances are computed here.
        self.balances[account] = self.balances.get(account, 0) + amount
        return Produced(f"event-{reply.body['seq']}", f"{account}:{self.balances[account]}", sent_ms)

    def _task(self, client: Client, n: int) -> Produced | None:
        sent_ms = _now_ms()
        reply = client.request("POST", "/tasks", {"input": n})
        if reply.status != 201:
            return None
        return Produced(f"task-{reply.body['id']}", f"{n}^2={n * n}", sent_ms)


def _now_ms() -> int:
    return int(time.time() * 1000)


# --------------------------------------------------------------------------- analysis


def settle(produced: list[Produced]) -> None:
    """Waits until every produced key has a result at the sink."""
    keys = {p.key for p in produced}

    def done() -> bool:
        _, results = sink_snapshot()
        return keys.issubset(results)

    try:
        wait_until(done, SETTLE_TIMEOUT_S, "every result to reach the sink")
    except ScenarioError:
        pass  # the checks below report what is missing


def analyse(ctx: Checks, produced: list[Produced]) -> tuple[dict, dict, list[Produced], list[Produced]]:
    if not produced:
        raise ScenarioError("the upstream accepted nothing; every check below would pass vacuously")
    summary, results = sink_snapshot()
    missing = [p for p in produced if p.key not in results]
    wrong = [p for p in produced if p.key in results and results[p.key]["value"] != p.expected]
    latencies = sorted(results[p.key]["firstAtMs"] - p.at_ms for p in produced if p.key in results)
    firsts = sorted(results[p.key]["firstAtMs"] for p in produced if p.key in results)
    ctx.metric("produced", len(produced))
    ctx.metric("applied", summary["applied"])
    ctx.metric("received", summary["received"])  # the side effects a sink without deduplication would cause
    ctx.metric("extra_copies", summary["received"] - summary["applied"])
    ctx.metric("conflicts", summary["conflicts"])
    ctx.metric("stale", summary["stale"])
    ctx.metric("missing", len(missing))
    ctx.metric("wrong", len(wrong))
    if latencies:
        ctx.metric("latency_p50_ms", int(statistics.median(latencies)))
        ctx.metric("latency_p99_ms", int(latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))]))
        ctx.metric("latency_max_ms", latencies[-1])
    if len(firsts) > 1:
        ctx.metric("longest_output_gap_ms", max(b - a for a, b in zip(firsts, firsts[1:])))
    return summary, results, missing, wrong


def check_complete_and_correct(ctx: Checks, missing: list[Produced], wrong: list[Produced]) -> None:
    ctx.check("every item has a result", not missing, [p.key for p in missing[:5]])
    ctx.check("every applied result is right", not wrong, [p.key for p in wrong[:5]])


# --------------------------------------------------------------------------- hot-hot


def scenario_hothot_crash(ctx: Checks) -> None:
    """a dies, comes back from its checkpoint and catches up; then b dies and a carries on alone."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("events", EVENTS_PER_S).start()
        time.sleep(3)
        cluster.kill("a")
        time.sleep(3)
        target = producer.last_seq()
        restart_at = time.monotonic()
        cluster.start("a")
        wait_until(lambda: worker_offset("a") >= target, 15.0, "a to catch up")
        catch_up = time.monotonic() - restart_at  # includes starting the process
        time.sleep(2)
        cluster.kill("b")
        time.sleep(3)
        produced = producer.stop()
        settle(produced)
        wait_until(lambda: worker_offset("a") >= len(produced), 10.0, "a to finish")

        summary, _, missing, wrong = analyse(ctx, produced)
        state = get_json(WORKER_PORTS["a"], "/state") or {}
        ctx.metric("catch_up_s", round(catch_up, 3))
        check_complete_and_correct(ctx, missing, wrong)
        check_hothot_survivor(ctx, summary, state, producer, len(produced))


def check_hothot_survivor(ctx: Checks, summary: dict, state: dict, producer: Producer, produced: int) -> None:
    ctx.check("the two workers never disagreed", summary["conflicts"] == 0, summary["conflicts"])
    gap = ctx.metrics.get("longest_output_gap_ms")
    ctx.check(f"output never paused for {MAX_OUTPUT_GAP_MS} ms", gap is not None and gap < MAX_OUTPUT_GAP_MS, gap)
    ctx.check(
        "a ends with the true balances",
        state.get("offset") == produced and state.get("balances") == producer.balances,
        state.get("offset"),
    )


def scenario_hothot_partition(ctx: Checks) -> None:
    """a is cut off from the upstream and the sink for 3 s, catches up after, then b dies."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("events", EVENTS_PER_S).start()
        time.sleep(3)
        cluster.proxy.block("a->up", "a->sink")
        time.sleep(3)
        target = producer.last_seq()
        healed_at = time.monotonic()
        cluster.proxy.unblock("a->up", "a->sink")
        wait_until(lambda: worker_offset("a") >= target, 15.0, "a to catch up")
        catch_up = time.monotonic() - healed_at
        time.sleep(2)
        cluster.kill("b")
        time.sleep(2)
        produced = producer.stop()
        settle(produced)
        wait_until(lambda: worker_offset("a") >= len(produced), 10.0, "a to finish")

        summary, _, missing, wrong = analyse(ctx, produced)
        state = get_json(WORKER_PORTS["a"], "/state") or {}
        ctx.metric("catch_up_s", round(catch_up, 3))
        check_complete_and_correct(ctx, missing, wrong)
        check_hothot_survivor(ctx, summary, state, producer, len(produced))


def scenario_hothot_skip(ctx: Checks) -> None:
    """Control group: a rejoins at the newest event instead of replaying what it missed, then b dies."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("events", EVENTS_PER_S).start()
        time.sleep(3)
        cluster.kill("a")
        time.sleep(3)
        cluster.start("a", START_FROM="latest")
        time.sleep(3)
        cluster.kill("b")
        time.sleep(3)
        produced = producer.stop()
        settle(produced)

        summary, _, missing, wrong = analyse(ctx, produced)
        ctx.check("every item still gets a result", not missing, [p.key for p in missing[:5]])
        # These prove the hazard is real, not that the setup is fine.
        ctx.check("a's results disagree with b's after it skipped events", summary["conflicts"] > 0, summary["conflicts"])
        ctx.check("once b is gone, wrong results are applied", len(wrong) > 0, len(wrong))


def scenario_hothot_nondeterministic(ctx: Checks) -> None:
    """Control group: each worker stamps its own clock into the result, so the two copies never match."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("events", EVENTS_PER_S).start()
        time.sleep(4)
        produced = producer.stop()
        settle(produced)

        summary, results, missing, _ = analyse(ctx, produced)
        del ctx.metrics["wrong"]  # every value carries a clock, so none can match the expected text
        kept = {name: sum(1 for r in results.values() if r["node"] == name) for name in WORKER_PORTS}
        ctx.metric("kept_from_a", kept["a"])
        ctx.metric("kept_from_b", kept["b"])
        ctx.check("every item has a result", not missing, len(missing))
        ctx.check("the two workers disagree", summary["conflicts"] > 0, summary["conflicts"])


# --------------------------------------------------------------------------- dispatch


def scenario_dispatch_crash(ctx: Checks) -> None:
    """a is killed in the middle of a task; the lease runs out and b does it. a comes back later."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("tasks", TASKS_PER_S).start()
        time.sleep(3)
        task = wait_for_task_on("a")
        cluster.kill("a")
        time.sleep(4)
        cluster.start("a")
        time.sleep(3)
        produced = producer.stop()
        settle(produced)

        _, results, missing, wrong = analyse(ctx, produced)
        stats = task_stats()
        held = results.get(f"task-{task}", {})
        held_at = next((p.at_ms for p in produced if p.key == f"task-{task}"), None)
        ctx.metric("reassignments", stats.get("reassignments"))
        ctx.metric("killed_task_latency_ms", held.get("firstAtMs", 0) - held_at if held and held_at else None)
        check_complete_and_correct(ctx, missing, wrong)
        ctx.check("the task a held was finished by b", held.get("node") == "b", held)
        ctx.check("results never disagreed", ctx.metrics["conflicts"] == 0, ctx.metrics["conflicts"])


def run_dispatch_pause(ctx: Checks, lease_check: bool) -> None:
    """a is frozen (SIGSTOP) for twice the lease while holding a task, then resumed."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("tasks", TASKS_PER_S).start()
        time.sleep(3)
        wait_for_task_on("a")
        cluster.pause("a")
        time.sleep(TASK_LEASE_TTL_S * 2)
        cluster.resume("a")
        time.sleep(3)
        produced = producer.stop()
        settle(produced)

        summary, _, missing, wrong = analyse(ctx, produced)
        stats = task_stats()
        a = worker_health("a")
        ctx.metric("reassignments", stats.get("reassignments"))
        ctx.metric("stale_completions", stats.get("staleCompletions"))
        ctx.metric("abandoned_by_a", a.get("abandoned"))
        check_complete_and_correct(ctx, missing, wrong)
        if lease_check:
            ctx.check("frozen a wrote nothing for the task it lost", summary["received"] == summary["applied"], summary)
            ctx.check("a noticed and gave the task up", (a.get("abandoned") or 0) >= 1, a)
        else:
            # The control group: these prove the frozen worker really does write late.
            ctx.check("frozen a wrote a result for a task it no longer held", summary["received"] > summary["applied"], summary)
            ctx.check("the upstream saw a stale completion", (stats.get("staleCompletions") or 0) >= 1, stats)


def scenario_dispatch_pause(ctx: Checks) -> None:
    run_dispatch_pause(ctx, lease_check=True)


def scenario_dispatch_pause_naive(ctx: Checks) -> None:
    run_dispatch_pause(ctx, lease_check=False)


def scenario_dispatch_sink_cut(ctx: Checks) -> None:
    """a cannot reach the sink while holding a task, but its heartbeats still reach the upstream."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("tasks", TASKS_PER_S).start()
        time.sleep(3)
        task = wait_for_task_on("a")
        cluster.proxy.block("a->sink")
        time.sleep(TASK_LEASE_TTL_S * 2 + 1)
        cluster.proxy.unblock("a->sink")
        time.sleep(3)
        produced = producer.stop()
        settle(produced)

        _, results, missing, wrong = analyse(ctx, produced)
        stuck = results.get(f"task-{task}", {})
        stuck_at = next((p.at_ms for p in produced if p.key == f"task-{task}"), None)
        ctx.metric("reassignments", task_stats().get("reassignments"))
        ctx.metric("cut_off_task_latency_ms", stuck.get("firstAtMs", 0) - stuck_at if stuck and stuck_at else None)
        ctx.metric("abandoned_by_a", worker_health("a").get("abandoned"))
        check_complete_and_correct(ctx, missing, wrong)
        # Heartbeats prove a is alive, not that it can finish: it must let go, or the task waits for the cut to end.
        ctx.check("a let the task go and b finished it", stuck.get("node") == "b", stuck)
        ctx.check("results never disagreed", ctx.metrics["conflicts"] == 0, ctx.metrics["conflicts"])


def scenario_dispatch_long(ctx: Checks) -> None:
    """Tasks take longer than the lease, so only heartbeats keep them from being handed out again."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("tasks", LONG_TASKS_PER_S).start()
        time.sleep(8)
        produced = producer.stop()
        settle(produced)

        summary, _, missing, wrong = analyse(ctx, produced)
        stats = task_stats()
        ctx.metric("reassignments", stats.get("reassignments"))
        check_complete_and_correct(ctx, missing, wrong)
        ctx.check("no task was handed out twice", stats.get("reassignments") == 0, stats.get("reassignments"))
        ctx.check("every task ran once", summary["received"] == summary["applied"], summary)


def scenario_dispatch_ack_lost(ctx: Checks) -> None:
    """a finishes a task and writes the result, but cannot tell the upstream; the task runs again on b."""
    with WorkCluster(ctx) as cluster:
        cluster.start_all()
        producer = Producer("tasks", TASKS_PER_S).start()
        time.sleep(3)
        wait_for_task_on("a")
        cluster.proxy.block("a->up")
        time.sleep(TASK_LEASE_TTL_S * 2 + 1)
        cluster.proxy.unblock("a->up")
        time.sleep(3)
        produced = producer.stop()
        settle(produced)

        summary, _, missing, wrong = analyse(ctx, produced)
        stats = task_stats()
        ctx.metric("reassignments", stats.get("reassignments"))
        ctx.metric("stale_completions", stats.get("staleCompletions"))
        check_complete_and_correct(ctx, missing, wrong)
        ctx.check("results never disagreed", summary["conflicts"] == 0, summary["conflicts"])
        # At-least-once made visible: the price the idempotent sink absorbs.
        ctx.check("the task was done twice and the sink kept one", summary["received"] > summary["applied"], summary)


HOTHOT = {"MODE": "hothot"}
DISPATCH = {"MODE": "dispatch"}

SCENARIOS = [
    Scenario("hothot-crash", "Hot-hot: a killed, restarts from its checkpoint; then b killed", scenario_hothot_crash,
             env=HOTHOT, family="independent"),
    Scenario("hothot-partition", "Hot-hot: a cut off from upstream and sink for 3 s; then b killed",
             scenario_hothot_partition, env=HOTHOT, family="independent"),
    Scenario("hothot-skip", "Hot-hot: a rejoins at the newest event instead of its checkpoint (control group)",
             scenario_hothot_skip, env=HOTHOT, family="independent"),
    Scenario("hothot-nondeterministic", "Hot-hot: results carry each worker's clock (control group)",
             scenario_hothot_nondeterministic, env={**HOTHOT, "DETERMINISTIC": "false"}, family="independent"),
    Scenario("dispatch-crash", "Dispatch: a killed while holding a task, restarted later", scenario_dispatch_crash,
             env=DISPATCH, family="independent"),
    Scenario("dispatch-pause", "Dispatch: a frozen past its lease, then resumed", scenario_dispatch_pause,
             env=DISPATCH, family="independent"),
    Scenario("dispatch-pause-naive", "Dispatch: same freeze, worker does not check its lease (control group)",
             scenario_dispatch_pause_naive, env={**DISPATCH, "LEASE_CHECK": "off"}, family="independent"),
    Scenario("dispatch-sink-cut", "Dispatch: a cannot reach the sink while holding a task", scenario_dispatch_sink_cut,
             env=DISPATCH, family="independent"),
    Scenario("dispatch-long", "Dispatch: tasks longer than the lease, kept alive by heartbeats", scenario_dispatch_long,
             env={**DISPATCH, "WORK_MS": str(LONG_WORK_MS)}, family="independent"),
    Scenario("dispatch-ack-lost", "Dispatch: a finishes a task but cannot report it", scenario_dispatch_ack_lost,
             env=DISPATCH, family="independent"),
]
