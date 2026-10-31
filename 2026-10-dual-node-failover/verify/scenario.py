#!/usr/bin/env python3
"""Black-box failover scenarios for the dual-node-failover PoC.

Two families of scenarios:
- replication (this file): primary-standby with a witness. Each scenario starts a fresh cluster (witness,
  node a, node b, gateway), keeps clients writing the whole time, injects one kind of failure, and then
  compares what the clients were told ("200 OK") with what the cluster ended up holding.
- independent (independent.py): two workers that never talk to each other, either both processing every
  event (hot-hot) or each taking tasks the upstream leases to it (dispatch).
Traffic between the processes goes through chaos.py, so partitions and jitter are real TCP behaviour, not
flags in the code. Standard library only, so any implementation can be checked with the same script.

Usage:
    scenario.py --impl dotnet                     # every scenario the implementation supports
    scenario.py --impl dotnet crash partition     # only these
    scenario.py --impl rust --family replication  # only one family
    scenario.py summary                           # markdown table of all saved results
    scenario.py compare                           # one column per implementation, for the README
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import independent
from chaos import ChaosProxy
from harness import (
    FAMILY_COMPONENTS,
    LOG_DIR,
    PORT_BASE,
    RESULTS_DIR,
    ROOT,
    START_TIMEOUT_S,
    Checks,
    Client,
    Clock,
    Scenario,
    ScenarioError,
    get_json,
    port_in_use,
    supported_families,
    wait_until,
)


GATEWAY_PORT = PORT_BASE
NODE_PORTS = {"a": PORT_BASE + 1, "b": PORT_BASE + 2}
WITNESS_PORT = PORT_BASE + 3
# link name -> (proxy port, target port). Each node reaches its peer and the witness only through these.
LINKS = {
    "a->b": (PORT_BASE + 11, NODE_PORTS["b"]),
    "b->a": (PORT_BASE + 12, NODE_PORTS["a"]),
    "a->w": (PORT_BASE + 13, WITNESS_PORT),
    "b->w": (PORT_BASE + 14, WITNESS_PORT),
}

REPLICATION_LINKS = ("a->b", "b->a")
LEASE_TTL_S = 3.0  # node and witness defaults; RTO is expected to be about this plus detection overhead
STABLE_TIMEOUT_S = 20.0
FAILOVER_TIMEOUT_S = 15.0
REJOIN_TIMEOUT_S = 30.0
WARMUP_S = 3.0
AFTER_FAILOVER_S = 5.0
WRITERS = 4
WRITE_PAUSE_S = 0.005
MONITOR_INTERVAL_S = 0.1
WITNESS_AGREE_TIMEOUT_S = 5.0
RTO_TARGET_S = 5.0


def node_health(node: str) -> dict | None:
    return get_json(NODE_PORTS[node], "/health")


def admin_data(node: str) -> dict[str, str]:
    body = get_json(NODE_PORTS[node], "/admin/data", timeout=10.0)
    if body is None:
        raise ScenarioError(f"node {node} did not return /admin/data")
    return body["data"]


def other(node: str) -> str:
    return "b" if node == "a" else "a"


def isolation_links(node: str) -> tuple[str, ...]:
    """Everything that connects `node` to the rest of the cluster (clients can still reach it)."""
    return (f"{node}->{other(node)}", f"{other(node)}->{node}", f"{node}->w")


# --------------------------------------------------------------------------- processes


class Cluster:
    """The four processes of one scenario, their data directories and the chaos proxy between them."""

    def __init__(self, impl: str, scenario: Scenario) -> None:
        self._run_sh = ROOT / impl / "run.sh"
        self._log_prefix = f"{impl}-{scenario.name}"
        self._env = scenario.env
        self._node_env = scenario.node_env
        self._link_delay_s = scenario.link_delay_ms / 1000
        self._procs: dict[str, subprocess.Popen] = {}
        self.data_root = Path(tempfile.mkdtemp(prefix=f"dnf-{scenario.name}-"))
        self.proxy = ChaosProxy(LINKS)

    def __enter__(self) -> Cluster:
        self.proxy.start()
        if self._link_delay_s:
            self.proxy.set_delay(REPLICATION_LINKS, self._link_delay_s, self._link_delay_s)
        return self

    def __exit__(self, *_: object) -> None:
        for name in list(self._procs):
            self.stop(name)
        self.proxy.stop()
        shutil.rmtree(self.data_root, ignore_errors=True)

    def start(self, name: str) -> None:
        component, env, port = self._spec(name)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_DIR / f"{self._log_prefix}-{name}.log", "ab") as log:
            self._procs[name] = subprocess.Popen(
                [str(self._run_sh), component],
                env={**os.environ, **env},
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        wait_until(lambda: get_json(port, "/health") is not None, START_TIMEOUT_S, f"{name} to start")

    def start_stable(self, with_witness: bool = True) -> tuple[str, str]:
        """Starts everything and waits for a primary whose standby is in sync. Returns (primary, standby)."""
        for name in (["witness"] if with_witness else []) + ["a", "b", "gateway"]:
            self.start(name)
        wait_until(lambda: (p := current_primary()) and p[1].get("peer") == "in-sync", STABLE_TIMEOUT_S, "a stable primary")
        primary = current_primary()[0]
        wait_until(lambda: gateway_primary() == primary, STABLE_TIMEOUT_S, "the gateway to find the primary")
        return primary, other(primary)

    def kill(self, name: str) -> None:
        """SIGKILL: no shutdown hooks, no flushing. The closest a process gets to pulling the plug."""
        proc = self._procs.pop(name)
        proc.send_signal(signal.SIGKILL)
        proc.wait()

    def stop(self, name: str) -> None:
        proc = self._procs.pop(name)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def _spec(self, name: str) -> tuple[str, dict[str, str], int]:
        if name == "witness":
            env = {"PORT": str(WITNESS_PORT), "DATA_DIR": str(self.data_root / "witness"), "NODES": "a,b"}
            if "LEASE_TTL_MS" in self._env:
                env["LEASE_TTL_MS"] = self._env["LEASE_TTL_MS"]
            return "witness", env, WITNESS_PORT
        if name == "gateway":
            urls = ",".join(f"{n}=http://127.0.0.1:{p}" for n, p in NODE_PORTS.items())
            return "gateway", {"PORT": str(GATEWAY_PORT), "NODE_URLS": urls}, GATEWAY_PORT
        env = {
            **self._env,
            "PORT": str(NODE_PORTS[name]),
            "NODE_ID": name,
            "PEER_URL": f"http://127.0.0.1:{LINKS[f'{name}->{other(name)}'][0]}",
            "WITNESS_URL": f"http://127.0.0.1:{LINKS[f'{name}->w'][0]}",
            "DATA_DIR": str(self.data_root / name),
            **self._node_env.get(name, {}),
        }
        return "node", env, NODE_PORTS[name]


def current_primary() -> tuple[str, dict] | None:
    """The node that says it is primary (the newest epoch if both do), with its health."""
    claims = [(n, h) for n in NODE_PORTS if (h := node_health(n)) and h.get("role") == "primary"]
    return max(claims, key=lambda c: c[1]["epoch"]) if claims else None


def witness_agrees(primary: str) -> bool:
    """The witness's own record: the lease is with `primary` and the other node may take over."""
    health = get_json(WITNESS_PORT, "/health")
    return bool(health) and health.get("holder") == primary and health.get("standbyInSync") is True


def gateway_primary() -> str | None:
    health = get_json(GATEWAY_PORT, "/health")
    return health.get("primary") if health else None


# --------------------------------------------------------------------------- load and observation


@dataclass(frozen=True)
class Write:
    key: str
    value: str
    via: str  # "gateway", "a" or "b"
    start: float  # seconds since the scenario started
    end: float
    status: int
    epoch: int | None


class WriteLoad:
    """Worker threads writing unique keys until stopped. Every key is written once, so the final data
    must contain every acknowledged write exactly as sent."""

    def __init__(self, clock: Clock, phase: str, targets: dict[str, int], workers: int) -> None:
        self._clock = clock
        self._phase = phase
        self._targets = targets
        self._workers = workers
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._results: list[list[Write]] = []

    def start(self) -> WriteLoad:
        for via, port in self._targets.items():
            for worker in range(self._workers):
                results: list[Write] = []
                self._results.append(results)
                thread = threading.Thread(target=self._run, args=(via, port, worker, results), daemon=True)
                self._threads.append(thread)
                thread.start()
        return self

    def stop(self) -> list[Write]:
        self._stop.set()
        for thread in self._threads:
            thread.join()
        return sorted((w for results in self._results for w in results), key=lambda w: w.start)

    def _run(self, via: str, port: int, worker: int, results: list[Write]) -> None:
        client = Client(port)
        seq = 0
        while not self._stop.is_set():
            key = f"{self._phase}-{via}-{worker}-{seq}"
            value = f"{worker}.{seq}"
            started = self._clock.now()
            reply = client.request("PUT", f"/kv/{key}", {"value": value})
            epoch = reply.body.get("epoch") if reply.status == 200 and isinstance(reply.body, dict) else None
            results.append(Write(key, value, via, started, self._clock.now(), reply.status, epoch))
            seq += 1
            time.sleep(WRITE_PAUSE_S)
        client.close()


@dataclass(frozen=True)
class Sample:
    at: float
    node: str
    role: str | None  # None when the node did not answer
    epoch: int | None


class RoleMonitor:
    """Samples both nodes' role every 100 ms, to catch a promotion or step-down that the end state hides."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.samples: list[Sample] = []

    def start(self) -> RoleMonitor:
        self._thread.start()
        return self

    def stop(self) -> list[Sample]:
        self._stop.set()
        self._thread.join()
        return self.samples

    def primary_samples(self, node: str, since: float = 0.0, until: float = float("inf")) -> int:
        return sum(1 for s in self.samples if s.node == node and s.role == "primary" and since <= s.at <= until)

    def standby_samples(self, node: str, since: float = 0.0, until: float = float("inf")) -> int:
        return sum(1 for s in self.samples if s.node == node and s.role == "standby" and since <= s.at <= until)

    def _run(self) -> None:
        while not self._stop.wait(MONITOR_INTERVAL_S):
            for node in NODE_PORTS:
                health = node_health(node)
                role = health.get("role") if health else None
                epoch = health.get("epoch") if health else None
                self.samples.append(Sample(self._clock.now(), node, role, epoch))


def acked(writes: list[Write]) -> list[Write]:
    return [w for w in writes if w.status == 200]


def lost(writes: list[Write], data: dict[str, str]) -> list[Write]:
    """Writes the client was told succeeded that the cluster no longer has."""
    return [w for w in acked(writes) if data.get(w.key) != w.value]


def acked_after_newer_epoch(writes: list[Write]) -> list[Write]:
    """Writes acknowledged in some epoch after a newer epoch had already acknowledged one: two primaries at once."""
    first_ack: dict[int, float] = {}
    for w in acked(writes):
        first_ack[w.epoch] = min(first_ack.get(w.epoch, w.end), w.end)
    return [w for w in acked(writes) if any(e > w.epoch and t < w.end for e, t in first_ack.items())]


def recovery_s(writes: list[Write], fault_at: float, old_epoch: int) -> float | None:
    """Fault to the first write acknowledged in a newer epoch, i.e. by the new primary."""
    ends = [w.end for w in acked(writes) if (w.epoch or 0) > old_epoch]
    return round(min(ends) - fault_at, 3) if ends else None


def longest_gap_s(writes: list[Write], since: float, until: float) -> float:
    """Longest stretch in [since, until] without any acknowledged write."""
    points = [since] + sorted(w.end for w in acked(writes) if since <= w.end <= until) + [until]
    return round(max(b - a for a, b in zip(points, points[1:])), 3)


def latency_ms(writes: list[Write], percentile: int) -> float | None:
    durations = [(w.end - w.start) * 1000 for w in acked(writes)]
    if len(durations) < 2:
        return round(durations[0], 2) if durations else None
    return round(statistics.quantiles(durations, n=100, method="inclusive")[percentile - 1], 2)


def failed(writes: list[Write], since: float = 0.0, until: float = float("inf")) -> int:
    return sum(1 for w in writes if w.status != 200 and since <= w.start <= until)


# --------------------------------------------------------------------------- scenario plumbing


class Context(Checks):
    def cluster(self) -> Cluster:
        return Cluster(self.impl, self.scenario)

    def load(self, phase: str, targets: dict[str, int], workers: int = WRITERS) -> WriteLoad:
        return WriteLoad(self.clock, phase, targets, workers).start()

    def monitor(self) -> RoleMonitor:
        return RoleMonitor(self.clock).start()

    def check_witness_agrees(self, primary: str) -> None:
        """The nodes' view is not enough: a lost reply can leave the witness believing the standby is behind."""
        try:
            wait_until(lambda: witness_agrees(primary), WITNESS_AGREE_TIMEOUT_S, "the witness to agree")
            agrees = True
        except ScenarioError:
            agrees = False
        self.check("witness would let the standby take over", agrees, get_json(WITNESS_PORT, "/health"))


# --------------------------------------------------------------------------- scenarios


def scenario_crash(ctx: Context) -> None:
    """Primary is killed under load; the standby takes over; the old primary comes back and later takes over again."""
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable()
        load = ctx.load("before", {"gateway": GATEWAY_PORT})
        time.sleep(WARMUP_S)
        old_epoch = node_health(p)["epoch"]
        fault_at = ctx.now()
        cluster.kill(p)
        wait_until(lambda: gateway_primary() == s, FAILOVER_TIMEOUT_S, f"{s} to take over")
        time.sleep(AFTER_FAILOVER_S)
        first = load.stop()

        rto = recovery_s(first, fault_at, old_epoch)
        lost_first = lost(first, admin_data(s))
        before_fault = [w for w in first if w.end <= fault_at]
        ctx.metric("acked_writes", len(acked(first)))
        ctx.metric("write_p50_ms", latency_ms(before_fault, 50))
        ctx.metric("write_p99_ms", latency_ms(before_fault, 99))
        ctx.metric("rto_s", rto)
        ctx.metric("longest_write_gap_s", longest_gap_s(first, fault_at - 1, ctx.now()))
        ctx.metric("lost_acked_writes", len(lost_first))
        ctx.check(f"{s} took over in a newer epoch", rto is not None, "no write acknowledged in a newer epoch")
        ctx.check(f"failover within {RTO_TARGET_S:.0f}s", rto is not None and rto <= RTO_TARGET_S, rto)
        ctx.check("no acknowledged write lost", not lost_first, [w.key for w in lost_first[:5]])

        rejoin_at = ctx.now()
        cluster.start(p)
        catch_up = wait_until(lambda: (node_health(s) or {}).get("peer") == "in-sync", REJOIN_TIMEOUT_S, f"{p} to catch up")
        ctx.metric("rejoin_catch_up_s", round(ctx.now() - rejoin_at, 3))
        ctx.metric("rejoin_catch_up_after_ready_s", round(catch_up, 3))
        ctx.metric("rejoin_discarded_entries", node_health(p)["truncatedEntries"])
        ctx.check(f"{p} came back as standby", node_health(p)["role"] == "standby", node_health(p))
        ctx.check(f"{p} holds exactly what {s} holds", admin_data(p) == admin_data(s))
        ctx.check_witness_agrees(s)

        load = ctx.load("after", {"gateway": GATEWAY_PORT})
        time.sleep(2)
        second_epoch = node_health(s)["epoch"]
        second_fault_at = ctx.now()
        cluster.kill(s)
        wait_until(lambda: gateway_primary() == p, FAILOVER_TIMEOUT_S, f"{p} to take over again")
        time.sleep(3)
        second = load.stop()
        lost_all = lost(first + second, admin_data(p))
        ctx.metric("second_rto_s", recovery_s(second, second_fault_at, second_epoch))
        ctx.metric("acked_writes_total", len(acked(first + second)))
        ctx.check(f"rejoined {p} took over again", recovery_s(second, second_fault_at, second_epoch) is not None)
        ctx.check("no acknowledged write lost across both failovers", not lost_all, [w.key for w in lost_all[:5]])


def scenario_crash_once(ctx: Context) -> None:
    """One crash under load, counting acknowledged writes the new primary does not have. Run in sync and async
    mode, on loopback and over a slower link, it shows what each replication mode costs."""
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable()
        load = ctx.load("before", {"gateway": GATEWAY_PORT})
        time.sleep(WARMUP_S)
        old_epoch = node_health(p)["epoch"]
        fault_at = ctx.now()
        cluster.kill(p)
        wait_until(lambda: gateway_primary() == s, FAILOVER_TIMEOUT_S, f"{s} to take over")
        time.sleep(AFTER_FAILOVER_S)
        writes = load.stop()

        rto = recovery_s(writes, fault_at, old_epoch)
        gone = lost(writes, admin_data(s))
        before_fault = [w for w in acked(writes) if w.end <= fault_at]
        ctx.metric("acked_writes", len(acked(writes)))
        ctx.metric("rto_s", rto)
        ctx.metric("lost_acked_writes", len(gone))
        ctx.metric("lost_window_ms", round((fault_at - min(w.end for w in gone)) * 1000, 1) if gone else 0)
        ctx.metric("write_p50_ms", latency_ms(before_fault, 50))
        ctx.metric("write_p99_ms", latency_ms(before_fault, 99))
        ctx.check(f"{s} took over in a newer epoch", rto is not None)

        cluster.start(p)
        wait_until(lambda: (node_health(s) or {}).get("peer") == "in-sync", REJOIN_TIMEOUT_S, f"{p} to catch up")
        ctx.metric("rejoin_discarded_entries", node_health(p)["truncatedEntries"])
        ctx.check(f"{p} rejoined and matches {s}", admin_data(p) == admin_data(s))
        ctx.check_witness_agrees(s)


def run_partition(ctx: Context, with_witness: bool) -> None:
    """The primary is cut off from the standby and the witness, but clients on its side can still reach it."""
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable(with_witness=with_witness)
        load = ctx.load("partition", {"gateway": GATEWAY_PORT, p: NODE_PORTS[p], s: NODE_PORTS[s]}, workers=2)
        time.sleep(WARMUP_S)
        old_epoch = node_health(p)["epoch"]
        fault_at = ctx.now()
        cluster.proxy.block(*isolation_links(p))
        wait_until(lambda: (node_health(s) or {}).get("role") == "primary", FAILOVER_TIMEOUT_S, f"{s} to be promoted")
        time.sleep(AFTER_FAILOVER_S)
        heal_at = ctx.now()
        cluster.proxy.heal_all()
        wait_until(
            lambda: (node_health(p) or {}).get("role") == "standby" and (node_health(s) or {}).get("peer") == "in-sync",
            REJOIN_TIMEOUT_S,
            "the partition to heal",
        )
        time.sleep(1)
        writes = load.stop()

        data_s, data_p = admin_data(s), admin_data(p)
        old_after_cut = [w for w in acked(writes) if w.epoch == old_epoch and w.start > fault_at]
        both_primary = acked_after_newer_epoch(writes)
        gone = lost(writes, data_s)
        ctx.metric("acked_writes", len(acked(writes)))
        ctx.metric("rto_s", recovery_s(writes, fault_at, old_epoch))
        ctx.metric("old_primary_acks_after_cut", len(old_after_cut))
        ctx.metric("acks_while_both_primary", len(both_primary))
        ctx.metric("lost_acked_writes", len(gone))
        ctx.metric("heal_to_converged_s", round(ctx.now() - heal_at - 1, 3))
        ctx.metric("rejoin_discarded_entries", node_health(p)["truncatedEntries"])
        ctx.check("both nodes hold the same data after healing", data_s == data_p)
        if with_witness:
            ctx.check("old primary acknowledged nothing after the cut", not old_after_cut, len(old_after_cut))
            ctx.check("never two primaries acknowledging writes", not both_primary, len(both_primary))
            ctx.check("no acknowledged write lost", not gone, [w.key for w in gone[:5]])
            ctx.check_witness_agrees(s)
        else:
            # The control group: these prove the hazard is real, not that the setup is fine.
            ctx.check("split brain: both sides acknowledged writes", bool(both_primary), len(both_primary))
            ctx.check("split brain: acknowledged writes were lost on healing", bool(gone), len(gone))


def scenario_partition(ctx: Context) -> None:
    run_partition(ctx, with_witness=True)


def scenario_partition_naive(ctx: Context) -> None:
    run_partition(ctx, with_witness=False)


def run_jitter(ctx: Context, expect_stable: bool) -> None:
    """The primary's network flaps: blackouts of 0.2-1.5 s and latency spikes, for 20 s."""
    rng = random.Random(42)  # same weather every run and for every implementation
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable()
        old_epoch = node_health(p)["epoch"]
        monitor = ctx.monitor()
        load = ctx.load("jitter", {"gateway": GATEWAY_PORT})
        time.sleep(2)
        links = isolation_links(p)
        storm_from = ctx.now()
        blackouts: list[float] = []
        slow_periods = 0
        while ctx.now() - storm_from < 20:
            if rng.random() < 0.5:
                duration = rng.uniform(0.2, 1.5)
                cluster.proxy.block(*links)
                time.sleep(duration)
                cluster.proxy.unblock(*links)
                blackouts.append(round(duration, 2))
            else:
                cluster.proxy.set_delay(links, 0.02, 0.2)
                time.sleep(rng.uniform(1.0, 3.0))
                cluster.proxy.set_delay(links, 0.0, 0.0)
                slow_periods += 1
            time.sleep(rng.uniform(1.0, 2.0))
        storm_until = ctx.now()
        wait_until(lambda: (q := current_primary()) and q[1].get("peer") == "in-sync", STABLE_TIMEOUT_S, "the cluster to settle")
        time.sleep(1)
        writes = load.stop()
        monitor.stop()

        final_primary, final = current_primary()
        gone = lost(writes, admin_data(final_primary))
        storm = [w for w in writes if storm_from <= w.start <= storm_until]
        failovers = max((x.epoch or 0) for x in monitor.samples) - old_epoch
        ctx.metric("blackouts", len(blackouts))
        ctx.metric("longest_blackout_s", max(blackouts, default=0))
        ctx.metric("slow_periods", slow_periods)
        ctx.metric("writes_during_storm", len(storm))
        ctx.metric("failed_writes_during_storm", failed(storm))
        ctx.metric("write_p99_ms_during_storm", latency_ms(storm, 99))
        ctx.metric("failovers", failovers)
        ctx.metric("lost_acked_writes", len(gone))
        ctx.metric("acks_while_both_primary", len(acked_after_newer_epoch(writes)))
        if expect_stable:
            ctx.check("no failover: same primary, same epoch", final_primary == p and final["epoch"] == old_epoch, final)
            ctx.check("standby never became primary", monitor.primary_samples(s) == 0)
        ctx.check("never two primaries acknowledging writes", not acked_after_newer_epoch(writes))
        ctx.check("no acknowledged write lost", not gone, [w.key for w in gone[:5]])
        ctx.check("both nodes hold the same data", admin_data(p) == admin_data(s))
        ctx.check_witness_agrees(final_primary)


def scenario_jitter(ctx: Context) -> None:
    run_jitter(ctx, expect_stable=True)


def scenario_jitter_fast(ctx: Context) -> None:
    run_jitter(ctx, expect_stable=False)


def scenario_witness_down(ctx: Context) -> None:
    """The witness dies and comes back. With the standby still answering, the primary should not notice."""
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable()
        old_epoch = node_health(p)["epoch"]
        monitor = ctx.monitor()
        load = ctx.load("witness-down", {"gateway": GATEWAY_PORT})
        time.sleep(2)
        down_at = ctx.now()
        cluster.kill("witness")
        time.sleep(6)
        back_at = ctx.now()
        cluster.start("witness")
        time.sleep(3)
        writes = load.stop()
        monitor.stop()

        final = node_health(p)
        gone = lost(writes, admin_data(p))
        ctx.metric("witness_down_s", round(back_at - down_at, 2))
        ctx.metric("acked_writes", len(acked(writes)))
        ctx.metric("failed_writes_after_witness_died", failed(writes, since=down_at))
        ctx.metric("longest_write_gap_s", longest_gap_s(writes, down_at, ctx.now()))
        ctx.check("writes kept succeeding without the witness", failed(writes, since=down_at) == 0, failed(writes, since=down_at))
        ctx.check("primary never stepped down", monitor.standby_samples(p, down_at) == 0)
        ctx.check("same primary, same epoch", final["role"] == "primary" and final["epoch"] == old_epoch, final)
        ctx.check("no acknowledged write lost", not gone, [w.key for w in gone[:5]])
        ctx.check_witness_agrees(p)


def scenario_stale_standby(ctx: Context) -> None:
    """Standby dies, primary carries on alone, then the primary dies too and only the stale standby comes back.
    It must refuse to take over: it lacks writes that clients were told succeeded."""
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable()
        monitor = ctx.monitor()
        load = ctx.load("stale", {"gateway": GATEWAY_PORT})
        time.sleep(2)
        standby_down_at = ctx.now()
        cluster.kill(s)
        time.sleep(4)
        primary_down_at = ctx.now()
        cluster.kill(p)
        cluster.start(s)
        time.sleep(8)
        primary_back_at = ctx.now()
        cluster.start(p)
        wait_until(lambda: gateway_primary() == p, FAILOVER_TIMEOUT_S, f"{p} to serve again")
        time.sleep(3)
        writes = load.stop()
        wait_until(lambda: (node_health(p) or {}).get("peer") == "in-sync", REJOIN_TIMEOUT_S, f"{s} to catch up")
        monitor.stop()

        solo = [w for w in acked(writes) if standby_down_at < w.start < primary_down_at]
        right_after = [w for w in solo if w.start < standby_down_at + 2]
        resumed = min((w.end for w in acked(writes) if w.start > primary_back_at), default=None)
        gone = lost(writes, admin_data(p))
        ctx.metric("acked_while_alone", len(solo))
        ctx.metric("stall_when_standby_died_ms", round(max((w.end - w.start) * 1000 for w in right_after), 1) if right_after else None)
        ctx.metric("acked_while_both_down", len(acked([w for w in writes if primary_down_at < w.start < primary_back_at])))
        ctx.metric("outage_s", round(resumed - primary_down_at, 3) if resumed else None)
        ctx.metric("lost_acked_writes", len(gone))
        ctx.check("primary kept accepting writes after the standby died", len(solo) > 0, len(solo))
        ctx.check(f"stale {s} never became primary", monitor.primary_samples(s) == 0, monitor.primary_samples(s))
        ctx.check("no acknowledged write lost", not gone, [w.key for w in gone[:5]])
        ctx.check(f"{s} caught up after {p} returned", admin_data(s) == admin_data(p))
        ctx.check_witness_agrees(p)


# Shorter lease: faster failover, but a blackout longer than the lease now looks like a dead primary.
FAST_LEASE = {"LEASE_TTL_MS": "1000", "RENEW_INTERVAL_MS": "200", "REPLICATION_TIMEOUT_MS": "400"}

def scenario_late_messages(ctx: Context) -> None:
    """The primary's links get slower than its timeouts, so requests it gave up on still arrive later and out of order.
    Meanwhile the standby dies and comes back, and finally the primary is killed. Whoever ends up primary must hold
    every acknowledged write, and two primaries must never acknowledge at once."""
    with ctx.cluster() as cluster:
        p, s = cluster.start_stable()
        load = ctx.load("late", {"gateway": GATEWAY_PORT})
        time.sleep(2)
        cluster.proxy.set_delay(isolation_links(p), 0.3, 0.9)  # calls time out after 0.5 s
        time.sleep(3)
        cluster.kill(s)
        time.sleep(3)
        cluster.start(s)
        time.sleep(4)
        old_epoch = max(h["epoch"] for n in NODE_PORTS if (h := node_health(n)))
        primary_down_at = ctx.now()
        cluster.kill(p)
        cluster.proxy.heal_all()
        try:
            wait_until(lambda: gateway_primary() == s, FAILOVER_TIMEOUT_S, f"{s} to take over")
            took_over = True
        except ScenarioError:
            took_over = False  # the witness judged s behind; bring p back instead
            cluster.start(p)
            wait_until(lambda: gateway_primary() is not None, FAILOVER_TIMEOUT_S, "a primary")
        time.sleep(2)
        writes = load.stop()

        final_primary, _ = current_primary()
        gone = lost(writes, admin_data(final_primary))
        ctx.metric("acked_writes", len(acked(writes)))
        ctx.metric("failed_writes", failed(writes))
        ctx.metric("standby_took_over", took_over)
        ctx.metric("rto_s", recovery_s(writes, primary_down_at, old_epoch) if took_over else None)
        ctx.metric("lost_acked_writes", len(gone))
        ctx.metric("acks_while_both_primary", len(acked_after_newer_epoch(writes)))
        ctx.check("never two primaries acknowledging writes", not acked_after_newer_epoch(writes))
        ctx.check("no acknowledged write lost", not gone, [w.key for w in gone[:5]])


SCENARIOS = [
    Scenario("crash", "Primary killed, then rejoins and takes over again (sync)", scenario_crash),
    Scenario("crash-async", "Primary killed, async replication", scenario_crash_once, env={"REPLICATION": "async"}),
    Scenario("crash-slow", "Primary killed, sync, 10 ms between the nodes", scenario_crash_once, link_delay_ms=10),
    Scenario(
        "crash-async-slow",
        "Primary killed, async, 10 ms between the nodes",
        scenario_crash_once,
        env={"REPLICATION": "async"},
        link_delay_ms=10,
    ),
    Scenario("partition", "Primary isolated from standby and witness", scenario_partition),
    Scenario(
        "partition-naive",
        "Same partition without a witness (control group)",
        scenario_partition_naive,
        env={"ARBITER": "none"},
        # keepalived-style priority: a wins the first election instead of both promoting at once
        node_env={"b": {"ELECTION_DELAY_MS": "1500"}},
    ),
    Scenario("crash-fast", "Primary killed, sync, 1 s lease", scenario_crash_once, env=FAST_LEASE),
    Scenario("jitter", "Primary's network flaps for 20 s (blackouts <= 1.5 s)", scenario_jitter),
    Scenario("jitter-fast", "Same flapping with a 1 s lease", scenario_jitter_fast, env=FAST_LEASE),
    Scenario("witness-down", "Witness killed for 6s, then restarted", scenario_witness_down),
    Scenario("stale-standby", "Standby dies, then primary dies, stale standby returns first", scenario_stale_standby),
    Scenario("late-messages", "Primary's links slower than its timeouts while the standby restarts; then primary killed", scenario_late_messages),
    *independent.SCENARIOS,
]

FAMILY_PORTS = {
    "replication": [GATEWAY_PORT, WITNESS_PORT, *NODE_PORTS.values(), *(p for p, _ in LINKS.values())],
    "independent": independent.PORTS,
}


# --------------------------------------------------------------------------- entry points


def run(impl: str, names: list[str], family: str | None) -> int:
    if not (ROOT / impl / "run.sh").is_file():
        print(f"unknown implementation: {impl}", file=sys.stderr)
        return 2
    by_name = {s.name: s for s in SCENARIOS}
    unknown = [n for n in names if n not in by_name]
    if unknown:
        print(f"unknown scenario(s): {', '.join(unknown)}; choose from {', '.join(by_name)}", file=sys.stderr)
        return 2
    supported = supported_families(impl)
    chosen = [by_name[n] for n in names] if names else [s for s in SCENARIOS if family in (None, s.family)]
    skipped = sorted({s.family for s in chosen} - supported)
    if skipped:
        missing = ", ".join(c for f in skipped for c in FAMILY_COMPONENTS[f])
        print(f"skipping {', '.join(skipped)} scenarios: {impl}/run.sh does not offer {missing}")
    chosen = [s for s in chosen if s.family in supported]
    busy = [port for f in {s.family for s in chosen} for port in FAMILY_PORTS[f] if port_in_use(port)]
    if busy:
        print(f"ports already in use: {busy}; stop those processes first", file=sys.stderr)
        return 1
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    failed_any = False
    for scenario in chosen:
        print(f"\n### {impl} / {scenario.name}: {scenario.title}")
        ctx = Context(impl, scenario)
        try:
            scenario.run(ctx)
        except ScenarioError as ex:
            ctx.failures.append(f"aborted: {ex}")
            print(f"  FAIL  aborted: {ex}")
        result = {
            "impl": impl,
            "scenario": scenario.name,
            "title": scenario.title,
            "passed": ctx.passed,
            "failures": ctx.failures,
            "metrics": ctx.metrics,
        }
        (RESULTS_DIR / f"{impl}-{scenario.name}.json").write_text(json.dumps(result, indent=2) + "\n")
        failed_any = failed_any or bool(ctx.failures)
    return 1 if failed_any else 0


HEADLINES = [
    ("write_p50_ms", "write p50 (ms)"),
    ("rto_s", "RTO (s)"),
    ("acked_writes", "acked"),
    ("lost_acked_writes", "lost acked"),
    ("acks_while_both_primary", "acked by both primaries"),
    ("failovers", "failovers"),
    ("failed_writes_during_storm", "failed during storm"),
    ("failed_writes_after_witness_died", "failed without witness"),
    ("outage_s", "outage (s)"),
]


def summary() -> int:
    results = [json.loads(p.read_text()) for p in sorted(RESULTS_DIR.glob("*.json"))]
    if not results:
        print("no results yet; run a scenario first")
        return 1
    order = {s.name: i for i, s in enumerate(SCENARIOS)}
    results.sort(key=lambda r: (r["impl"], order.get(r["scenario"], 99)))
    print("| impl | scenario | checks | " + " | ".join(label for _, label in HEADLINES) + " |")
    print("|" + "---|" * (3 + len(HEADLINES)))
    for r in results:
        checks = f"{r['passed']}/{r['passed'] + len(r['failures'])}"
        cells = [str(r["metrics"].get(key, "")) for key, _ in HEADLINES]
        print(f"| {r['impl']} | {r['scenario']} | {checks} | " + " | ".join(cells) + " |")
    return 0


# (scenario, metric, label) rows for `compare`: the numbers the README sets side by side.
COMPARISON = [
    ("crash", "rto_s", "主機崩潰：RTO（秒）"),
    ("crash", "second_rto_s", "切回修好的節點：RTO（秒）"),
    ("crash", "rejoin_catch_up_s", "舊主機重啟到追上（秒，含啟動）"),
    ("crash", "acked_writes_total", "兩次切換合計確認的寫入"),
    ("crash", "write_p50_ms", "同步、loopback：寫入 p50（ms）"),
    ("crash", "write_p99_ms", "同步、loopback：寫入 p99（ms）"),
    ("crash-async", "write_p50_ms", "非同步、loopback：寫入 p50（ms）"),
    ("crash-async", "lost_acked_writes", "非同步、loopback：崩潰遺失"),
    ("crash-slow", "write_p50_ms", "同步、10 ms 鏈路：寫入 p50（ms）"),
    ("crash-slow", "write_p99_ms", "同步、10 ms 鏈路：寫入 p99（ms）"),
    ("crash-async-slow", "write_p50_ms", "非同步、10 ms 鏈路：寫入 p50（ms）"),
    ("crash-async-slow", "lost_acked_writes", "非同步、10 ms 鏈路：崩潰遺失"),
    ("crash-async-slow", "lost_window_ms", "非同步、10 ms 鏈路：遺失的時間窗（ms）"),
    ("crash-fast", "rto_s", "1 秒租約：RTO（秒）"),
    ("partition", "rto_s", "分區（有 witness）：新主機開始服務（秒）"),
    ("partition", "acks_while_both_primary", "分區（有 witness）：兩台同時確認"),
    ("partition", "lost_acked_writes", "分區（有 witness）：遺失"),
    ("partition", "rejoin_discarded_entries", "分區（有 witness）：舊主機丟掉的未確認寫入"),
    ("partition-naive", "rto_s", "分區（沒有 witness）：備機升格（秒）"),
    ("partition-naive", "acks_while_both_primary", "分區（沒有 witness）：兩台同時確認"),
    ("partition-naive", "lost_acked_writes", "分區（沒有 witness）：遺失"),
    ("jitter", "failovers", "抖動、3 秒租約：誤判切換"),
    ("jitter", "failed_writes_during_storm", "抖動、3 秒租約：失敗的寫入"),
    ("jitter-fast", "failovers", "抖動、1 秒租約：誤判切換"),
    ("jitter-fast", "failed_writes_during_storm", "抖動、1 秒租約：失敗的寫入"),
    ("witness-down", "failed_writes_after_witness_died", "witness 掛掉：失敗的寫入"),
    ("stale-standby", "acked_while_alone", "備機落後：主機單獨確認的寫入"),
    ("stale-standby", "outage_s", "備機落後：停機（秒）"),
    ("stale-standby", "lost_acked_writes", "備機落後：遺失"),
    ("late-messages", "standby_took_over", "訊息晚到：備機有沒有接手"),
    ("late-messages", "lost_acked_writes", "訊息晚到：遺失"),
    ("hothot-crash", "missing", "B 熱熱、a 被砍：漏掉的結果"),
    ("hothot-crash", "longest_output_gap_ms", "B 熱熱、a 被砍：最長沒有新結果（ms）"),
    ("hothot-crash", "extra_copies", "B 熱熱：下游去掉的重複"),
    ("hothot-skip", "wrong", "B 重啟沒補讀（對照組）：錯誤結果"),
    ("hothot-nondeterministic", "conflicts", "B 結果帶時鐘（對照組）：不一致"),
    ("dispatch-crash", "killed_task_latency_ms", "C 指派、a 被砍：那件任務的延遲（ms）"),
    ("dispatch-pause", "extra_copies", "C 凍結、有檢查租約：晚到的寫入"),
    ("dispatch-pause-naive", "extra_copies", "C 凍結、不檢查（對照組）：晚到的寫入"),
    ("dispatch-sink-cut", "cut_off_task_latency_ms", "C 連不到下游：那件任務的延遲（ms）"),
    ("dispatch-ack-lost", "extra_copies", "C 回報遺失：做了兩次的任務"),
]


def compare() -> int:
    """Markdown table with one column per implementation, for the README."""
    results = {(r["impl"], r["scenario"]): r for r in map(lambda p: json.loads(p.read_text()), RESULTS_DIR.glob("*.json"))}
    impls = [i for i in ("dotnet", "rust", "python") if any(k[0] == i for k in results)]
    if not impls:
        print("no results yet; run a scenario first")
        return 1
    print("| 指標 | " + " | ".join(impls) + " |")
    print("|" + "---|" * (1 + len(impls)))
    checks = [f"{sum(r['passed'] for k, r in results.items() if k[0] == i)}/"
              f"{sum(r['passed'] + len(r['failures']) for k, r in results.items() if k[0] == i)}" for i in impls]
    print("| 通過的檢查 | " + " | ".join(checks) + " |")
    for scenario, metric, label in COMPARISON:
        cells = [str(results.get((i, scenario), {}).get("metrics", {}).get(metric, "—")) for i in impls]
        print(f"| {label} | " + " | ".join(cells) + " |")
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        return summary()
    if len(sys.argv) > 1 and sys.argv[1] == "compare":
        return compare()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--impl", required=True, help="implementation directory next to verify/, e.g. dotnet")
    parser.add_argument("--family", choices=["replication", "independent"], help="only this family of scenarios")
    parser.add_argument("scenarios", nargs="*", help="scenario names; all when omitted")
    args = parser.parse_args()
    return run(args.impl, args.scenarios, args.family)


if __name__ == "__main__":
    sys.exit(main())
