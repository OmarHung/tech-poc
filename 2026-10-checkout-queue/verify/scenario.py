#!/usr/bin/env python3
"""Black-box scenarios for the checkout-queue PoC.

Every language implementation follows the same HTTP contract (see ../README.md), so this one
script verifies all of them. Standard library only, so it runs without any virtualenv.

Requests are spread over every instance in APP_PORTS, round-robin per request, like a load
balancer without sticky sessions: a user may join on one instance, poll on another, and
double-click "checkout" so that each click lands on a different instance.

Usage:
    scenario.py flash-sale --lang dotnet   # correctness: a crowd buys out the stock
    scenario.py throughput --lang dotnet   # how many joins / polls per second
    scenario.py summary                    # markdown tables of all saved results
"""

from __future__ import annotations

import argparse
import http.client
import itertools
import json
import os
import random
import statistics
import sys
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"

PORTS = [int(p) for p in os.environ.get("APP_PORTS", "8091,8092").split(",")]
INITIAL_STOCK = int(os.environ.get("INITIAL_STOCK", "200"))
MAX_PER_ORDER = int(os.environ.get("MAX_PER_ORDER", "2"))
USERS = int(os.environ.get("USERS", "1000"))
# By default every user opens their connections before the sale starts, like shoppers already
# sitting on the product page. COLD_CONNECT=1 makes all of them connect at the same instant.
COLD_CONNECT = os.environ.get("COLD_CONNECT") == "1"
SALE_TIMEOUT_S = 180.0
BENCH_THREADS = 32
BENCH_DURATION_S = 5.0

# Ten user profiles, assigned by user index % 10.
PROFILES = ["buyer"] * 5 + ["double-tab", "leaves", "idle-after-admit", "never-pays", "buyer"]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class Reply:
    status: int
    body: object
    served_by: str | None


class Client:
    """Keep-alive HTTP client for one instance. Not thread-safe: one per thread."""

    def __init__(self, port: int, recorder: Recorder, timeout: float = 10.0) -> None:
        self._port = port
        self._recorder = recorder
        self._timeout = timeout
        self._conn: http.client.HTTPConnection | None = None

    def connect(self) -> None:
        self._conn = self._conn or http.client.HTTPConnection("127.0.0.1", self._port, timeout=self._timeout)
        self._conn.connect()

    def request(self, label: str, method: str, path: str, body: object = None, raw: bytes | None = None) -> Reply:
        payload = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        conn = self._conn or http.client.HTTPConnection("127.0.0.1", self._port, timeout=self._timeout)
        self._conn = conn
        start = time.perf_counter()
        try:
            conn.request(method, path, body=payload, headers=headers)
            response = conn.getresponse()
            data = response.read()
        except (OSError, http.client.HTTPException):
            conn.close()
            self._conn = None
            raise
        reply = Reply(response.status, _decode(data), response.getheader("X-Served-By"))
        self._recorder.record(label, (time.perf_counter() - start) * 1000, reply)
        return reply

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _decode(data: bytes) -> object:
    if not data:
        return None
    try:
        return json.loads(data)
    except ValueError:
        return data.decode(errors="replace")


class Recorder:
    """Thread-safe latency samples, per-instance counts and anything unexpected."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.latency: dict[str, list[float]] = defaultdict(list)
        self.served_by: Counter[str] = Counter()
        self.server_errors: list[str] = []

    def record(self, label: str, ms: float, reply: Reply) -> None:
        with self._lock:
            self.latency[label].append(ms)
            self.served_by[reply.served_by or "unknown"] += 1
            if reply.status >= 500 and len(self.server_errors) < 20:
                self.server_errors.append(f"{label} {reply.status} {reply.body}")

    def unexpected(self, detail: str) -> None:
        with self._lock:
            if len(self.server_errors) < 20:
                self.server_errors.append(detail)

    @property
    def total(self) -> int:
        return sum(len(v) for v in self.latency.values())


def percentiles(samples: list[float]) -> dict:
    if len(samples) < 2:
        return {"n": len(samples)}
    cuts = statistics.quantiles(samples, n=100)
    return {
        "n": len(samples),
        "p50_ms": round(statistics.median(samples), 2),
        "p95_ms": round(cuts[94], 2),
        "p99_ms": round(cuts[98], 2),
        "max_ms": round(max(samples), 2),
    }


@dataclass
class Checks:
    passed: int = 0
    failures: list[str] = field(default_factory=list)

    def check(self, name: str, condition: bool, detail: object = "") -> bool:
        if condition:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failures.append(f"{name} ({detail})")
            print(f"  FAIL  {name}  -> {detail}")
        return condition


# ---------------------------------------------------------------- one user


@dataclass
class UserRun:
    user_id: str
    profile: str
    ticket_id: str | None = None
    join_ticket_ids: list[str] = field(default_factory=list)
    final_status: str | None = None
    joined_at_ms: int | None = None
    join_done_at: float | None = None
    first_estimate_s: int | None = None
    first_estimate_at_ms: int | None = None
    admitted_seen_at_ms: int | None = None
    admitted_at_ms: int | None = None
    order_ids: set[str] = field(default_factory=set)
    pay_charged: int = 0
    paid_quantity: int = 0


class User:
    """One shopper. Every request goes to the next instance in turn."""

    def __init__(self, index: int, recorder: Recorder, deadline: float) -> None:
        self.run = UserRun(user_id=f"user-{index:05d}", profile=PROFILES[index % len(PROFILES)])
        self._index = index
        self._rng = random.Random(index)
        self._recorder = recorder
        self._deadline = deadline
        self._clients = [Client(port, recorder) for port in PORTS]
        self._turn = index % len(PORTS)

    def _next(self) -> Client:
        self._turn = (self._turn + 1) % len(self._clients)
        return self._clients[self._turn]

    def _both(self, label: str, method: str, path: str, body: object = None) -> list[Reply]:
        """Two identical requests at the same moment, each to a different instance."""
        replies: list[Reply | None] = [None, None]
        clients = [self._clients[0], self._clients[-1]]

        def send(slot: int) -> None:
            replies[slot] = clients[slot].request(label, method, path, body)

        other = threading.Thread(target=send, args=(1,))
        other.start()
        send(0)
        other.join()
        return [r for r in replies if r is not None]

    def play(self, start: threading.Barrier) -> UserRun:
        try:
            if not COLD_CONNECT:
                for client in self._clients:
                    client.connect()
            start.wait()
            self._play()
        except (OSError, http.client.HTTPException) as exc:
            self._recorder.unexpected(f"{self.run.user_id}: {exc!r}")
            self.run.final_status = "client_error"
        finally:
            for client in self._clients:
                client.close()
        return self.run

    def _play(self) -> None:
        run = self.run
        run.joined_at_ms = now_ms()
        if run.profile == "double-tab":
            replies = self._both("join", "POST", "/queue/join", {"userId": run.user_id})
        else:
            replies = [self._next().request("join", "POST", "/queue/join", {"userId": run.user_id})]
        for reply in replies:
            if reply.status in (200, 201):
                run.join_ticket_ids.append(reply.body["ticketId"])
            elif reply.status == 409:
                run.final_status = "sold_out"
                return
            else:
                self._recorder.unexpected(f"join {reply.status} {reply.body}")
                return
        run.join_done_at = time.monotonic()
        run.ticket_id = run.join_ticket_ids[0]
        ticket = replies[0].body
        if run.profile == "leaves":
            run.final_status = "left"
            return

        ticket = self._wait_for_turn(ticket)
        run.final_status = ticket["status"]
        if ticket["status"] != "admitted" or run.profile == "idle-after-admit":
            return
        run.admitted_seen_at_ms = now_ms()
        run.admitted_at_ms = ticket.get("admittedAtMs")

        time.sleep(self._rng.uniform(0.2, 0.8))  # filling in the checkout form
        order = self._checkout(ticket)
        if order is None or run.profile == "never-pays":
            return
        time.sleep(self._rng.uniform(0.1, 0.3))
        self._pay(order)

    def _wait_for_turn(self, ticket: dict) -> dict:
        while ticket["status"] == "waiting" and time.monotonic() < self._deadline:
            if self.run.first_estimate_at_ms is None and ticket.get("estimatedWaitSeconds") is not None:
                self.run.first_estimate_s = ticket["estimatedWaitSeconds"]
                self.run.first_estimate_at_ms = now_ms()
            time.sleep(ticket["pollAfterMs"] / 1000)
            reply = self._next().request("poll", "GET", f"/queue/tickets/{self.run.ticket_id}")
            if reply.status != 200:
                self._recorder.unexpected(f"poll {reply.status} {reply.body}")
                return {"status": "client_error"}
            ticket = reply.body
        return ticket

    def _checkout(self, ticket: dict) -> dict | None:
        """Keeps trying while admitted and only short of stock: unpaid orders may release some."""
        quantity = 1 + self._index % MAX_PER_ORDER
        double_click = self._index % 3 == 0
        while time.monotonic() < self._deadline:
            body = {"ticketId": self.run.ticket_id, "quantity": quantity}
            replies = self._both("checkout", "POST", "/checkout", body) if double_click else [
                self._next().request("checkout", "POST", "/checkout", body)]
            orders = [r.body for r in replies if r.status in (200, 201)]
            self.run.order_ids.update(o["orderId"] for o in orders)
            if orders:
                self.run.final_status = "ordered"
                return orders[0]
            codes = {r.body.get("code") if isinstance(r.body, dict) else r.status for r in replies}
            if codes & {"admission_expired", "sold_out"}:
                self.run.final_status = min(codes & {"admission_expired", "sold_out"})
                return None
            if codes != {"insufficient_stock"}:
                self._recorder.unexpected(f"checkout {[(r.status, r.body) for r in replies]}")
                return None
            quantity = 1
            time.sleep(ticket["pollAfterMs"] / 1000)
        return None

    def _pay(self, order: dict) -> None:
        order_id = order["orderId"]
        double_click = self._index % 4 == 0
        replies = self._both("pay", "POST", f"/orders/{order_id}/pay") if double_click else [
            self._next().request("pay", "POST", f"/orders/{order_id}/pay")]
        self.run.pay_charged = sum(r.status == 201 for r in replies)
        if any(r.status in (200, 201) for r in replies):
            self.run.final_status = "paid"
            self.run.paid_quantity = order["quantity"]
        else:
            self._recorder.unexpected(f"pay {[(r.status, r.body) for r in replies]}")


# ---------------------------------------------------------------- phases


def health(checks: Checks, recorder: Recorder) -> list[str]:
    names = []
    for port in PORTS:
        reply = Client(port, recorder).request("health", "GET", "/health")
        checks.check(f"instance on :{port} is healthy", reply.status == 200, f"{reply.status} {reply.body}")
        names.append(reply.served_by or "?")
    checks.check("every instance has its own name", len(set(names)) == len(PORTS), names)
    return names


def fetch_json(recorder: Recorder, path: str) -> dict:
    reply = Client(PORTS[0], recorder).request("admin", "GET", path)
    if reply.status != 200 or not isinstance(reply.body, dict):
        raise RuntimeError(f"GET {path} failed: {reply.status} {reply.body}")
    return reply.body


def phase_flash_sale(checks: Checks) -> dict:
    recorder = Recorder()
    instances = health(checks, recorder)
    print(f"  {USERS} users, {INITIAL_STOCK} in stock, up to {MAX_PER_ORDER} each, instances {instances}, cold connect: {COLD_CONNECT}")

    deadline = time.monotonic() + SALE_TIMEOUT_S
    users = [User(i, recorder, deadline) for i in range(USERS)]
    start = threading.Barrier(USERS + 1)
    results: list[UserRun] = []
    lock = threading.Lock()

    def play(user: User) -> None:
        run = user.play(start)
        with lock:
            results.append(run)

    threads = [threading.Thread(target=play, args=(u,), daemon=True) for u in users]
    for thread in threads:
        thread.start()
    start.wait()
    began = time.monotonic()
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()) + 5)
    elapsed = time.monotonic() - began
    join_wall = max((r.join_done_at for r in results if r.join_done_at), default=began) - began
    print(f"  everyone joined within {join_wall:.2f}s; sale finished in {elapsed:.1f}s, {recorder.total} requests ({recorder.total / elapsed:.0f}/s)")

    audit = fetch_json(recorder, "/admin/audit")
    tickets = final_tickets(recorder, results)
    return {**assess_flash_sale(checks, recorder, results, audit, tickets, elapsed), "cold_connect": COLD_CONNECT, "join_wall_s": round(join_wall, 2)}


def final_tickets(recorder: Recorder, results: list[UserRun]) -> list[dict]:
    client = Client(PORTS[0], recorder)
    tickets = []
    for run in results:
        if run.ticket_id:
            reply = client.request("final-poll", "GET", f"/queue/tickets/{run.ticket_id}")
            if reply.status == 200:
                tickets.append(reply.body)
    client.close()
    return tickets


def assess_flash_sale(checks: Checks, recorder: Recorder, results: list[UserRun], audit: dict, tickets: list[dict], elapsed: float) -> dict:
    print(f"  audit: {json.dumps(audit)}")
    checks.check("every user finished before the timeout", len(results) == USERS, f"{len(results)}/{USERS}")
    checks.check("no 5xx, transport errors or unexpected replies", not recorder.server_errors, recorder.server_errors[:5])

    # The two numbers the PoC is judged on.
    checks.check("oversold units = 0", audit["oversold"] == 0, audit["oversold"])
    checks.check("duplicate charges = 0", audit["duplicateCharges"] == 0, audit["duplicateCharges"])
    checks.check("stock + held + sold = initial stock", audit["stockBalanced"], audit)
    checks.check("charges = paid orders", audit["charges"] == audit["paidOrders"], audit)
    checks.check("no user holds two live orders", audit["usersWithMultipleLiveOrders"] == 0, audit)

    # The same facts seen from the client side, independent of the server's own audit.
    two_orders = [r.user_id for r in results if len(r.order_ids) > 1]
    checks.check("double clicks on two instances got the same order", not two_orders, two_orders[:5])
    double_tab = [r for r in results if r.profile == "double-tab" and len(r.join_ticket_ids) == 2]
    split = [r.user_id for r in double_tab if len(set(r.join_ticket_ids)) != 1]
    checks.check(f"joining from two tabs at once gave one ticket ({len(double_tab)} users)", double_tab and not split, split[:5])
    paid_runs = [r for r in results if r.final_status == "paid"]
    wrong_charges = [(r.user_id, r.pay_charged) for r in paid_runs if r.pay_charged != 1]
    checks.check("every paid order was charged by exactly one request", not wrong_charges, wrong_charges[:5])
    client_paid = sum(r.paid_quantity for r in paid_runs)
    checks.check("client-side paid units = server-side paid units", client_paid == audit["paidQuantity"], f"{client_paid} vs {audit['paidQuantity']}")

    # Sold out exactly: unpaid stock went back on the shelf and was sold to someone else.
    checks.check("all stock sold and paid, none left held", (audit["paidQuantity"], audit["pendingQuantity"], audit["stock"]) == (INITIAL_STOCK, 0, 0), audit)

    admitted = sorted((t for t in tickets if "admittedAtMs" in t), key=lambda t: t["seq"])
    out_of_order = [(a["seq"], b["seq"]) for a, b in itertools.pairwise(admitted) if b["admittedAtMs"] < a["admittedAtMs"]]
    checks.check(f"admitted in arrival order ({len(admitted)} admissions)", admitted and not out_of_order, out_of_order[:5])
    statuses = Counter(t["status"] for t in tickets)
    checks.check("users who stopped polling were skipped", statuses["abandoned"] > 0, dict(statuses))
    checks.check("users who did not check out in time were expired", statuses["expired"] > 0, dict(statuses))
    checks.check("unpaid orders were released", audit["expiredOrders"] > 0, audit)
    share = {name: n / max(1, recorder.total) for name, n in recorder.served_by.items()}
    checks.check("every instance served a fair share of traffic", all(share.get(f"instance-{i}", 0) > 0.3 for i in range(len(PORTS))), share)

    return {
        "users": USERS,
        "initial_stock": INITIAL_STOCK,
        "elapsed_s": round(elapsed, 2),
        "requests": recorder.total,
        "requests_per_s": round(recorder.total / elapsed),
        "served_by": dict(recorder.served_by),
        "audit": audit,
        "ticket_statuses": dict(statuses),
        "profiles": {p: dict(Counter(r.final_status for r in results if r.profile == p)) for p in sorted(set(PROFILES))},
        "latency": {label: percentiles(samples) for label, samples in sorted(recorder.latency.items())},
        "status_delay": status_delay(results),
        "estimate_error": estimate_error(results),
    }


def status_delay(results: list[UserRun]) -> dict:
    """How long after the server admitted a user the user found out (by polling)."""
    delays = [float(r.admitted_seen_at_ms - r.admitted_at_ms) for r in results if r.admitted_seen_at_ms and r.admitted_at_ms]
    return percentiles(delays)


def estimate_error(results: list[UserRun]) -> dict:
    """First wait estimate a user saw vs. how long they really waited from that moment."""
    errors = [
        r.first_estimate_s - (r.admitted_at_ms - r.first_estimate_at_ms) / 1000
        for r in results
        if r.first_estimate_at_ms and r.admitted_at_ms and r.admitted_at_ms > r.first_estimate_at_ms
    ]
    if len(errors) < 2:
        return {"n": len(errors)}
    return {
        "n": len(errors),
        "median_error_s": round(statistics.median(errors), 1),
        "median_abs_error_s": round(statistics.median(abs(e) for e in errors), 1),
    }


def hammer(label: str, work) -> dict:
    """Runs `work(client, n)` from BENCH_THREADS threads for BENCH_DURATION_S; returns rate and latency."""
    recorder = Recorder()
    stop = threading.Event()

    def worker(index: int) -> None:
        client = Client(PORTS[index % len(PORTS)], recorder)
        n = 0
        while not stop.is_set():
            work(client, index, n)
            n += 1
        client.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(BENCH_THREADS)]
    for thread in threads:
        thread.start()
    time.sleep(BENCH_DURATION_S)
    stop.set()
    for thread in threads:
        thread.join()
    samples = recorder.latency[label]
    errors = recorder.server_errors
    result = {"requests_per_s": round(len(samples) / BENCH_DURATION_S), "errors": len(errors), **percentiles(samples)}
    print(f"  BENCH {label:<6} {result['requests_per_s']:>6}/s  p50={result['p50_ms']}ms p95={result['p95_ms']}ms p99={result['p99_ms']}ms errors={len(errors)}")
    return result


def phase_throughput(checks: Checks) -> dict:
    recorder = Recorder()
    health(checks, recorder)
    ticket_ids: list[str] = []
    lock = threading.Lock()

    def join(client: Client, index: int, n: int) -> None:
        reply = client.request("join", "POST", "/queue/join", {"userId": f"bench-{index}-{n}"})
        if reply.status == 201:
            with lock:
                ticket_ids.append(reply.body["ticketId"])

    def poll(client: Client, index: int, n: int) -> None:
        client.request("poll", "GET", f"/queue/tickets/{ticket_ids[(index * 7919 + n) % len(ticket_ids)]}")

    joins = hammer("join", join)
    if not checks.check("joins succeeded without errors", joins["errors"] == 0 and bool(ticket_ids), joins):
        return {"join": joins}
    polls = hammer("poll", poll)
    checks.check("polls succeeded without errors", polls["errors"] == 0, polls)
    poll_interval_s = 1.0
    return {
        "threads": BENCH_THREADS,
        "instances": len(PORTS),
        "join": joins,
        "poll": polls,
        # Each waiting user polls once per interval, so this is how many can wait at once.
        "waiting_users_supported_at_1s_poll": round(polls["requests_per_s"] * poll_interval_s),
    }


PHASES = {"flash-sale": phase_flash_sale, "throughput": phase_throughput}


def run_phase(lang: str, phase: str) -> int:
    print(f"\n=== {lang} / {phase} ===")
    checks = Checks()
    try:
        extra = PHASES[phase](checks)
    except (OSError, http.client.HTTPException, RuntimeError, KeyError, TypeError) as exc:
        checks.check("scenario ran to completion", False, repr(exc))
        extra = {}
    result = {"lang": lang, "phase": phase, "passed": checks.passed, "failures": checks.failures, **extra}
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / f"{lang}-{phase}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=list) + "\n")
    print(f"  => {checks.passed} passed, {len(checks.failures)} failed")
    return 0 if not checks.failures else 1


def summary() -> int:
    results = [json.loads(path.read_text()) for path in sorted(RESULTS_DIR.glob("*.json"))]
    print("| 語言 | 情境 | 通過 | 失敗 |")
    print("|------|------|------|------|")
    for r in results:
        print(f"| {r['lang']} | {r['phase']} | {r['passed']} | {len(r['failures'])} |")
    for r in (r for r in results if r["phase"] == "flash-sale" and "audit" in r):
        a = r["audit"]
        print(f"\n**{r['lang']} flash-sale**：{r['users']} 人搶 {r['initial_stock']} 件，全員 {r['join_wall_s']} 秒內排進隊伍，整場 {r['elapsed_s']} 秒，{r['requests']} 個請求（{r['requests_per_s']}/s）")
        print("\n| 超賣 | 重複扣款 | 售出 | 釋放的未付款訂單 | 狀態更新延遲 p50 / p95 | 預估等待誤差（中位數） |")
        print("|------|----------|------|------------------|------------------------|------------------------|")
        d, e = r["status_delay"], r["estimate_error"]
        print(f"| {a['oversold']} | {a['duplicateCharges']} | {a['paidQuantity']} | {a['expiredOrders']} 筆 | {d.get('p50_ms')} / {d.get('p95_ms')} ms | {e.get('median_abs_error_s')} 秒 |")
        print("\n| 操作 | 次數 | p50 (ms) | p95 (ms) | p99 (ms) |")
        print("|------|------|----------|----------|----------|")
        for op, s in r["latency"].items():
            if "p50_ms" in s:
                print(f"| {op} | {s['n']} | {s['p50_ms']} | {s['p95_ms']} | {s['p99_ms']} |")
    for r in (r for r in results if r["phase"] == "throughput" and "join" in r):
        print(f"\n**{r['lang']} throughput**（{r['instances']} 台、{r['threads']} 條連線）")
        print("\n| 操作 | 每秒請求 | p50 (ms) | p95 (ms) | p99 (ms) |")
        print("|------|----------|----------|----------|----------|")
        for op in ("join", "poll"):
            s = r[op]
            print(f"| {op} | {s['requests_per_s']} | {s['p50_ms']} | {s['p95_ms']} | {s['p99_ms']} |")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=[*PHASES, "summary"])
    parser.add_argument("--lang")
    args = parser.parse_args()
    if args.command == "summary":
        return summary()
    if not args.lang:
        parser.error("--lang is required")
    return run_phase(args.lang, args.command)


if __name__ == "__main__":
    sys.exit(main())
