#!/usr/bin/env python3
"""Black-box scenario for the modular-monolith-to-microservices PoC.

Every language implementation follows the same HTTP contract (see ../README.md),
so this one script verifies all of them and produces comparable numbers.
Standard library only, so it runs without any project's virtualenv.

Usage:
    scenario.py --lang rust --phase monolith       # before extraction
    scenario.py --lang rust --phase extracted      # catalog moved out, gateway switching
    scenario.py --lang rust --phase catalog-down   # catalog-service stopped
    scenario.py summary                            # markdown table of all saved results
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import statistics
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"

GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "8080"))
MONOLITH_PORT = int(os.environ.get("MONOLITH_PORT", "8081"))
CATALOG_PORT = int(os.environ.get("CATALOG_PORT", "8082"))

MAX_QUANTITY = 100
HIGH_STOCK_PRODUCT_ID = 3  # seeded with a huge stock so load tests never run out
# Time-based so freshly restarted processes are measured warm, like a long-running
# server. A fixed 100-2000 requests left .NET's tiered JIT mid-optimization.
BENCH_WARMUP_S = 3.0
BENCH_REQUESTS = 1000
SWITCH_DURATION_S = 3.0
SWITCH_INTERVAL_S = 0.2
SWITCH_WORKERS = 8
MIN_SWITCH_TOGGLES = 10


@dataclass(frozen=True)
class Reply:
    status: int
    body: object
    served_by: str | None


class Client:
    """Keep-alive HTTP client for one local port."""

    def __init__(self, port: int, timeout: float = 5.0) -> None:
        self._port = port
        self._timeout = timeout
        self._conn: http.client.HTTPConnection | None = None

    def request(self, method: str, path: str, body: object = None, raw: bytes | None = None) -> Reply:
        payload = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        conn = self._conn or http.client.HTTPConnection("127.0.0.1", self._port, timeout=self._timeout)
        self._conn = conn
        try:
            conn.request(method, path, body=payload, headers=headers)
            response = conn.getresponse()
            data = response.read()
        except (OSError, http.client.HTTPException):
            conn.close()
            self._conn = None
            raise
        return Reply(response.status, _decode(data), response.getheader("X-Served-By"))

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


def is_error_body(reply: Reply) -> bool:
    return isinstance(reply.body, dict) and isinstance(reply.body.get("error"), str)


def expect_error(checks: Checks, name: str, reply: Reply, status: int) -> None:
    checks.check(name, reply.status == status and is_error_body(reply), f"{reply.status} {reply.body}")


def set_route(gateway: Client, target: str) -> Reply:
    reply = gateway.request("PUT", "/admin/routes", {"catalog": target})
    if reply.status != 200:
        raise RuntimeError(f"failed to switch gateway route to {target}: {reply.status} {reply.body}")
    return reply


# ---------------------------------------------------------------- functional checks


def check_health(checks: Checks, gateway: Client, monolith: Client, catalog: Client | None, mode: str) -> None:
    gw = gateway.request("GET", "/health")
    checks.check("gateway /health", gw.status == 200 and gw.served_by == "gateway", f"{gw.status} {gw.served_by}")
    mono = monolith.request("GET", "/health")
    checks.check(
        f"monolith /health reports catalogMode={mode}",
        mono.status == 200 and isinstance(mono.body, dict) and mono.body.get("catalogMode") == mode,
        mono.body,
    )
    if catalog is not None:
        cat = catalog.request("GET", "/health")
        checks.check("catalog-service /health", cat.status == 200 and cat.served_by == "catalog-service", cat.served_by)


def check_catalog_reads(checks: Checks, gateway: Client, served_by: str) -> None:
    products = gateway.request("GET", "/products")
    checks.check(
        f"GET /products served by {served_by}",
        products.status == 200 and isinstance(products.body, list) and products.served_by == served_by,
        f"{products.status} {products.served_by}",
    )
    ids = [p.get("id") for p in products.body] if isinstance(products.body, list) else []
    checks.check("GET /products returns seeded products in id order", ids == [1, 2, 3], ids)
    single = gateway.request("GET", "/products/1")
    checks.check("GET /products/1", single.status == 200 and isinstance(single.body, dict) and single.body.get("id") == 1, single.body)


def check_order_flow(checks: Checks, gateway: Client, stock_readers: dict[str, Client]) -> None:
    before = {name: client.request("GET", "/products/1").body for name, client in stock_readers.items()}
    reference = next(iter(before.values()))
    order = gateway.request("POST", "/orders", {"productId": 1, "quantity": 2})
    created = order.status == 201 and isinstance(order.body, dict)
    checks.check("POST /orders returns 201", created, f"{order.status} {order.body}")
    if not created:
        return
    checks.check("order total = price x quantity", order.body.get("total") == reference["price"] * 2, order.body)
    checks.check("order keeps product name snapshot", order.body.get("productName") == reference["name"], order.body)
    for name, client in stock_readers.items():
        after = client.request("GET", "/products/1").body
        checks.check(f"stock decreased by 2 (seen via {name})", after["stock"] == before[name]["stock"] - 2, after)
    fetched = gateway.request("GET", f"/orders/{order.body['id']}")
    checks.check("GET /orders/{id} returns the same order", fetched.status == 200 and fetched.body == order.body, fetched.body)

    remaining = gateway.request("GET", "/products/1").body["stock"]
    if remaining + 1 <= MAX_QUANTITY:
        over = gateway.request("POST", "/orders", {"productId": 1, "quantity": remaining + 1})
        expect_error(checks, "ordering more than stock returns 409", over, 409)
        unchanged = gateway.request("GET", "/products/1").body["stock"]
        checks.check("rejected order does not change stock", unchanged == remaining, unchanged)


def check_validation(checks: Checks, gateway: Client) -> None:
    cases = [
        ("quantity 0", {"productId": 1, "quantity": 0}),
        (f"quantity {MAX_QUANTITY + 1}", {"productId": 1, "quantity": MAX_QUANTITY + 1}),
        ("negative quantity", {"productId": 1, "quantity": -1}),
        ("quantity as string", {"productId": 1, "quantity": "2"}),
        ("productId 0", {"productId": 0, "quantity": 1}),
        ("missing productId", {"quantity": 1}),
    ]
    for label, body in cases:
        expect_error(checks, f"POST /orders with {label} returns 400", gateway.request("POST", "/orders", body), 400)
    expect_error(checks, "POST /orders with malformed JSON returns 400", gateway.request("POST", "/orders", raw=b"{not json"), 400)
    expect_error(checks, "POST /orders for unknown product returns 404", gateway.request("POST", "/orders", {"productId": 999, "quantity": 1}), 404)
    expect_error(checks, "GET /products/999 returns 404", gateway.request("GET", "/products/999"), 404)
    expect_error(checks, "GET /products/abc returns 400", gateway.request("GET", "/products/abc"), 400)
    expect_error(checks, "GET /products/0 returns 400", gateway.request("GET", "/products/0"), 400)
    expect_error(checks, "GET /orders/999 returns 404", gateway.request("GET", "/orders/999"), 404)
    expect_error(checks, "GET /orders/abc returns 400", gateway.request("GET", "/orders/abc"), 400)
    expect_error(checks, "reserve with quantity 0 returns 400", gateway.request("POST", "/products/1/reserve", {"quantity": 0}), 400)
    expect_error(checks, "invalid gateway route returns 400", gateway.request("PUT", "/admin/routes", {"catalog": "nope"}), 400)


# ---------------------------------------------------------------- performance


def measure(client: Client, method: str, path: str, body: object = None) -> dict:
    warm_until = time.monotonic() + BENCH_WARMUP_S
    while time.monotonic() < warm_until:
        client.request(method, path, body)
    samples: list[float] = []
    errors = 0
    for _ in range(BENCH_REQUESTS):
        start = time.perf_counter()
        reply = client.request(method, path, body)
        samples.append((time.perf_counter() - start) * 1000)
        errors += reply.status >= 400
    cuts = statistics.quantiles(samples, n=100)
    return {
        "n": BENCH_REQUESTS,
        "p50_ms": round(statistics.median(samples), 3),
        "p95_ms": round(cuts[94], 3),
        "p99_ms": round(cuts[98], 3),
        "errors": errors,
    }


def benchmark(label: str, gateway: Client, *, include_orders: bool = True) -> dict:
    results = {"get_product": measure(gateway, "GET", f"/products/{HIGH_STOCK_PRODUCT_ID}")}
    if include_orders:
        order_body = {"productId": HIGH_STOCK_PRODUCT_ID, "quantity": 1}
        results["place_order"] = measure(gateway, "POST", "/orders", order_body)
    for name, stats in results.items():
        print(f"  BENCH {label:<22} {name:<12} p50={stats['p50_ms']:.3f}ms p95={stats['p95_ms']:.3f}ms p99={stats['p99_ms']:.3f}ms errors={stats['errors']}")
    return results


def switch_under_load(gateway: Client) -> dict:
    """Flip the catalog route back and forth while clients keep sending traffic."""
    stop = threading.Event()
    lock = threading.Lock()
    totals = {"requests": 0, "failures": 0}
    served_by: Counter[str] = Counter()
    failure_samples: list[str] = []

    def worker(index: int) -> None:
        client = Client(GATEWAY_PORT)
        reads = index % 2 == 0
        while not stop.is_set():
            try:
                if reads:
                    reply = client.request("GET", f"/products/{HIGH_STOCK_PRODUCT_ID}")
                else:
                    reply = client.request("POST", "/orders", {"productId": HIGH_STOCK_PRODUCT_ID, "quantity": 1})
                failed, detail = reply.status not in (200, 201), f"{reply.status} {reply.body}"
            except (OSError, http.client.HTTPException) as exc:
                reply, failed, detail = None, True, repr(exc)
            with lock:
                totals["requests"] += 1
                if failed:
                    totals["failures"] += 1
                    if len(failure_samples) < 5:
                        failure_samples.append(detail)
                elif reads and reply is not None:
                    served_by[reply.served_by or "unknown"] += 1
        client.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(SWITCH_WORKERS)]
    for thread in threads:
        thread.start()
    toggles, target = 0, "service"
    deadline = time.monotonic() + SWITCH_DURATION_S
    while time.monotonic() < deadline:
        set_route(gateway, target)
        toggles += 1
        target = "monolith" if target == "service" else "service"
        time.sleep(SWITCH_INTERVAL_S)
    stop.set()
    for thread in threads:
        thread.join()
    set_route(gateway, "monolith")
    return {**totals, "toggles": toggles, "served_by": dict(served_by), "failure_samples": failure_samples}


# ---------------------------------------------------------------- phases


def phase_monolith(checks: Checks) -> dict:
    gateway, monolith, catalog = Client(GATEWAY_PORT), Client(MONOLITH_PORT), Client(CATALOG_PORT)
    set_route(gateway, "monolith")
    check_health(checks, gateway, monolith, catalog, "inprocess")
    check_catalog_reads(checks, gateway, "monolith")
    check_order_flow(checks, gateway, {"gateway": gateway})
    check_validation(checks, gateway)
    return {"latency": {"monolith (in-process)": benchmark("monolith (in-process)", gateway)}}


def phase_extracted(checks: Checks) -> dict:
    gateway, monolith, catalog = Client(GATEWAY_PORT), Client(MONOLITH_PORT), Client(CATALOG_PORT)
    set_route(gateway, "monolith")
    check_health(checks, gateway, monolith, catalog, "remote")
    check_catalog_reads(checks, gateway, "monolith")
    check_order_flow(checks, gateway, {"gateway": gateway, "catalog-service": catalog})
    mono_view = monolith.request("GET", "/products/1").body
    service_view = catalog.request("GET", "/products/1").body
    checks.check("monolith and catalog-service see the same stock", mono_view == service_view, f"{mono_view} vs {service_view}")
    check_validation(checks, gateway)
    latency = {"extracted, via monolith": benchmark("extracted, via monolith", gateway)}

    switch = switch_under_load(gateway)
    print(f"  SWITCH requests={switch['requests']} failures={switch['failures']} toggles={switch['toggles']} served_by={switch['served_by']}")
    checks.check("no failed requests while switching routes", switch["failures"] == 0, switch["failure_samples"])
    checks.check(f"route switched at least {MIN_SWITCH_TOGGLES} times", switch["toggles"] >= MIN_SWITCH_TOGGLES, switch["toggles"])
    checks.check(
        "reads were served by both monolith and catalog-service during switching",
        {"monolith", "catalog-service"} <= set(switch["served_by"]),
        switch["served_by"],
    )

    set_route(gateway, "service")
    check_catalog_reads(checks, gateway, "catalog-service")
    # Orders take the same path under either route, so only catalog reads are measured here.
    latency["extracted, direct route"] = benchmark("extracted, direct route", gateway, include_orders=False)
    set_route(gateway, "monolith")
    return {"latency": latency, "switch": switch}


def phase_catalog_down(checks: Checks) -> dict:
    gateway, monolith = Client(GATEWAY_PORT), Client(MONOLITH_PORT)
    check_health(checks, gateway, monolith, None, "remote")
    expect_error(checks, "POST /orders returns 502 when catalog is down", gateway.request("POST", "/orders", {"productId": 1, "quantity": 1}), 502)
    set_route(gateway, "service")
    expect_error(checks, "GET /products returns 502 via direct route", gateway.request("GET", "/products"), 502)
    set_route(gateway, "monolith")
    expect_error(checks, "GET /products returns 502 via monolith", gateway.request("GET", "/products"), 502)
    existing = gateway.request("GET", "/orders/1")
    checks.check("existing orders are still readable (monolith keeps its own data)", existing.status == 200, f"{existing.status} {existing.body}")
    return {}


PHASES = {"monolith": phase_monolith, "extracted": phase_extracted, "catalog-down": phase_catalog_down}


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
    (RESULTS_DIR / f"{lang}-{phase}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"  => {checks.passed} passed, {len(checks.failures)} failed")
    return 0 if not checks.failures else 1


def summary() -> int:
    results = [json.loads(path.read_text()) for path in sorted(RESULTS_DIR.glob("*.json"))]
    print("| 語言 | 階段 | 通過 | 失敗 |")
    print("|------|------|------|------|")
    for r in results:
        print(f"| {r['lang']} | {r['phase']} | {r['passed']} | {len(r['failures'])} |")
    print("\n| 語言 | 情境 | 操作 | p50 (ms) | p95 (ms) | p99 (ms) |")
    print("|------|------|------|----------|----------|----------|")
    for r in results:
        for label, ops in r.get("latency", {}).items():
            for op, s in ops.items():
                print(f"| {r['lang']} | {label} | {op} | {s['p50_ms']:.3f} | {s['p95_ms']:.3f} | {s['p99_ms']:.3f} |")
    print("\n| 語言 | 切換期間請求數 | 失敗 | 切換次數 |")
    print("|------|----------------|------|----------|")
    for r in results:
        if "switch" in r:
            s = r["switch"]
            print(f"| {r['lang']} | {s['requests']} | {s['failures']} | {s['toggles']} |")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", nargs="?", choices=["run", "summary"], default="run")
    parser.add_argument("--lang")
    parser.add_argument("--phase", choices=sorted(PHASES))
    args = parser.parse_args()
    if args.command == "summary":
        return summary()
    if not args.lang or not args.phase:
        parser.error("--lang and --phase are required")
    return run_phase(args.lang, args.phase)


if __name__ == "__main__":
    sys.exit(main())
