#!/usr/bin/env python3
"""Polling vs server-sent events, measured on the same moving line.

A long line moves at a steady pace: MAX_ACTIVE users at a time are let in, each checks out
after a short think time, freeing the slot for the next. The same crowd watches its tickets
either by polling GET /queue/tickets/{id} or over GET /queue/tickets/{id}/events.

Measured over a fixed window once the line has formed:
- how long after the server admitted a user the user found out
- requests reaching the app, Redis script time, app CPU and memory

Standard library only. One asyncio thread drives every user, so the client adds as little
delay of its own as possible (the threaded flash-sale client loses seconds to the GIL).

Usage (verify/run.sh sets the environment):
    push.py --lang dotnet --mode poll
    push.py --lang dotnet --mode sse
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"
PORTS = [int(p) for p in os.environ.get("APP_PORTS", "8091,8092").split(",")]
APP_PIDS = os.environ.get("APP_PIDS", "").split()
REDIS_CONTAINER = os.environ.get("REDIS_CONTAINER", "")
USERS = int(os.environ.get("PUSH_USERS", "1000"))
JOIN_SPREAD_S = 2.0       # joins are spread out; the spike itself is the flash-sale scenario's job
SETTLE_S = 3.0            # let the line form before measuring
WINDOW_S = 20.0
THINK_S = 0.5             # time an admitted user spends before checking out
SSE_P95_LIMIT_MS = 500


def now_ms() -> int:
    return time.time_ns() // 1_000_000


# ---------------------------------------------------------------- a tiny HTTP/1.1 client


class Http:
    """One keep-alive connection. Handles sized and chunked bodies, which is all this API sends."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def _send(self, method: str, path: str, body: object = None, accept: str = "application/json") -> None:
        if self.writer is None:
            self.reader, self.writer = await asyncio.open_connection("127.0.0.1", self.port)
        payload = b"" if body is None else json.dumps(body).encode()
        head = f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\nAccept: {accept}\r\n"
        if body is not None:
            head += f"Content-Type: application/json\r\nContent-Length: {len(payload)}\r\n"
        self.writer.write(head.encode() + b"\r\n" + payload)
        await self.writer.drain()

    async def _head(self) -> tuple[int, dict[str, str]]:
        status_line = await self.reader.readline()
        if not status_line:
            raise ConnectionError("connection closed")
        status = int(status_line.split()[1])
        headers = {}
        while (line := await self.reader.readline()) not in (b"\r\n", b""):
            name, _, value = line.decode().partition(":")
            headers[name.strip().lower()] = value.strip()
        return status, headers

    async def _chunk(self) -> bytes:
        size = int((await self.reader.readline()).split(b";")[0], 16)
        data = await self.reader.readexactly(size) if size else b""
        await self.reader.readline()
        return data

    async def request(self, method: str, path: str, body: object = None) -> tuple[int, dict]:
        await self._send(method, path, body)
        status, headers = await self._head()
        if "content-length" in headers:
            data = await self.reader.readexactly(int(headers["content-length"]))
        else:
            parts = []
            while chunk := await self._chunk():
                parts.append(chunk)
            data = b"".join(parts)
        return status, json.loads(data) if data else {}

    async def events(self, path: str):
        """Yields each server-sent event's JSON data until the server ends the stream."""
        await self._send("GET", path, accept="text/event-stream")
        status, _ = await self._head()
        if status != 200:
            raise ConnectionError(f"events returned {status}")
        buffer = ""
        while chunk := await self._chunk():
            buffer += chunk.decode()
            while "\n\n" in buffer:
                event, buffer = buffer.split("\n\n", 1)
                for line in event.split("\n"):
                    if line.startswith("data: "):
                        yield json.loads(line[6:])

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None


# ---------------------------------------------------------------- the crowd


@dataclass
class Tally:
    requests: list[float] = field(default_factory=list)   # monotonic time of each request to the app
    events: list[float] = field(default_factory=list)     # monotonic time of each event received
    admissions: list[tuple[int, int]] = field(default_factory=list)  # (server admittedAtMs, delay ms)
    errors: list[str] = field(default_factory=list)

    def request(self) -> None:
        self.requests.append(time.monotonic())


async def user(index: int, mode: str, tally: Tally, deadline: float) -> None:
    port = PORTS[index % len(PORTS)]
    http = Http(port)
    try:
        tally.request()
        status, ticket = await http.request("POST", "/queue/join", {"userId": f"push-{mode}-{index}"})
        if status not in (200, 201):
            tally.errors.append(f"join {status} {ticket}")
            return
        path = f"/queue/tickets/{ticket['ticketId']}"
        if mode == "poll":
            while ticket["status"] == "waiting" and time.monotonic() < deadline:
                await asyncio.sleep(ticket["pollAfterMs"] / 1000)
                tally.request()
                status, ticket = await http.request("GET", path)
        else:
            stream = Http(port)
            tally.request()
            try:
                async for ticket in stream.events(f"{path}/events"):
                    tally.events.append(time.monotonic())
                    if ticket["status"] != "waiting":
                        break
            finally:
                stream.close()
        if ticket["status"] != "admitted":
            return
        tally.admissions.append((ticket["admittedAtMs"], now_ms() - ticket["admittedAtMs"]))
        await asyncio.sleep(THINK_S)
        tally.request()
        status, order = await http.request("POST", "/checkout", {"ticketId": ticket["ticketId"], "quantity": 1})
        if status != 201:
            tally.errors.append(f"checkout {status} {order}")
    except (OSError, ConnectionError, asyncio.IncompleteReadError, ValueError) as exc:
        tally.errors.append(f"user {index}: {exc!r}")
    finally:
        http.close()


# ---------------------------------------------------------------- server-side measurements


def redis_script_stats() -> tuple[int, int]:
    """(calls, microseconds) spent in EVAL/EVALSHA so far."""
    out = subprocess.run(["docker", "exec", REDIS_CONTAINER, "redis-cli", "INFO", "commandstats"],
                         capture_output=True, text=True, check=True).stdout
    calls = usec = 0
    for line in out.splitlines():
        if line.startswith(("cmdstat_eval:", "cmdstat_evalsha:")):
            fields = dict(part.split("=") for part in line.split(":", 1)[1].split(","))
            calls += int(fields["calls"])
            usec += int(fields["usec"])
    return calls, usec


def app_usage() -> tuple[float, int]:
    """(CPU seconds, resident KB) summed over the app instances."""
    cpu = 0.0
    rss = 0
    for pid in APP_PIDS:
        cputime, rss_kb = subprocess.run(["ps", "-o", "cputime=,rss=", "-p", pid], capture_output=True, text=True, check=True).stdout.split()
        seconds = 0.0
        for part in cputime.split(":"):
            seconds = seconds * 60 + float(part)
        cpu += seconds
        rss += int(rss_kb)
    return cpu, rss


async def snapshot() -> dict:
    (calls, usec), (cpu, rss) = await asyncio.gather(asyncio.to_thread(redis_script_stats), asyncio.to_thread(app_usage))
    http = Http(PORTS[0])
    try:
        _, audit = await http.request("GET", "/admin/audit")
    finally:
        http.close()
    return {"t": time.monotonic(), "wall_ms": now_ms(), "redis_calls": calls, "redis_usec": usec, "cpu_s": cpu, "rss_kb": rss, "waiting": audit["waiting"]}


def percentiles(samples: list[float]) -> dict:
    if len(samples) < 2:
        return {"n": len(samples)}
    cuts = statistics.quantiles(samples, n=100)
    return {"n": len(samples), "p50_ms": round(statistics.median(samples)), "p95_ms": round(cuts[94]), "p99_ms": round(cuts[98]), "max_ms": round(max(samples))}


async def run(mode: str) -> dict:
    deadline = time.monotonic() + JOIN_SPREAD_S + SETTLE_S + WINDOW_S
    tally = Tally()
    tasks = []
    for i in range(USERS):
        tasks.append(asyncio.create_task(user(i, mode, tally, deadline)))
        await asyncio.sleep(JOIN_SPREAD_S / USERS)
    await asyncio.sleep(SETTLE_S)
    start = await snapshot()
    await asyncio.sleep(WINDOW_S)
    end = await snapshot()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    span = end["t"] - start["t"]

    def in_window(times: list[float]) -> int:
        return sum(start["t"] <= t < end["t"] for t in times)

    delays = [delay for admitted_at, delay in tally.admissions if start["wall_ms"] <= admitted_at < end["wall_ms"]]
    return {
        "mode": mode,
        "users": USERS,
        "window_s": round(span, 1),
        "waiting_at_start": start["waiting"],
        "admissions_per_s": round(len(delays) / span, 1),
        "status_delay": percentiles(delays),
        "app_requests_per_s": round(in_window(tally.requests) / span),
        "events_per_s": round(in_window(tally.events) / span),
        "redis_scripts_per_s": round((end["redis_calls"] - start["redis_calls"]) / span),
        "redis_busy_percent": round((end["redis_usec"] - start["redis_usec"]) / span / 10_000, 1),
        "app_cpu_percent": round((end["cpu_s"] - start["cpu_s"]) / span * 100, 1),
        "app_rss_mb": round(end["rss_kb"] / 1024),
        "errors": tally.errors[:10],
        "error_count": len(tally.errors),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lang", required=True)
    parser.add_argument("--mode", required=True, choices=["poll", "sse"])
    args = parser.parse_args()
    if not REDIS_CONTAINER or not APP_PIDS:
        parser.error("REDIS_CONTAINER and APP_PIDS must be set (verify/run.sh does this)")

    print(f"\n=== {args.lang} / push ({args.mode}) ===")
    result = asyncio.run(run(args.mode))
    d = result["status_delay"]
    print(f"  {result['waiting_at_start']} waiting, {result['admissions_per_s']} admissions/s, delay p50={d.get('p50_ms')} p95={d.get('p95_ms')} max={d.get('max_ms')} ms")
    print(f"  app {result['app_requests_per_s']} req/s, {result['events_per_s']} events/s, CPU {result['app_cpu_percent']}%, RSS {result['app_rss_mb']} MB; "
          f"Redis {result['redis_scripts_per_s']} scripts/s, busy {result['redis_busy_percent']}%")

    failures = []
    if result["error_count"]:
        failures.append(f"{result['error_count']} errors: {result['errors']}")
    if d.get("n", 0) < 20:
        failures.append(f"too few admissions measured ({d.get('n', 0)})")
    if args.mode == "sse" and d.get("p95_ms", 0) > SSE_P95_LIMIT_MS:
        failures.append(f"SSE delay p95 {d['p95_ms']} ms > {SSE_P95_LIMIT_MS} ms")
    for failure in failures:
        print(f"  FAIL  {failure}")
    result = {"lang": args.lang, "phase": f"push-{args.mode}", "passed": 3 - len(failures), "failures": failures, **result}
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / f"{args.lang}-push-{args.mode}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"  => {result['passed']} passed, {len(failures)} failed")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
