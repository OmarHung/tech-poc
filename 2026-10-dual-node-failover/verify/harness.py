"""Plumbing shared by every scenario family: HTTP, waiting, timing, checks and process control.

Standard library only, like the rest of verify/.
"""

from __future__ import annotations

import http.client
import json
import os
import re
import signal
import socket
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "verify" / "results"
LOG_DIR = ROOT / "verify" / "logs"

# VERIFY_PORT_BASE moves every port, so two implementations can be verified at the same time.
PORT_BASE = int(os.environ.get("VERIFY_PORT_BASE", "8100"))

START_TIMEOUT_S = 60.0
CLIENT_TIMEOUT_S = 5.0
STOP_TIMEOUT_S = 10.0


class ScenarioError(Exception):
    pass


# --------------------------------------------------------------------------- HTTP


@dataclass(frozen=True)
class Reply:
    status: int  # 0 when no HTTP response came back (refused, reset, timed out)
    body: object
    served_by: str | None


class Client:
    """Keep-alive HTTP client for one local port."""

    def __init__(self, port: int, timeout: float = CLIENT_TIMEOUT_S) -> None:
        self._port = port
        self._timeout = timeout
        self._conn: http.client.HTTPConnection | None = None

    def request(self, method: str, path: str, body: object = None) -> Reply:
        payload = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        conn = self._conn or http.client.HTTPConnection("127.0.0.1", self._port, timeout=self._timeout)
        self._conn = conn
        try:
            conn.request(method, path, body=payload, headers=headers)
            response = conn.getresponse()
            data = response.read()
        except (OSError, http.client.HTTPException) as ex:
            self.close()
            return Reply(0, str(ex), None)
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


def get_json(port: int, path: str, timeout: float = 1.0) -> dict | None:
    reply = Client(port, timeout).request("GET", path)
    return reply.body if reply.status == 200 and isinstance(reply.body, dict) else None


def port_in_use(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------------- time


def wait_until(condition: Callable[[], object], timeout: float, what: str) -> float:
    started = time.monotonic()
    while not condition():
        if time.monotonic() - started > timeout:
            raise ScenarioError(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(0.05)
    return time.monotonic() - started


class Clock:
    def __init__(self) -> None:
        self._t0 = time.monotonic()

    def now(self) -> float:
        return time.monotonic() - self._t0


# --------------------------------------------------------------------------- scenarios


@dataclass
class Scenario:
    name: str
    title: str
    run: Callable[..., None]
    env: dict[str, str] = field(default_factory=dict)
    node_env: dict[str, dict[str, str]] = field(default_factory=dict)
    link_delay_ms: int = 0  # one-way latency added between the nodes, e.g. two racks or two rooms apart
    family: str = "replication"


# Which run.sh components a family needs. An implementation runs a family only if its run.sh usage line lists them.
FAMILY_COMPONENTS = {
    "replication": ("node", "witness", "gateway"),
    "independent": ("upstream", "worker", "sink"),
}


def supported_families(impl: str) -> set[str]:
    usage = re.search(r"usage: \$0 ([\w|]+)", (ROOT / impl / "run.sh").read_text())
    commands = set(usage.group(1).split("|")) if usage else set()
    return {family for family, needed in FAMILY_COMPONENTS.items() if commands.issuperset(needed)}


class Checks:
    """Pass/fail checks and metrics for one scenario run."""

    def __init__(self, impl: str, scenario: Scenario) -> None:
        self.impl = impl
        self.scenario = scenario
        self.clock = Clock()
        self.passed = 0
        self.failures: list[str] = []
        self.metrics: dict[str, object] = {}

    def now(self) -> float:
        return self.clock.now()

    def check(self, name: str, condition: bool, detail: object = "") -> None:
        if condition:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failures.append(f"{name} ({detail})")
            print(f"  FAIL  {name}  -> {detail}")

    def metric(self, name: str, value: object) -> None:
        self.metrics[name] = value
        print(f"  ....  {name} = {value}")


# --------------------------------------------------------------------------- processes


class ProcessGroup:
    """Processes started through one implementation's run.sh, each logging to verify/logs/<impl>-<scenario>-<name>.log."""

    def __init__(self, impl: str, scenario: str) -> None:
        self._run_sh = ROOT / impl / "run.sh"
        self._log_prefix = f"{impl}-{scenario}"
        self._procs: dict[str, subprocess.Popen] = {}

    def start(self, name: str, component: str, env: dict[str, str], health_port: int) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_DIR / f"{self._log_prefix}-{name}.log", "ab") as log:
            self._procs[name] = subprocess.Popen(
                [str(self._run_sh), component],
                env={**os.environ, **env},
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        wait_until(lambda: get_json(health_port, "/health") is not None, START_TIMEOUT_S, f"{name} to start")

    def kill(self, name: str) -> None:
        """SIGKILL: no shutdown hooks, no flushing. The closest a process gets to pulling the plug."""
        proc = self._procs.pop(name)
        proc.send_signal(signal.SIGKILL)
        proc.wait()

    def pause(self, name: str) -> None:
        """SIGSTOP: the process freezes mid-instruction, like a long GC pause or a suspended VM."""
        self._procs[name].send_signal(signal.SIGSTOP)

    def resume(self, name: str) -> None:
        self._procs[name].send_signal(signal.SIGCONT)

    def stop(self, name: str) -> None:
        proc = self._procs.pop(name)
        proc.send_signal(signal.SIGCONT)  # a stopped process would not act on SIGTERM
        proc.terminate()
        try:
            proc.wait(timeout=STOP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def stop_all(self) -> None:
        for name in list(self._procs):
            self.stop(name)
