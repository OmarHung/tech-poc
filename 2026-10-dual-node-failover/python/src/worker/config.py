from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from shared.env import EnvReader, Lookup
from shared.errors import ConfigError
from shared.wire import is_node_id


class WorkerMode(StrEnum):
    HOTHOT = "hothot"
    """Hot-hot redundancy: read every event from the upstream log and process all of them, like the other worker."""
    DISPATCH = "dispatch"
    """Dispatched worker: take one task at a time from the upstream queue, under a lease."""


class StartFrom(StrEnum):
    CHECKPOINT = "checkpoint"
    """Resume from the saved position and replay what was missed while down."""
    LATEST = "latest"
    """Jump to the newest event, skipping whatever happened while down. The naive way to rejoin; a control group."""


@dataclass(frozen=True)
class WorkerConfig:
    port: int = 8121
    worker_id: str = "a"
    mode: WorkerMode = WorkerMode.HOTHOT
    upstream_url: str = "http://127.0.0.1:8120"
    sink_url: str = "http://127.0.0.1:8123"
    data_dir: str = "data/worker-a"
    start_from: StartFrom = StartFrom.CHECKPOINT
    deterministic: bool = True
    """When false, every result carries this worker's clock, so the two hot-hot workers never agree. Shows why
    processing must be deterministic."""
    lease_check: bool = True
    """When false, a dispatched worker writes its result even after losing the lease. Control group for the
    frozen-worker case."""
    work_ms: int = 300
    """How long one dispatched task takes."""
    rpc_timeout_ms: int = 1000
    """Timeout of every call to the upstream or the sink, on top of any long-poll wait."""
    poll_wait_ms: int = 500
    """How long a read of the event log or a task lease request waits for new work."""
    retry_delay_ms: int = 100

    def __post_init__(self) -> None:
        if not is_node_id(self.worker_id):
            raise ConfigError(f"WORKER_ID must be 1-32 lowercase letters, digits or '-', got '{self.worker_id}'")

    @property
    def work_time(self) -> float:
        return self.work_ms / 1000

    @property
    def rpc_timeout(self) -> float:
        return self.rpc_timeout_ms / 1000

    @property
    def poll_wait(self) -> float:
        return self.poll_wait_ms / 1000

    @property
    def retry_delay(self) -> float:
        return self.retry_delay_ms / 1000

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> WorkerConfig:
        source = os.environ if environ is None else environ
        return cls.from_lookup(source.get)

    @classmethod
    def from_lookup(cls, lookup: Lookup) -> WorkerConfig:
        env = EnvReader(lookup)
        d = cls()
        worker_id = env.string("WORKER_ID", d.worker_id)
        return cls(
            port=env.port("PORT", d.port),
            worker_id=worker_id,
            mode=env.choice("MODE", d.mode, {mode.value: mode for mode in WorkerMode}),
            upstream_url=env.url("UPSTREAM_URL", d.upstream_url),
            sink_url=env.url("SINK_URL", d.sink_url),
            data_dir=env.string("DATA_DIR", f"data/worker-{worker_id}"),
            start_from=env.choice("START_FROM", d.start_from, {start.value: start for start in StartFrom}),
            deterministic=env.choice("DETERMINISTIC", d.deterministic, {"true": True, "false": False}),
            lease_check=env.choice("LEASE_CHECK", d.lease_check, {"on": True, "off": False}),
            work_ms=env.integer("WORK_MS", d.work_ms, 0, 60_000),
            rpc_timeout_ms=env.integer("RPC_TIMEOUT_MS", d.rpc_timeout_ms, 10, 30_000),
        )
