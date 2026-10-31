"""The node process: HTTP API plus the election and replication loops. `create_app` is the uvicorn factory."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from node.cluster.contracts import ReplicateRequest
from node.cluster.election import ElectionWorker
from node.cluster.node_state import NodeState
from node.cluster.primary_term import Arbiter
from node.cluster.replication import ReplicationWorker
from node.cluster.rpc import RpcClient
from node.cluster.witness_client import NoArbiter, WitnessClient
from node.config import ArbiterMode, NodeConfig
from node.kv import KvService, parse_value, validate_key
from node.storage.replicated_log import ReplicatedLog
from shared import tasks
from shared.clock import Clock, MonotonicClock
from shared.errors import ConfigError
from shared.http import ServedBy, install_error_handlers, read_body
from shared.logs import configure_logging

LOG_FILE_NAME = "log.jsonl"

logger = logging.getLogger(__name__)


class NodeRuntime:
    """Everything one node holds. Tests build one and start or stop its loops themselves."""

    def __init__(
        self,
        config: NodeConfig,
        clock: Clock | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        on_fatal: tasks.FatalHandler = tasks.stop_process,
    ) -> None:
        self.config = config
        self.log = ReplicatedLog.open(Path(config.data_dir) / LOG_FILE_NAME)
        self.rpc = RpcClient(config.renew_interval, transport)
        self.witness = WitnessClient(self.rpc, config)
        arbiter: Arbiter = self.witness if config.arbiter is ArbiterMode.WITNESS else NoArbiter()
        self.state = NodeState(config, self.log, arbiter, clock or MonotonicClock())
        self.kv = KvService(self.state, config)
        self.election = ElectionWorker(self.state, self.witness, config)
        self.replication = ReplicationWorker(self.state, self.rpc, config)
        self._on_fatal = on_fatal
        self._workers: list[asyncio.Task[None]] = []

    @property
    def workers(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(self._workers)

    async def start(self) -> None:
        self._workers = [
            tasks.spawn(tasks.supervise("election loop", self.election.run, self._on_fatal), "election"),
            tasks.spawn(tasks.supervise("replication loop", self.replication.run, self._on_fatal), "replication"),
        ]

    async def stop(self) -> None:
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        self.state.shut_down()  # ends the term and cancels its tasks; nothing is acknowledged after this
        await self.rpc.aclose()
        self.log.close()


def build_app(runtime: NodeRuntime, run_workers: bool = True) -> FastAPI:
    state, kv = runtime.state, runtime.kv

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if run_workers:
            await runtime.start()
        yield
        await runtime.stop()

    app = FastAPI(title="node", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.runtime = runtime
    install_error_handlers(app)
    app.add_middleware(ServedBy, name=f"node-{runtime.config.node_id}")

    # Every endpoint is `async def`: a plain `def` would run in a thread pool and race the event loop.
    # No auth anywhere: the PoC binds to 127.0.0.1. A real deployment must at least keep /internal and /admin off the
    # client network and authenticate the peer (mTLS or similar).
    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(state.health().to_wire())

    @app.put("/kv/{key}")
    async def put_value(key: str, request: Request) -> JSONResponse:
        validate_key(key)
        value = await read_body(request, parse_value)
        return JSONResponse((await kv.put(key, value)).to_wire())

    @app.get("/kv/{key}")
    async def get_value(key: str) -> JSONResponse:
        validate_key(key)
        return JSONResponse((await kv.get(key)).to_wire())

    @app.post("/internal/replicate")
    async def replicate(request: Request) -> JSONResponse:
        replicate_request = await read_body(request, ReplicateRequest.from_wire)
        return JSONResponse(state.handle_replicate(replicate_request).to_wire())

    # Everything this node holds, primary or not. The verify script compares the two nodes with it.
    @app.get("/admin/data")
    async def admin_data() -> JSONResponse:
        health = state.health()
        return JSONResponse(
            {
                "node": health.node,
                "role": health.role,
                "epoch": health.epoch,
                "lastIndex": health.last_index,
                "data": state.log.snapshot(),
            }
        )

    return app


def create_app(
    config: NodeConfig | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    run_workers: bool = True,
    on_fatal: tasks.FatalHandler = tasks.stop_process,
) -> FastAPI:
    if config is None:
        configure_logging()
        try:
            config = NodeConfig.from_env()
        except ConfigError as ex:
            raise SystemExit(f"node: {ex}") from ex
    app = build_app(NodeRuntime(config, transport=transport, on_fatal=on_fatal), run_workers)
    logger.info(
        "Node %s on http://127.0.0.1:%d: arbiter %s, replication %s, peer %s, data %s",
        config.node_id,
        config.port,
        config.arbiter.value,
        config.replication.value,
        config.peer_url,
        Path(config.data_dir).resolve(),
    )
    return app
