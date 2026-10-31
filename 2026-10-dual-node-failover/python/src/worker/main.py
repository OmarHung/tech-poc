"""The worker process: a small HTTP API plus one work loop. `create_app` is the uvicorn factory."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from shared import tasks
from shared.clock import Clock, MonotonicClock
from shared.errors import ConfigError
from shared.http import ServedBy, install_error_handlers
from shared.logs import configure_logging
from worker.client import WorkClient
from worker.config import WorkerConfig, WorkerMode
from worker.dispatch import DispatchWorker
from worker.hothot import HotHotWorker
from worker.status import WorkerStatus

logger = logging.getLogger(__name__)


class WorkerRuntime:
    """Everything one worker holds. Tests build one and start or stop its loop themselves."""

    def __init__(
        self,
        config: WorkerConfig,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Clock | None = None,
        on_fatal: tasks.FatalHandler = tasks.stop_process,
    ) -> None:
        self.config = config
        self.client = WorkClient(config, transport)
        self.status = WorkerStatus()
        self.loop: HotHotWorker | DispatchWorker = (
            HotHotWorker(self.client, self.status, config)
            if config.mode is WorkerMode.HOTHOT
            else DispatchWorker(self.client, self.status, config, clock or MonotonicClock())
        )
        self._on_fatal = on_fatal
        self._running: asyncio.Task[None] | None = None

    @property
    def running(self) -> asyncio.Task[None] | None:
        return self._running

    async def start(self) -> None:
        name = f"{self.config.mode.value} loop"
        self._running = tasks.spawn(tasks.supervise(name, self.loop.run, self._on_fatal), "work")

    async def stop(self) -> None:
        if self._running is not None:
            self._running.cancel()
            await asyncio.gather(self._running, return_exceptions=True)
            self._running = None
        await self.client.aclose()


def build_app(runtime: WorkerRuntime, run_loop: bool = True) -> FastAPI:
    config, status = runtime.config, runtime.status

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if run_loop:
            await runtime.start()
        yield
        await runtime.stop()

    app = FastAPI(title="worker", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.runtime = runtime
    install_error_handlers(app)
    app.add_middleware(ServedBy, name=f"worker-{config.worker_id}")

    # No auth: the PoC binds to 127.0.0.1 only.
    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(status.health(config))

    # The hot-hot ledger, so the verify script can check a restarted worker caught up to the same balances.
    @app.get("/state")
    async def state() -> JSONResponse:
        return JSONResponse(status.ledger.to_file())

    return app


def create_app(
    config: WorkerConfig | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    run_loop: bool = True,
    on_fatal: tasks.FatalHandler = tasks.stop_process,
) -> FastAPI:
    if config is None:
        configure_logging()
        try:
            config = WorkerConfig.from_env()
        except ConfigError as ex:
            raise SystemExit(f"worker: {ex}") from ex
    app = build_app(WorkerRuntime(config, transport, on_fatal=on_fatal), run_loop)
    logger.info(
        "Worker %s (%s) on http://127.0.0.1:%d, upstream %s, sink %s, data %s",
        config.worker_id,
        config.mode.value,
        config.port,
        config.upstream_url,
        config.sink_url,
        Path(config.data_dir).resolve(),
    )
    return app
