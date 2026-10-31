"""The upstream process. `create_app` is the uvicorn factory."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from shared import tasks
from shared.clock import Clock, MonotonicClock, PeriodicTimer
from shared.errors import ConfigError, NotFound
from shared.http import (
    ServedBy,
    error_response,
    install_error_handlers,
    parse_integer,
    query_integer,
    read_body,
    until_disconnected,
)
from shared.logs import configure_logging
from shared.wire import INT32_MAX, INT32_MIN
from shared.work_contracts import NewEvent, NewTask, TaskAttempt, TaskLeaseRequest
from upstream.config import UpstreamConfig
from upstream.event_log import EventLog
from upstream.task_queue import TaskQueue

MAX_WAIT_S = 10.0
DEFAULT_PAGE = 100

logger = logging.getLogger(__name__)


def wait_seconds(wait_ms: int) -> float:
    """A caller's long-poll wait, kept between 0 and 10 s."""
    return min(max(wait_ms, 0) / 1000, MAX_WAIT_S)


def task_id(raw: str) -> int:
    """The {id} route segment. Not a number means no such route, as with ASP.NET's {id:long} constraint."""
    value = parse_integer(raw)
    if value is None:
        raise NotFound("Not Found")
    return value


async def reap(queue: TaskQueue, interval: float) -> None:
    """Requeues expired task leases even when no worker is asking, so a waiting worker is woken right away."""
    timer = PeriodicTimer(interval)
    while True:
        await timer.tick()
        queue.expire_overdue()


def create_app(
    config: UpstreamConfig | None = None,
    clock: Clock | None = None,
    run_reaper: bool = True,
    on_fatal: tasks.FatalHandler = tasks.stop_process,
) -> FastAPI:
    """run_reaper: tests turn the expiry timer off and call TaskQueue.expire_overdue themselves."""
    if config is None:
        configure_logging()
        try:
            config = UpstreamConfig.from_env()
        except ConfigError as ex:
            raise SystemExit(f"upstream: {ex}") from ex
    events = EventLog()
    queue = TaskQueue(config, clock or MonotonicClock())
    reap_interval = config.reap_interval

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        reaper = None
        if run_reaper:
            reaping = tasks.supervise("lease reaper", lambda: reap(queue, reap_interval), on_fatal)
            reaper = tasks.spawn(reaping, "reaper")
        yield
        if reaper is not None:
            reaper.cancel()
            await asyncio.gather(reaper, return_exceptions=True)

    app = FastAPI(title="upstream", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.events = events
    app.state.tasks = queue
    install_error_handlers(app)
    app.add_middleware(ServedBy, name="upstream")

    # No auth: the PoC binds to 127.0.0.1 only.
    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(
            {"status": "ok", "service": "upstream", "lastEvent": events.last, "tasks": queue.stats().to_wire()}
        )

    @app.post("/events")
    async def append_event(request: Request) -> JSONResponse:
        new_event = await read_body(request, NewEvent.from_wire)
        return JSONResponse(events.append(new_event).to_wire(), status_code=201)

    @app.get("/events")
    async def read_events(request: Request) -> JSONResponse:
        after = query_integer(request, "after", 0)
        limit = query_integer(request, "limit", DEFAULT_PAGE, INT32_MIN, INT32_MAX)
        wait_ms = query_integer(request, "waitMs", 0, INT32_MIN, INT32_MAX)
        return JSONResponse((await events.read(after, limit, wait_seconds(wait_ms))).to_wire())

    @app.post("/tasks")
    async def add_task(request: Request) -> JSONResponse:
        new_task = await read_body(request, NewTask.from_wire)
        return JSONResponse({"id": queue.add(new_task.input)}, status_code=201)

    @app.post("/tasks/lease")
    async def lease_task(request: Request) -> Response:
        lease_request = await read_body(request, TaskLeaseRequest.from_wire)
        wait = wait_seconds(lease_request.wait_ms)
        lease = await until_disconnected(request, queue.lease(lease_request.worker, wait))
        return Response(status_code=204) if lease is None else JSONResponse(lease.to_wire())

    @app.post("/tasks/{id}/heartbeat")
    async def heartbeat(id: str, request: Request) -> Response:
        number = task_id(id)
        attempt = await read_body(request, TaskAttempt.from_wire)
        if queue.heartbeat(number, attempt):
            return Response(status_code=200)
        return error_response(409, "lease lost")

    @app.post("/tasks/{id}/complete")
    async def complete(id: str, request: Request) -> Response:
        number = task_id(id)
        attempt = await read_body(request, TaskAttempt.from_wire)
        if queue.complete(number, attempt):
            return Response(status_code=200)
        return error_response(409, "lease lost; another attempt owns this task")

    @app.get("/tasks/stats")
    async def stats() -> JSONResponse:
        return JSONResponse(queue.stats().to_wire())

    logger.info("Upstream on http://127.0.0.1:%d, task lease %d ms", config.port, config.task_lease_ttl_ms)
    return app
