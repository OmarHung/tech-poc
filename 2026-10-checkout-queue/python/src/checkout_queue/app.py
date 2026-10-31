"""Assembles the FastAPI app for one sale: routes, error handling, and the two background loops."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from redis.asyncio import Redis

from .api import create_api_router
from .clock import Clock
from .config import Config
from .demo import create_demo_router
from .events import create_events_router
from .maintenance import run_maintenance_loop
from .services import Services
from .web import ServedByMiddleware, UnhandledErrorMiddleware, install_error_handlers

log = logging.getLogger(__name__)

# Called when a background loop dies of a bug; production stops the server.
OnFatal = Callable[[BaseException], None]


def create_app(
    config: Config,
    redis: Redis,
    clock: Clock,
    *,
    run_maintenance: bool = True,
    on_fatal: OnFatal | None = None,
) -> FastAPI:
    """`run_maintenance=False` lets tests turn the background tick off and drive
    `app.state.services.maintenance` themselves."""
    services = Services.build(config, redis, clock)
    report_fatal = on_fatal or _log_only

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        tasks: list[asyncio.Task[None]] = []

        def spawn(name: str, work: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
            task = asyncio.create_task(work, name=name)
            task.add_done_callback(lambda done: _check_crash(name, done, report_fatal))
            tasks.append(task)
            return task

        # Subscribe to admissions before taking requests, then start both loops.
        await services.push.start(spawn)
        if run_maintenance:
            spawn("maintenance", run_maintenance_loop(services.maintenance, clock, config.tick_ms, config.sale_id))
        try:
            yield
        finally:
            await services.push.stop()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(
        title="checkout-queue",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        redirect_slashes=False,
    )
    app.state.services = services
    install_error_handlers(app)
    app.add_middleware(UnhandledErrorMiddleware)
    app.add_middleware(ServedByMiddleware, name=config.instance_name)  # added last = outermost
    app.include_router(create_api_router(services))
    app.include_router(create_events_router(services))
    app.include_router(create_demo_router())
    return app


def _check_crash(name: str, task: asyncio.Task[None], on_fatal: OnFatal) -> None:
    if task.cancelled() or task.exception() is None:
        return
    error = task.exception()
    assert error is not None
    log.critical("Background task '%s' crashed; stopping", name, exc_info=error)
    on_fatal(error)


def _log_only(_: BaseException) -> None:
    """Without a server to stop (tests that build the app by hand), logging is all there is."""
