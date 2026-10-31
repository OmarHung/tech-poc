"""The sink process. `create_app` is the uvicorn factory."""

from __future__ import annotations

import logging
from collections.abc import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.clock import unix_ms
from shared.errors import ConfigError
from shared.http import ServedBy, install_error_handlers, read_body
from shared.logs import configure_logging
from shared.work_contracts import ResultRecord, ResultReply
from sink.config import SinkConfig
from sink.result_store import ResultStatus, ResultStore

STATUS_CODES = {ResultStatus.APPLIED: 201, ResultStatus.DUPLICATE: 200}
"""Conflicts and stale attempts are 409: the result was not taken."""

logger = logging.getLogger(__name__)


def create_app(config: SinkConfig | None = None, wall_clock_ms: Callable[[], int] = unix_ms) -> FastAPI:
    if config is None:
        configure_logging()
        try:
            config = SinkConfig.from_env()
        except ConfigError as ex:
            raise SystemExit(f"sink: {ex}") from ex
    store = ResultStore(wall_clock_ms)

    app = FastAPI(title="sink", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.results = store
    install_error_handlers(app)
    app.add_middleware(ServedBy, name="sink")

    # No auth: the PoC binds to 127.0.0.1 only.
    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "sink", "summary": store.summary.to_wire()})

    @app.post("/results")
    async def record(request: Request) -> JSONResponse:
        status = store.record(await read_body(request, ResultRecord.from_wire))
        return JSONResponse(ResultReply(status.value).to_wire(), status_code=STATUS_CODES.get(status, 409))

    @app.get("/results")
    async def results() -> JSONResponse:
        return JSONResponse(store.snapshot())

    logger.info("Sink on http://127.0.0.1:%d", config.port)
    return app
