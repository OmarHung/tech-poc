"""The witness process. `create_app` is the uvicorn factory."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.clock import Clock, MonotonicClock
from shared.errors import ConfigError
from shared.http import ServedBy, install_error_handlers, read_body
from shared.logs import configure_logging
from shared.witness_contracts import LeaseRequest, SyncRequest
from witness.config import WitnessConfig
from witness.lease_table import LeaseTable

logger = logging.getLogger(__name__)


def create_app(config: WitnessConfig | None = None, clock: Clock | None = None) -> FastAPI:
    if config is None:
        configure_logging()
        try:
            config = WitnessConfig.from_env()
        except ConfigError as ex:
            raise SystemExit(f"witness: {ex}") from ex
    leases = LeaseTable(config, clock or MonotonicClock())

    app = FastAPI(title="witness", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.leases = leases
    install_error_handlers(app)
    app.add_middleware(ServedBy, name="witness")

    # No auth: the PoC binds to 127.0.0.1 only. A real witness must only accept the two nodes (mTLS or similar).
    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(leases.health())

    @app.post("/lease")
    async def lease(request: Request) -> JSONResponse:
        lease_request = await read_body(request, LeaseRequest.from_wire)
        return JSONResponse(leases.acquire(lease_request.node).to_wire())

    @app.post("/sync")
    async def sync(request: Request) -> JSONResponse:
        sync_request = await read_body(request, SyncRequest.from_wire)
        return JSONResponse(leases.set_standby_in_sync(sync_request).to_wire())

    logger.info(
        "Witness for nodes %s on http://127.0.0.1:%d, lease %d ms",
        ", ".join(config.nodes),
        config.port,
        config.lease_ttl_ms,
    )
    return app
