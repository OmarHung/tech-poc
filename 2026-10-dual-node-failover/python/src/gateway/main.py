"""The gateway process. `create_app` is the uvicorn factory."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from gateway.config import UPSTREAM_TIMEOUT_S, GatewayConfig
from gateway.tracker import PrimaryTracker
from shared import tasks
from shared.errors import ConfigError, PayloadTooLarge
from shared.http import SERVED_BY, error_response, install_error_handlers, read_capped_body
from shared.logs import configure_logging

PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
HOP_BY_HOP = frozenset(
    {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding"}
    | {"upgrade"}
)
# Host and Content-Length are recomputed on each side; uvicorn adds its own Date and Server.
DROPPED_HEADERS = HOP_BY_HOP | {"host", "content-length", "date", "server"}
# httpx hands back the body already decoded, so an upstream Content-Encoding would no longer be true.
DROPPED_RESPONSE_HEADERS = DROPPED_HEADERS | {"content-encoding"}
OWN = {SERVED_BY: "gateway"}
UPSTREAM_LIMITS = httpx.Limits(max_connections=None, max_keepalive_connections=100)

logger = logging.getLogger(__name__)


def forwardable(headers: Iterable[tuple[str, str]], dropped: frozenset[str] = DROPPED_HEADERS) -> list[tuple[str, str]]:
    return [(name, value) for name, value in headers if name.lower() not in dropped]


def create_app(
    config: GatewayConfig | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    on_fatal: tasks.FatalHandler = tasks.stop_process,
) -> FastAPI:
    if config is None:
        configure_logging()
        try:
            config = GatewayConfig.from_env()
        except ConfigError as ex:
            raise SystemExit(f"gateway: {ex}") from ex
    # Health polls and forwarded requests share one connection pool (and, in tests, one fake transport). No cap on
    # connections, like .NET's handler: with httpx's default of 100, slow writes could starve the health polls and
    # make the gateway think there is no primary.
    client = httpx.AsyncClient(
        timeout=UPSTREAM_TIMEOUT_S, follow_redirects=False, trust_env=False, transport=transport, limits=UPSTREAM_LIMITS
    )
    tracker = PrimaryTracker(config, client)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        polling = tasks.spawn(tasks.supervise("primary tracker", tracker.run, on_fatal), "tracker")
        yield
        polling.cancel()
        await asyncio.gather(polling, return_exceptions=True)
        await client.aclose()

    app = FastAPI(title="gateway", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.tracker = tracker
    install_error_handlers(app)

    # Gateway-owned responses are tagged so callers can tell them from forwarded ones.
    @app.get("/health")
    async def health() -> JSONResponse:
        current = tracker.current
        body = {
            "status": "ok",
            "service": "gateway",
            "primary": None if current is None else current.target.id,
            "epoch": None if current is None else current.epoch,
        }
        return JSONResponse(body, headers=OWN)

    @app.api_route("/{path:path}", methods=PROXY_METHODS)
    async def forward(request: Request) -> Response:
        primary = tracker.current
        if primary is None:
            return error_response(503, "no primary available", OWN)
        # raw_path keeps the client's percent-encoding, as a reverse proxy should (some servers include the query).
        path = request.scope.get("raw_path", request.url.path.encode()).decode("latin-1").split("?", 1)[0]
        query = f"?{request.url.query}" if request.url.query else ""
        try:
            body = await read_capped_body(request)
        except PayloadTooLarge as ex:
            return error_response(ex.status_code, ex.message, OWN)
        try:
            upstream = await client.request(
                request.method,
                primary.target.url.rstrip("/") + path + query,
                content=body,
                headers=forwardable(request.headers.items()),
            )
        except httpx.HTTPError as ex:
            tracker.nudge()
            logger.debug("Forwarding to %s failed: %r", primary.target.id, ex)
            return error_response(502, "primary unavailable", OWN)
        if upstream.status_code == 503:
            tracker.nudge()  # the node says it is not primary (any more)
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=dict(forwardable(upstream.headers.items(), DROPPED_RESPONSE_HEADERS)),
        )

    logger.info(
        "Gateway on http://127.0.0.1:%d for %s, polling every %d ms",
        config.port,
        ", ".join(f"{node.id}={node.url}" for node in config.nodes),
        config.poll_interval_ms,
    )
    return app
