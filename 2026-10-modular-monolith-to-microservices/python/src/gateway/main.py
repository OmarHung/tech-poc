from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from gateway.routing import CatalogRoute, RoutingTable, Upstreams, parse_route

UPSTREAM_TIMEOUT_S = 5.0
PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
# Host and Content-Length are recomputed on each side; Content-Encoding because httpx decodes bodies.
DROPPED_HEADERS = HOP_BY_HOP | {"host", "content-length", "content-encoding"}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GatewayConfig:
    upstreams: Upstreams
    initial_route: CatalogRoute = "monolith"

    @classmethod
    def from_env(cls) -> GatewayConfig:
        return cls(
            upstreams=Upstreams(
                monolith=os.environ.get("MONOLITH_URL", "http://127.0.0.1:8081"),
                catalog=os.environ.get("CATALOG_URL", "http://127.0.0.1:8082"),
            ),
            initial_route=parse_route(os.environ.get("CATALOG_ROUTE", "monolith")),
        )


def own_response(body: object, status_code: int = 200) -> JSONResponse:
    """Gateway-owned responses are tagged so callers can tell them from proxied ones."""
    return JSONResponse(body, status_code=status_code, headers={"X-Served-By": "gateway"})


def forwardable(headers: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(name, value) for name, value in headers if name.lower() not in DROPPED_HEADERS]


def create_app(config: GatewayConfig | None = None, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    settings = config or GatewayConfig.from_env()
    routing = RoutingTable(settings.upstreams, settings.initial_route)
    client = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT_S, follow_redirects=False, transport=transport)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await client.aclose()

    app = FastAPI(title="gateway", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/health")
    async def health() -> JSONResponse:
        return own_response({"status": "ok", "service": "gateway", "routes": {"catalog": routing.catalog_route}})

    @app.get("/admin/routes")
    async def get_routes() -> JSONResponse:
        return own_response({"catalog": routing.catalog_route})

    # No auth: the PoC binds to 127.0.0.1 only. A real gateway must protect this.
    @app.put("/admin/routes")
    async def put_routes(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except ValueError:
            return own_response({"error": "request body must be valid JSON"}, 400)
        try:
            route = parse_route(payload.get("catalog") if isinstance(payload, dict) else None)
        except ValueError as exc:
            return own_response({"error": str(exc)}, 400)
        routing.switch(route)
        return own_response({"catalog": route})

    @app.api_route("/{path:path}", methods=PROXY_METHODS)
    async def proxy(request: Request) -> Response:
        upstream = routing.resolve(request.url.path)
        query = f"?{request.url.query}" if request.url.query else ""
        try:
            upstream_response = await client.request(
                request.method,
                f"{upstream}{request.url.path}{query}",
                content=await request.body(),
                headers=forwardable(request.headers.items()),
            )
        except httpx.HTTPError as exc:
            logger.warning("upstream %s failed: %r", upstream, exc)
            return own_response({"error": "upstream unavailable"}, 502)
        return Response(
            content=upstream_response.content,
            status_code=upstream_response.status_code,
            headers=dict(forwardable(upstream_response.headers.items())),
        )

    return app
