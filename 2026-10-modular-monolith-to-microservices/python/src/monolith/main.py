from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from monolith.config import MonolithConfig
from monolith.modules.catalog import (
    REQUEST_TIMEOUT_S,
    CatalogPort,
    HttpCatalogClient,
    InMemoryCatalog,
    ServedByMiddleware,
    create_catalog_router,
    register_error_handlers,
)
from monolith.modules.orders import OrderService, create_orders_router


def create_app(config: MonolithConfig | None = None) -> FastAPI:
    """Catalog routes and orders both go through the same CatalogPort, so in remote
    mode the monolith delegates everything catalog-related to catalog-service and
    never holds a second copy of the data."""
    settings = config or MonolithConfig.from_env()
    http = (
        httpx.AsyncClient(base_url=settings.catalog_url, timeout=REQUEST_TIMEOUT_S)
        if settings.catalog_mode == "remote"
        else None
    )
    catalog: CatalogPort = HttpCatalogClient(http) if http else InMemoryCatalog.seeded()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if http is not None:
            await http.aclose()

    app = FastAPI(title="monolith", lifespan=lifespan)
    register_error_handlers(app)
    app.add_middleware(ServedByMiddleware, name="monolith")
    app.include_router(create_catalog_router(catalog))
    app.include_router(create_orders_router(OrderService(catalog)))

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "monolith", "catalogMode": settings.catalog_mode}

    return app
