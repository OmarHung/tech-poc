from fastapi import FastAPI

from catalog_service.catalog import InMemoryCatalog, ServedByMiddleware, create_catalog_router, register_error_handlers


def create_app() -> FastAPI:
    app = FastAPI(title="catalog-service")
    register_error_handlers(app)
    app.add_middleware(ServedByMiddleware, name="catalog-service")
    app.include_router(create_catalog_router(InMemoryCatalog.seeded()))

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "catalog-service"}

    return app
