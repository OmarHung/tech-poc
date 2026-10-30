from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .errors import CatalogError
from .models import Product, ReserveRequest
from .port import CatalogPort
from .validation import parse_id


def create_catalog_router(catalog: CatalogPort) -> APIRouter:
    """Catalog routes, identical whether hosted by the monolith or by catalog_service."""
    router = APIRouter()

    @router.get("/products")
    async def list_products() -> list[Product]:
        return await catalog.list_products()

    @router.get("/products/{product_id}")
    async def get_product(product_id: str) -> Product:
        return await catalog.get_product(parse_id(product_id, "product id"))

    @router.post("/products/{product_id}/reserve")
    async def reserve_product(product_id: str, request: ReserveRequest) -> Product:
        return await catalog.reserve(parse_id(product_id, "product id"), request.quantity)

    return router


def register_error_handlers(app: FastAPI) -> None:
    """Every error becomes `{"error": "..."}`; body validation failures are 400, not 422."""

    @app.exception_handler(CatalogError)
    async def handle_catalog_error(_: Request, exc: CatalogError) -> JSONResponse:
        return JSONResponse({"error": exc.message}, status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"])
        return JSONResponse({"error": f"{location}: {first['msg']}"}, status_code=400)


class ServedByMiddleware:
    """Tags every response with X-Served-By so callers can tell who answered.

    Plain ASGI rather than @app.middleware("http"), which adds measurable latency.
    """

    def __init__(self, app: ASGIApp, name: str) -> None:
        self._app = app
        self._header = (b"x-served-by", name.encode())

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        async def send_tagged(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), self._header]}
            await send(message)

        await self._app(scope, receive, send_tagged)
