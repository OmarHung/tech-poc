"""CatalogPort over HTTP: what the monolith uses once the catalog is extracted."""

from __future__ import annotations

import logging
from collections.abc import Callable

import httpx
from pydantic import BaseModel, TypeAdapter, ValidationError

from .errors import CatalogError, CatalogUnavailable, InsufficientStock, InvalidInput, NotFound
from .models import Product

REQUEST_TIMEOUT_S = 2.0

logger = logging.getLogger(__name__)

_PRODUCT_LIST = TypeAdapter(list[Product])
_ERRORS_BY_STATUS: dict[int, type[CatalogError]] = {
    400: InvalidInput,
    404: NotFound,
    409: InsufficientStock,
}


class HttpCatalogClient:
    """Raises the same CatalogError subclasses as the in-process catalog, so callers
    cannot tell the difference. Network details are logged, never returned."""

    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def list_products(self) -> list[Product]:
        response = await self._send("GET", "/products")
        return _parse(lambda: _PRODUCT_LIST.validate_json(response.content))

    async def get_product(self, product_id: int) -> Product:
        response = await self._send("GET", f"/products/{product_id}")
        return _parse(lambda: Product.model_validate_json(response.content))

    async def reserve(self, product_id: int, quantity: int) -> Product:
        response = await self._send("POST", f"/products/{product_id}/reserve", {"quantity": quantity})
        return _parse(lambda: Product.model_validate_json(response.content))

    async def _send(self, method: str, path: str, body: dict[str, int] | None = None) -> httpx.Response:
        try:
            response = await self._http.request(method, path, json=body)
        except httpx.HTTPError as exc:
            logger.warning("catalog-service request failed: %r", exc)
            raise CatalogUnavailable("catalog unavailable") from exc
        if response.is_success:
            return response
        raise _error_from(response)


def _parse[T](decode: Callable[[], T]) -> T:
    try:
        return decode()
    except ValidationError as exc:
        logger.warning("catalog-service sent an unexpected body: %s", exc)
        raise CatalogUnavailable("catalog unavailable") from exc


class _ErrorBody(BaseModel):
    error: str


def _error_from(response: httpx.Response) -> CatalogError:
    try:
        message = _ErrorBody.model_validate_json(response.content).error
    except ValidationError:
        message = f"catalog-service returned {response.status_code}"
    error_type = _ERRORS_BY_STATUS.get(response.status_code)
    if error_type is None:
        logger.warning("catalog-service error %s: %s", response.status_code, message)
        return CatalogUnavailable("catalog unavailable")
    return error_type(message)
