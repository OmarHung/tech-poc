"""Catalog module — MIGRATED to catalog-service (已轉微服務).

The data and business logic now live in catalog_service. This copy stays in its
original place so the before/after is easy to compare, and so CATALOG_MODE=inprocess
can roll back. Still in use after the migration:

- CatalogPort: the interface the orders module depends on
- HttpCatalogClient: added by the migration, calls catalog-service
- create_catalog_router: the /products routes, which now delegate through the HTTP client

Everything else (InMemoryCatalog, seed data) only runs in inprocess mode and can be
deleted once rollback is no longer needed. See README.md in this folder.
"""

from .api import ServedByMiddleware, create_catalog_router, register_error_handlers
from .errors import CatalogError, CatalogUnavailable, InsufficientStock, InvalidInput, NotFound
from .models import ApiModel, Product, ReserveRequest, seed_products
from .port import CatalogPort
from .remote import REQUEST_TIMEOUT_S, HttpCatalogClient
from .store import InMemoryCatalog
from .validation import MAX_QUANTITY, parse_id, validate_id, validate_quantity

__all__ = [
    "MAX_QUANTITY",
    "REQUEST_TIMEOUT_S",
    "ApiModel",
    "CatalogError",
    "CatalogPort",
    "CatalogUnavailable",
    "HttpCatalogClient",
    "InMemoryCatalog",
    "InsufficientStock",
    "InvalidInput",
    "NotFound",
    "Product",
    "ReserveRequest",
    "ServedByMiddleware",
    "create_catalog_router",
    "parse_id",
    "register_error_handlers",
    "seed_products",
    "validate_id",
    "validate_quantity",
]
