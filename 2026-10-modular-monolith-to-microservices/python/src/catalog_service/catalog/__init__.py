"""Catalog module, moved here from the monolith's modules/catalog.

The files are the same as the monolith's copy (which is now marked as migrated),
minus the HTTP client the monolith uses to call this service.
"""

from .api import ServedByMiddleware, create_catalog_router, register_error_handlers
from .errors import CatalogError, CatalogUnavailable, InsufficientStock, InvalidInput, NotFound
from .models import ApiModel, Product, ReserveRequest, seed_products
from .port import CatalogPort
from .store import InMemoryCatalog
from .validation import MAX_QUANTITY, parse_id, validate_id, validate_quantity

__all__ = [
    "MAX_QUANTITY",
    "ApiModel",
    "CatalogError",
    "CatalogPort",
    "CatalogUnavailable",
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
