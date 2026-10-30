from typing import Protocol

from .models import Product


class CatalogPort(Protocol):
    """Everything the rest of the system may ask of the catalog.

    Orders depend on this protocol only, which is the seam that lets the catalog
    move out of process: swap the in-memory implementation for an HTTP client and
    nothing else changes.
    """

    async def list_products(self) -> list[Product]: ...

    async def get_product(self, product_id: int) -> Product: ...

    async def reserve(self, product_id: int, quantity: int) -> Product:
        """Atomically takes `quantity` units out of stock and returns the updated product."""
        ...
