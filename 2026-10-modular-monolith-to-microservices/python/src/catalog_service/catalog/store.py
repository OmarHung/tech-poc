from __future__ import annotations

import threading
from collections.abc import Iterable

from .errors import insufficient_stock, product_not_found
from .models import Product, seed_products
from .validation import validate_quantity


class InMemoryCatalog:
    def __init__(self, products: Iterable[Product]) -> None:
        self._lock = threading.Lock()
        self._products: dict[int, Product] = {p.id: p for p in products}

    @classmethod
    def seeded(cls) -> InMemoryCatalog:
        return cls(seed_products())

    async def list_products(self) -> list[Product]:
        return sorted(self._products.values(), key=lambda p: p.id)

    async def get_product(self, product_id: int) -> Product:
        product = self._products.get(product_id)
        if product is None:
            raise product_not_found(product_id)
        return product

    async def reserve(self, product_id: int, quantity: int) -> Product:
        validate_quantity(quantity)
        with self._lock:
            current = self._products.get(product_id)
            if current is None:
                raise product_not_found(product_id)
            if current.stock < quantity:
                raise insufficient_stock(product_id, quantity, current.stock)
            updated = current.model_copy(update={"stock": current.stock - quantity})
            self._products[product_id] = updated
            return updated
