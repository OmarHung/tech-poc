"""Orders module. Stays in the monolith; depends on the catalog only through CatalogPort."""

import itertools
import threading

from fastapi import APIRouter

from monolith.modules.catalog import ApiModel, CatalogPort, NotFound, parse_id, validate_id, validate_quantity


class Order(ApiModel):
    id: int
    product_id: int
    # Name and price are copied at order time, so later catalog changes (or the
    # catalog being unreachable) never alter an existing order.
    product_name: str
    unit_price: int
    quantity: int
    total: int


class CreateOrderRequest(ApiModel):
    product_id: int
    quantity: int


class OrderService:
    def __init__(self, catalog: CatalogPort) -> None:
        self._catalog = catalog
        self._lock = threading.Lock()
        self._orders: dict[int, Order] = {}
        self._ids = itertools.count(1)

    async def place(self, product_id: int, quantity: int) -> Order:
        validate_id(product_id, "productId")
        validate_quantity(quantity)
        product = await self._catalog.reserve(product_id, quantity)
        with self._lock:
            order = Order(
                id=next(self._ids),
                product_id=product_id,
                product_name=product.name,
                unit_price=product.price,
                quantity=quantity,
                total=product.price * quantity,
            )
            self._orders[order.id] = order
        return order

    def find(self, order_id: int) -> Order:
        order = self._orders.get(order_id)
        if order is None:
            raise NotFound(f"order {order_id} not found")
        return order


def create_orders_router(service: OrderService) -> APIRouter:
    router = APIRouter()

    @router.post("/orders", status_code=201)
    async def create_order(request: CreateOrderRequest) -> Order:
        return await service.place(request.product_id, request.quantity)

    @router.get("/orders/{order_id}")
    async def get_order(order_id: str) -> Order:
        return service.find(parse_id(order_id, "order id"))

    return router
