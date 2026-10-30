import httpx
import pytest
from fastapi.testclient import TestClient

from catalog_service.main import create_app as create_catalog_service
from monolith.config import MonolithConfig
from monolith.main import create_app as create_monolith
from monolith.modules.catalog import (
    CatalogUnavailable,
    HttpCatalogClient,
    InMemoryCatalog,
    InsufficientStock,
    InvalidInput,
    NotFound,
    Product,
)
from monolith.modules.orders import OrderService

pytestmark = pytest.mark.anyio


def order_service() -> OrderService:
    return OrderService(InMemoryCatalog([Product(id=1, name="Keyboard", price=300, stock=5)]))


async def test_place_order_snapshots_product_and_computes_total() -> None:
    order = await order_service().place(1, 2)
    assert (order.product_name, order.unit_price, order.total) == ("Keyboard", 300, 600)


async def test_placed_order_can_be_found_and_ids_are_sequential() -> None:
    service = order_service()
    first = await service.place(1, 1)
    second = await service.place(1, 1)
    assert service.find(first.id) == first
    assert second.id == first.id + 1


async def test_catalog_errors_pass_through() -> None:
    with pytest.raises(InsufficientStock):
        await order_service().place(1, 6)
    with pytest.raises(NotFound):
        await order_service().place(9, 1)


@pytest.mark.parametrize(("product_id", "quantity"), [(0, 1), (1, 0)])
async def test_invalid_input_is_rejected(product_id: int, quantity: int) -> None:
    with pytest.raises(InvalidInput):
        await order_service().place(product_id, quantity)


def test_unknown_order_is_not_found() -> None:
    with pytest.raises(NotFound, match="order 42 not found"):
        order_service().find(42)


def test_config_rejects_unknown_catalog_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CATALOG_MODE", "sideways")
    with pytest.raises(ValueError, match="CATALOG_MODE"):
        MonolithConfig.from_env()


def test_monolith_app_places_orders_in_process() -> None:
    with TestClient(create_monolith(MonolithConfig(catalog_mode="inprocess"))) as client:
        response = client.post("/orders", json={"productId": 1, "quantity": 2})
        assert response.status_code == 201
        assert response.headers["x-served-by"] == "monolith"
        assert response.json()["total"] == 6400
        assert client.get("/products/1").json()["stock"] == 48


def remote_client(transport: httpx.AsyncBaseTransport) -> HttpCatalogClient:
    return HttpCatalogClient(httpx.AsyncClient(transport=transport, base_url="http://catalog"))


def against_real_catalog_service() -> HttpCatalogClient:
    """In-process round trip through the real catalog_service app."""
    return remote_client(httpx.ASGITransport(app=create_catalog_service()))


async def test_remote_client_round_trips_successful_calls() -> None:
    client = against_real_catalog_service()
    assert len(await client.list_products()) == 3
    assert (await client.reserve(1, 2)).stock == 48
    assert (await client.get_product(1)).stock == 48


async def test_remote_client_maps_error_statuses_back_to_catalog_errors() -> None:
    client = against_real_catalog_service()
    with pytest.raises(NotFound, match="product 99 not found"):
        await client.get_product(99)
    with pytest.raises(InsufficientStock, match="requested 99, available 50"):
        await client.reserve(1, 99)
    with pytest.raises(InvalidInput):
        await client.reserve(1, 0)


async def test_remote_client_hides_network_errors() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(CatalogUnavailable, match="^catalog unavailable$"):
        await remote_client(httpx.MockTransport(refuse)).list_products()


@pytest.mark.parametrize(
    "response",
    [httpx.Response(500, json={"error": "boom"}), httpx.Response(200, json={"unexpected": True})],
)
async def test_remote_client_treats_server_errors_and_bad_bodies_as_unavailable(response: httpx.Response) -> None:
    with pytest.raises(CatalogUnavailable):
        await remote_client(httpx.MockTransport(lambda _: response)).get_product(1)
