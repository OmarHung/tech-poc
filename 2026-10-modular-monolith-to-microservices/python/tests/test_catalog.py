"""Runs against both copies of the catalog module: the monolith's (migrated, kept for
rollback) and catalog-service's. Also guards against the two copies drifting apart."""

import asyncio
from types import ModuleType
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import catalog_service.catalog as service_catalog
import monolith.modules.catalog as monolith_catalog

pytestmark = pytest.mark.anyio


@pytest.fixture(params=[monolith_catalog, service_catalog], ids=["monolith", "catalog-service"])
def module(request: pytest.FixtureRequest) -> ModuleType:
    return request.param


def catalog_with_stock(module: ModuleType, stock: int) -> Any:  # InMemoryCatalog of whichever copy
    return module.InMemoryCatalog([module.Product(id=1, name="Keyboard", price=100, stock=stock)])


async def test_list_returns_products_sorted_by_id(module: ModuleType) -> None:
    products = await module.InMemoryCatalog.seeded().list_products()
    assert [p.id for p in products] == [1, 2, 3]


async def test_get_unknown_product_is_not_found(module: ModuleType) -> None:
    with pytest.raises(module.NotFound, match="product 99 not found"):
        await catalog_with_stock(module, 5).get_product(99)


async def test_reserve_returns_new_product_and_keeps_old_one_unchanged(module: ModuleType) -> None:
    catalog = catalog_with_stock(module, 5)
    before = await catalog.get_product(1)
    updated = await catalog.reserve(1, 2)
    assert updated.stock == 3
    assert before.stock == 5
    assert (await catalog.get_product(1)).stock == 3


async def test_reserve_more_than_stock_is_rejected_without_change(module: ModuleType) -> None:
    catalog = catalog_with_stock(module, 1)
    with pytest.raises(module.InsufficientStock):
        await catalog.reserve(1, 2)
    assert (await catalog.get_product(1)).stock == 1


async def test_concurrent_reservations_never_oversell(module: ModuleType) -> None:
    catalog = catalog_with_stock(module, 10)

    async def attempt() -> bool:
        try:
            await catalog.reserve(1, 1)
            return True
        except module.InsufficientStock:
            return False

    results = await asyncio.gather(*(attempt() for _ in range(50)))
    assert sum(results) == 10
    assert (await catalog.get_product(1)).stock == 0


@pytest.mark.parametrize("quantity", [0, -1, 101])
def test_validate_quantity_rejects_out_of_range(module: ModuleType, quantity: int) -> None:
    with pytest.raises(module.InvalidInput):
        module.validate_quantity(quantity)


@pytest.mark.parametrize("raw", ["0", "-1", "abc", "", "1.5", " 1", "١"])
def test_parse_id_accepts_only_positive_ascii_integers(module: ModuleType, raw: str) -> None:
    with pytest.raises(module.InvalidInput, match="product id must be a positive integer"):
        module.parse_id(raw, "product id")


def test_parse_id_returns_int(module: ModuleType) -> None:
    assert module.parse_id("42", "product id") == 42


@pytest.fixture
def api(module: ModuleType) -> TestClient:
    app = FastAPI()
    module.register_error_handlers(app)
    app.include_router(module.create_catalog_router(module.InMemoryCatalog.seeded()))
    return TestClient(app)


@pytest.mark.parametrize(
    ("method", "path", "body", "status"),
    [
        ("GET", "/products/999", None, 404),
        ("GET", "/products/abc", None, 400),
        ("POST", "/products/1/reserve", {"quantity": "2"}, 400),
        ("POST", "/products/1/reserve", {"quantity": 0}, 400),
        ("POST", "/products/1/reserve", {}, 400),
        ("POST", "/products/1/reserve", {"quantity": 99}, 409),
    ],
)
def test_api_errors_use_status_code_and_error_body(
    api: TestClient, method: str, path: str, body: object, status: int
) -> None:
    response = api.request(method, path, json=body)
    assert response.status_code == status
    assert isinstance(response.json()["error"], str)


def test_api_rejects_malformed_json(api: TestClient) -> None:
    response = api.post("/products/1/reserve", content=b"{not json", headers={"Content-Type": "application/json"})
    assert response.status_code == 400


def test_api_reserve_returns_updated_product(api: TestClient) -> None:
    response = api.post("/products/1/reserve", json={"quantity": 2})
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Mechanical Keyboard", "price": 3200, "stock": 48}
