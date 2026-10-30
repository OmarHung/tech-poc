import httpx
import pytest
from fastapi.testclient import TestClient

from gateway.main import GatewayConfig, create_app
from gateway.routing import Upstreams, parse_route, resolve_upstream

UPSTREAMS = Upstreams(monolith="http://monolith.test", catalog="http://catalog.test")


@pytest.mark.parametrize("path", ["/products", "/products/1", "/products/1/reserve"])
def test_catalog_paths_follow_the_switch(path: str) -> None:
    assert resolve_upstream(path, "monolith", UPSTREAMS) == UPSTREAMS.monolith
    assert resolve_upstream(path, "service", UPSTREAMS) == UPSTREAMS.catalog


@pytest.mark.parametrize("path", ["/orders", "/orders/1", "/productsX", "/", "/health"])
def test_other_paths_always_go_to_the_monolith(path: str) -> None:
    assert resolve_upstream(path, "service", UPSTREAMS) == UPSTREAMS.monolith


@pytest.mark.parametrize("value", ["Service", "", None, 1])
def test_parse_route_rejects_unknown_values(value: object) -> None:
    with pytest.raises(ValueError, match="catalog must be"):
        parse_route(value)


def echo_upstream_host(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"host": request.url.host, "path": request.url.path})


@pytest.fixture
def gateway() -> TestClient:
    app = create_app(GatewayConfig(upstreams=UPSTREAMS), transport=httpx.MockTransport(echo_upstream_host))
    return TestClient(app)


def test_switching_the_route_moves_catalog_traffic_only(gateway: TestClient) -> None:
    assert gateway.get("/products/1").json()["host"] == "monolith.test"
    switched = gateway.put("/admin/routes", json={"catalog": "service"})
    assert switched.json() == {"catalog": "service"}
    assert switched.headers["x-served-by"] == "gateway"
    assert gateway.get("/products/1").json()["host"] == "catalog.test"
    assert gateway.post("/orders", json={}).json()["host"] == "monolith.test"


@pytest.mark.parametrize("body", [{"catalog": "nope"}, {"other": 1}, ["service"]])
def test_rejects_invalid_route_updates(gateway: TestClient, body: object) -> None:
    response = gateway.put("/admin/routes", json=body)
    assert response.status_code == 400
    assert "catalog must be" in response.json()["error"]
    assert gateway.get("/admin/routes").json() == {"catalog": "monolith"}


def test_rejects_malformed_route_update(gateway: TestClient) -> None:
    response = gateway.put("/admin/routes", content=b"{not json")
    assert response.status_code == 400


def test_unreachable_upstream_returns_502_without_details() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused to 10.0.0.1", request=request)

    client = TestClient(create_app(GatewayConfig(upstreams=UPSTREAMS), transport=httpx.MockTransport(refuse)))
    response = client.get("/orders/1")
    assert response.status_code == 502
    assert response.json() == {"error": "upstream unavailable"}
