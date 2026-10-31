from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import httpx
import pytest

from gateway.config import GatewayConfig, NodeTarget
from gateway.main import create_app
from gateway.tracker import NodeStatus, PrimaryChoice, PrimaryTracker, choose

A = NodeTarget("a", "http://a")
B = NodeTarget("b", "http://b")


def test_choose_picks_the_node_that_says_primary() -> None:
    assert choose([(A, NodeStatus("standby", 3)), (B, NodeStatus("primary", 3))]) == PrimaryChoice(B, 3)


def test_choose_prefers_the_newer_epoch_when_both_claim_it() -> None:
    assert choose([(A, NodeStatus("primary", 4)), (B, NodeStatus("primary", 5))]) == PrimaryChoice(B, 5)
    assert choose([(A, NodeStatus("primary", 5)), (B, NodeStatus("primary", 5))]) == PrimaryChoice(A, 5)


def test_choose_returns_nothing_without_a_primary() -> None:
    assert choose([(A, None), (B, NodeStatus("standby", 2))]) is None
    assert choose([]) is None


@pytest.mark.parametrize(
    ("content_type", "body", "expected"),
    [
        ("application/json", b'{"role":"primary","epoch":2,"other":1}', NodeStatus("primary", 2)),
        ("application/json; charset=utf-8", b'{"Role":"standby"}', NodeStatus("standby", 0)),
        ("text/plain", b'{"role":"primary","epoch":2}', None),
        ("application/json", b"<html>", None),
        ("application/json", b"null", None),
        ("application/json", b'{"role":1}', None),
        ("application/json", b'{"role":"primary","epoch":"2"}', None),
        (None, b"", None),
    ],
)
def test_garbage_health_marks_the_node_unknown(content_type: str | None, body: bytes, expected) -> None:
    assert NodeStatus.parse(content_type, body) == expected


@dataclass
class FakeNodes:
    """Answers /health with a role and epoch per node; everything else echoes which node answered."""

    health: dict[str, tuple[str, int]] = field(default_factory=dict)
    down: set[str] = field(default_factory=set)
    fail_requests: set[str] = field(default_factory=set)
    garbage: set[str] = field(default_factory=set)
    status: int = 200
    seen: list[httpx.Request] = field(default_factory=list)

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        node = request.url.host
        if node in self.down:
            raise httpx.ConnectError(f"{node} is down", request=request)
        if request.url.path == "/health":
            if node in self.garbage:
                return httpx.Response(200, content=b"<html>not json</html>", headers={"content-type": "text/html"})
            role, epoch = self.health.get(node, ("standby", 0))
            return httpx.Response(200, json={"role": role, "epoch": epoch})
        if node in self.fail_requests:
            raise httpx.ReadError(f"{node} reset the connection", request=request)
        self.seen.append(request)
        return httpx.Response(
            self.status,
            json={"node": node, "path": request.url.path, "query": request.url.query.decode()},
            headers={
                "X-Served-By": f"node-{node}",
                "Date": "upstream",
                "Server": "upstream",
                "Content-Encoding": "identity",
            },
        )


@pytest.fixture
def nodes() -> FakeNodes:
    return FakeNodes()


@pytest.fixture
async def gateway(nodes: FakeNodes) -> AsyncIterator[tuple[httpx.AsyncClient, PrimaryTracker]]:
    config = GatewayConfig(nodes=(A, B), poll_interval_ms=20)
    app = create_app(config, transport=httpx.MockTransport(nodes))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
        yield client, app.state.tracker


@pytest.mark.anyio
async def test_no_primary_is_503_from_the_gateway(gateway, nodes: FakeNodes) -> None:
    client, tracker = gateway
    nodes.health["a"] = ("standby", 1)
    await tracker.poll_once()

    response = await client.put("/kv/k", json={"value": "v"})

    assert (response.status_code, response.json()) == (503, {"error": "no primary available"})
    assert response.headers["x-served-by"] == "gateway"


@pytest.mark.anyio
async def test_requests_go_to_the_primary_with_method_path_query_and_body(gateway, nodes: FakeNodes) -> None:
    client, tracker = gateway
    nodes.health.update(a=("primary", 1), b=("standby", 1))
    await tracker.poll_once()

    response = await client.put("/kv/a%20b?x=1", json={"value": "v"})

    assert response.status_code == 200
    assert response.json() == {"node": "a", "path": "/kv/a b", "query": "x=1"}
    assert response.headers["x-served-by"] == "node-a"
    assert response.headers.get_list("server") == []  # upstream Date/Server are not doubled up
    assert "content-encoding" not in response.headers
    forwarded = nodes.seen[-1]
    assert (forwarded.method, forwarded.content, forwarded.url.raw_path) == ("PUT", b'{"value":"v"}', b"/kv/a%20b?x=1")
    assert (await client.get("/health")).json() == {"status": "ok", "service": "gateway", "primary": "a", "epoch": 1}


@pytest.mark.anyio
async def test_requests_follow_a_failover(gateway, nodes: FakeNodes) -> None:
    client, tracker = gateway
    nodes.health.update(a=("primary", 1), b=("standby", 1))
    await tracker.poll_once()

    nodes.down.add("a")
    nodes.health["b"] = ("primary", 2)
    await tracker.poll_once()
    response = await client.get("/kv/k")

    assert response.headers["x-served-by"] == "node-b"


@pytest.mark.anyio
async def test_unreachable_primary_is_502_and_a_503_from_it_triggers_a_poll(gateway, nodes: FakeNodes) -> None:
    client, tracker = gateway
    nodes.health.update(a=("primary", 1), b=("standby", 1))
    await tracker.poll_once()

    nodes.fail_requests.add("a")
    unreachable = await client.get("/kv/k")
    nodes.fail_requests.clear()
    nodes.status = 503
    not_primary = await client.get("/kv/k")

    assert (unreachable.status_code, unreachable.json()) == (502, {"error": "primary unavailable"})
    assert unreachable.headers["x-served-by"] == "gateway"
    assert (not_primary.status_code, not_primary.headers["x-served-by"]) == (503, "node-a")
    assert tracker._nudged.is_set()


@pytest.mark.anyio
async def test_a_node_answering_garbage_is_skipped_not_fatal(gateway, nodes: FakeNodes) -> None:
    _, tracker = gateway
    nodes.garbage.add("a")
    nodes.health["b"] = ("primary", 3)

    assert await tracker.poll_once() == PrimaryChoice(B, 3)


@pytest.mark.anyio
async def test_the_lifespan_polls_in_the_background_and_reacts_to_a_nudge(nodes: FakeNodes) -> None:
    config = GatewayConfig(nodes=(A, B), poll_interval_ms=10_000)
    app = create_app(config, transport=httpx.MockTransport(nodes))
    tracker: PrimaryTracker = app.state.tracker
    nodes.health["b"] = ("primary", 1)

    async with app.router.lifespan_context(app):
        await asyncio.sleep(0.05)
        assert tracker.current == PrimaryChoice(B, 1)
        nodes.health["b"] = ("primary", 2)
        tracker.nudge()  # without it the next poll would be 10 s away
        await asyncio.sleep(0.05)
        assert tracker.current == PrimaryChoice(B, 2)


def test_create_app_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NODE_URLS", "x=http://127.0.0.1:1")
    monkeypatch.setenv("POLL_INTERVAL_MS", "50")

    assert create_app().state.tracker.current is None
    monkeypatch.setenv("NODE_URLS", "nope")
    with pytest.raises(SystemExit, match="gateway: NODE_URLS"):
        create_app()


@pytest.mark.anyio
async def test_an_oversized_body_is_refused_by_the_gateway(gateway, nodes: FakeNodes, monkeypatch) -> None:
    client, tracker = gateway
    nodes.health["a"] = ("primary", 1)
    await tracker.poll_once()
    monkeypatch.setattr("shared.http.MAX_BODY_BYTES", 8)

    response = await client.put("/kv/k", json={"value": "too long"})

    assert (response.status_code, response.headers["x-served-by"]) == (413, "gateway")
    assert nodes.seen == []
