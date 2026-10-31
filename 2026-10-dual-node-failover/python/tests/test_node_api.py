"""The real HTTP pipeline of one node over an in-process ASGI transport, with election and replication driven by the
test (the lifespan, and with it the background loops, never starts)."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from node.config import ArbiterMode, NodeConfig
from node.kv import MAX_VALUE_LENGTH
from node.main import NodeRuntime, build_app

pytestmark = pytest.mark.anyio


@pytest.fixture
async def node(tmp_path: Path) -> AsyncIterator[tuple[httpx.AsyncClient, NodeRuntime]]:
    runtime = NodeRuntime(NodeConfig(data_dir=str(tmp_path), arbiter=ArbiterMode.NONE))
    app = build_app(runtime, run_workers=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://node") as client:
        yield client, runtime
    await runtime.stop()


async def promote(runtime: NodeRuntime) -> None:
    state = runtime.state
    state.try_promote(state.epoch + 1, math.inf, standby_in_sync=False)
    term = state.current_term
    assert term is not None
    assert await term.try_go_solo() is True  # what the replicator does at the start of a term


async def test_standby_answers_503_and_says_who_it_is(node) -> None:
    client, _ = node

    put = await client.put("/kv/k", json={"value": "v"})
    get = await client.get("/kv/k")

    assert put.status_code == 503
    assert put.json() == {"error": "not primary"}
    assert get.status_code == 503
    assert put.headers["x-served-by"] == "node-a"


async def test_primary_stores_and_returns_values(node) -> None:
    client, runtime = node
    await promote(runtime)

    put = await client.put("/kv/order-1", json={"value": "paid"})
    get = await client.get("/kv/order-1")

    assert put.status_code == 200
    assert put.json() == {"key": "order-1", "value": "paid", "index": 1, "epoch": 1}
    assert get.json() == put.json()


async def test_missing_key_is_404(node) -> None:
    client, runtime = node
    await promote(runtime)

    response = await client.get("/kv/nothing")

    assert (response.status_code, response.json()) == (404, {"error": "key 'nothing' not found"})


@pytest.mark.parametrize(
    ("path", "body", "error"),
    [
        ("/kv/bad%20key", b'{"value":"v"}', "key must be"),
        ("/kv/" + "k" * 129, b'{"value":"v"}', "key must be"),
        ("/kv/k", b'{"value":1}', "invalid JSON body at $.value"),
        ("/kv/k", b'{"value":null}', "value must be a string"),
        ("/kv/k", b"{}", "value must be a string"),
        ("/kv/k", b"not json", "invalid JSON body at $"),
        ("/kv/k", b"null", "request body is required"),
        ("/kv/k", b"[1]", "invalid JSON body at $"),
    ],
)
async def test_bad_writes_are_400(node, path: str, body: bytes, error: str) -> None:
    client, runtime = node
    await promote(runtime)

    response = await client.put(path, content=body, headers={"content-type": "application/json"})

    assert response.status_code == 400
    assert response.json()["error"].startswith(error)


async def test_a_body_that_is_not_json_is_400(node) -> None:
    client, runtime = node
    await promote(runtime)

    response = await client.put("/kv/k", content=b'{"value":"v"}', headers={"content-type": "text/plain"})

    assert (response.status_code, response.json()) == (400, {"error": "request body must be JSON"})


async def test_value_length_is_counted_like_dotnet(node) -> None:
    client, runtime = node
    await promote(runtime)

    longest = await client.put("/kv/k", json={"value": "x" * MAX_VALUE_LENGTH})
    too_long = await client.put("/kv/k", json={"value": "x" * (MAX_VALUE_LENGTH + 1)})
    # An emoji is one Python character but two UTF-16 code units, as in .NET's string.Length.
    emoji = await client.put("/kv/k", json={"value": "\U0001f600" * (MAX_VALUE_LENGTH // 2 + 1)})

    assert (longest.status_code, too_long.status_code, emoji.status_code) == (200, 400, 400)


async def test_unknown_routes_and_methods_are_json_errors(node) -> None:
    client, _ = node

    missing = await client.get("/nowhere")
    wrong_method = await client.delete("/kv/k")

    assert (missing.status_code, missing.json()) == (404, {"error": "Not Found"})
    assert (wrong_method.status_code, wrong_method.json()) == (405, {"error": "Method Not Allowed"})
    assert missing.headers["x-served-by"] == "node-a"


async def test_replicate_applies_entries_and_admin_data_shows_them(node) -> None:
    client, _ = node
    request = {
        "leader": "b",
        "epoch": 2,
        "prevIndex": 0,
        "prevEpoch": 0,
        "entries": [{"index": 1, "epoch": 2, "key": "k", "value": "v"}],
        "leaderLastIndex": 1,
    }

    reply = await client.post("/internal/replicate", json=request)
    data = await client.get("/admin/data")
    again = await client.post("/internal/replicate", json=request)

    assert reply.json() == {"status": "ok", "epoch": 2, "matchIndex": 1, "promiseMs": 3000, "reason": None}
    assert data.json() == {"node": "a", "role": "standby", "epoch": 2, "lastIndex": 1, "data": {"k": "v"}}
    assert again.json()["status"] == "ok"


@pytest.mark.parametrize(
    ("body", "error"),
    [
        ({"epoch": "2"}, "invalid JSON body at $.epoch"),
        ({"epoch": 1, "entries": [{"index": "1"}], "leaderLastIndex": 1}, "invalid JSON body at $.entries[0].index"),
        ({"epoch": 1, "entries": [None], "leaderLastIndex": 1}, "entry 0 does not continue the batch"),
        ({"epoch": 1, "entries": [{"index": 1, "epoch": 1, "key": "k"}], "leaderLastIndex": 1}, "entry 0"),
        ({"epoch": 1, "prevIndex": 3, "leaderLastIndex": 1}, "prevIndex and leaderLastIndex"),
    ],
)
async def test_bad_replication_requests_are_400(node, body: dict[str, object], error: str) -> None:
    client, _ = node

    response = await client.post("/internal/replicate", json=body)

    assert response.status_code == 400
    assert response.json()["error"].startswith(error)


async def test_health_reports_role_and_modes(node) -> None:
    client, _ = node

    health = (await client.get("/health")).json()

    assert health == {
        "status": "ok",
        "node": "a",
        "role": "standby",
        "epoch": 0,
        "lastIndex": 0,
        "matchIndex": None,
        "peer": None,
        "leaseRemainingMs": None,
        "truncatedEntries": 0,
        "arbiter": "none",
        "replication": "sync",
    }


async def test_unexpected_errors_are_500_without_details(node, monkeypatch: pytest.MonkeyPatch) -> None:
    client, runtime = node

    def boom() -> None:
        raise RuntimeError("secret detail")

    monkeypatch.setattr(runtime.state, "health", boom)
    transport = httpx.ASGITransport(app=client._transport.app, raise_app_exceptions=False)  # type: ignore[attr-defined]
    async with httpx.AsyncClient(transport=transport, base_url="http://node") as raw:
        response = await raw.get("/health")

    assert (response.status_code, response.json()) == (500, {"error": "internal error"})


async def test_an_oversized_body_is_413(node, monkeypatch: pytest.MonkeyPatch) -> None:
    client, runtime = node
    await promote(runtime)
    monkeypatch.setattr("shared.http.MAX_BODY_BYTES", 64)

    declared = await client.put("/kv/k", json={"value": "x" * 100})

    async def chunks():
        yield b'{"value":"'
        yield b"x" * 100
        yield b'"}'

    streamed = await client.put("/kv/k", content=chunks(), headers={"content-type": "application/json"})

    assert (declared.status_code, declared.json()) == (413, {"error": "request body too large"})
    assert streamed.status_code == 413
