from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from shared.errors import InvalidInput
from shared.work_contracts import ResultRecord
from sink.config import SinkConfig
from sink.main import create_app
from sink.result_store import MAX_VALUE_LENGTH, ResultStatus, ResultStore, SinkSummary

pytestmark = pytest.mark.anyio


@pytest.fixture
def store() -> ResultStore:
    return ResultStore(lambda: 1_000)


def test_the_first_copy_is_applied_and_the_same_value_again_is_a_duplicate(store: ResultStore) -> None:
    assert store.record(ResultRecord("event-1", "a", 0, "acct:5")) is ResultStatus.APPLIED
    assert store.record(ResultRecord("event-1", "b", 0, "acct:5")) is ResultStatus.DUPLICATE

    assert store.summary == SinkSummary(received=2, applied=1, duplicates=1)
    assert store.result("event-1") == {
        "value": "acct:5",
        "node": "a",
        "attempt": 0,
        "firstAtMs": 1_000,
        "copies": 2,
        "conflicts": 0,
        "stale": 0,
    }


def test_a_different_value_is_a_conflict_and_the_first_stays(store: ResultStore) -> None:
    store.record(ResultRecord("event-1", "a", 0, "acct:5"))

    assert store.record(ResultRecord("event-1", "b", 0, "acct:7")) is ResultStatus.CONFLICT

    result = store.result("event-1")
    assert result is not None and (result["value"], result["conflicts"]) == ("acct:5", 1)
    assert store.summary.conflicts == 1


def test_an_older_attempt_after_a_newer_one_is_stale(store: ResultStore) -> None:
    store.record(ResultRecord("task-1", "b", 2, "1^2=1"))

    assert store.record(ResultRecord("task-1", "a", 1, "1^2=1")) is ResultStatus.STALE
    assert store.record(ResultRecord("task-1", "a", 3, "1^2=1")) is ResultStatus.DUPLICATE
    assert store.record(ResultRecord("task-1", "a", 2, "1^2=1")) is ResultStatus.STALE  # 3 is the newest now

    assert store.summary == SinkSummary(received=4, applied=1, duplicates=1, stale=2)
    assert store.result("missing") is None


@pytest.mark.parametrize(
    ("key", "node", "attempt", "value"),
    [
        (None, "a", 0, "v"),
        ("bad key", "a", 0, "v"),
        ("k", "A", 0, "v"),
        ("k", None, 0, "v"),
        ("k", "a", -1, "v"),
        ("k", "a", 0, None),
        ("k", "a", 0, "€" * (MAX_VALUE_LENGTH + 1)),
        ("k", "a", 0, "😀" * (MAX_VALUE_LENGTH // 2 + 1)),  # two UTF-16 units each, as .NET counts them
    ],
)
def test_bad_records_are_rejected(store: ResultStore, key: str | None, node: str | None, attempt: int, value) -> None:
    with pytest.raises(InvalidInput):
        store.record(ResultRecord(key, node, attempt, value))
    assert store.summary.received == 0


@pytest.fixture
async def sink() -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(SinkConfig(), wall_clock_ms=lambda: 42)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://sink") as client:
        yield client


async def post(sink: httpx.AsyncClient, node: str, attempt: int, value: str) -> httpx.Response:
    return await sink.post("/results", json={"key": "event-1", "node": node, "attempt": attempt, "value": value})


async def test_results_answer_with_their_status(sink: httpx.AsyncClient) -> None:
    applied = await post(sink, "a", 1, "x:1")
    duplicate = await post(sink, "b", 1, "x:1")
    conflict = await post(sink, "b", 1, "x:2")
    stale = await post(sink, "b", 0, "x:1")
    snapshot = (await sink.get("/results")).json()
    health = (await sink.get("/health")).json()

    assert (applied.status_code, applied.json()) == (201, {"status": "applied"})
    assert (duplicate.status_code, duplicate.json()) == (200, {"status": "duplicate"})
    assert (conflict.status_code, conflict.json()) == (409, {"status": "conflict"})
    assert (stale.status_code, stale.json()) == (409, {"status": "stale"})
    assert applied.headers["x-served-by"] == "sink"
    summary = {"received": 4, "applied": 1, "duplicates": 1, "conflicts": 1, "stale": 1}
    first = {"value": "x:1", "node": "a", "attempt": 1, "firstAtMs": 42, "copies": 4, "conflicts": 1, "stale": 1}
    assert snapshot == {"summary": summary, "results": {"event-1": first}}
    assert health == {"status": "ok", "service": "sink", "summary": summary}


@pytest.mark.parametrize(
    "body",
    [
        {"key": "bad key", "node": "a", "attempt": 0, "value": "v"},
        {"key": "k", "node": "a", "attempt": "0", "value": "v"},
        {"key": "k", "node": "a", "attempt": 0, "value": 5},
        [],
    ],
)
async def test_bad_results_are_400(sink: httpx.AsyncClient, body: object) -> None:
    response = await sink.post("/results", json=body)

    assert response.status_code == 400
    assert "error" in response.json()


def test_create_app_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "9123")
    assert create_app().state.results.summary.received == 0

    monkeypatch.setenv("PORT", "0")
    with pytest.raises(SystemExit, match="sink: PORT"):
        create_app()
