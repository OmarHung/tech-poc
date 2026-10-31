from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from helpers import FakeClock
from witness.config import WitnessConfig
from witness.main import create_app

pytestmark = pytest.mark.anyio


@pytest.fixture
async def witness(tmp_path: Path, clock: FakeClock) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(WitnessConfig(data_dir=str(tmp_path)), clock)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://witness") as client:
        yield client


async def test_lease_sync_and_health(witness: httpx.AsyncClient, tmp_path: Path) -> None:
    lease = await witness.post("/lease", json={"node": "a"})
    sync = await witness.post("/sync", json={"node": "a", "epoch": 1, "inSync": True, "expectedVersion": 1})
    health = await witness.get("/health")

    assert lease.json() == {
        "granted": True,
        "holder": "a",
        "epoch": 1,
        "ttlMs": 3000,
        "standbyInSync": False,
        "syncVersion": 1,
        "reason": None,
    }
    assert lease.headers["x-served-by"] == "witness"
    assert sync.json() == {"epoch": 1, "standbyInSync": True, "syncVersion": 2}
    assert health.json()["holder"] == "a"
    assert (tmp_path / "witness.json").read_text() == '{"epoch":1,"holder":"a","standbyInSync":true,"syncVersion":2}'


async def test_refusals_are_409_and_bad_input_is_400(witness: httpx.AsyncClient) -> None:
    await witness.post("/lease", json={"node": "a"})

    not_holder = await witness.post("/sync", json={"node": "b", "epoch": 1, "inSync": False})
    stale = await witness.post("/sync", json={"node": "a", "epoch": 1, "inSync": True, "expectedVersion": 0})
    unknown = await witness.post("/lease", json={"node": "c"})
    wrong_type = await witness.post("/sync", json={"node": "a", "epoch": 1, "inSync": "yes"})

    assert not_holder.status_code == 409
    assert "not the lease holder" in not_holder.json()["error"]
    assert stale.status_code == 409
    assert (unknown.status_code, unknown.json()) == (400, {"error": "node must be one of a, b, got 'c'"})
    assert (wrong_type.status_code, wrong_type.json()) == (400, {"error": "invalid JSON body at $.inSync"})


def test_create_app_reads_the_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NODES", "x,y")

    app = create_app()

    assert app.state.leases.health()["holder"] is None


def test_create_app_exits_with_the_variable_to_fix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NODES", "x")

    with pytest.raises(SystemExit, match="witness: NODES must be"):
        create_app()
