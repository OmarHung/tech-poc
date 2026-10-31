"""Whole-cluster behaviour in one process: witness and both nodes on ASGI transports, with the real election and
replication loops on a short lease. verify/scenario.py covers the same ground with real processes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import httpx
import pytest

from node.config import NodeConfig
from node.main import NodeRuntime, build_app
from shared.clock import MonotonicClock
from witness.config import WitnessConfig
from witness.main import create_app as create_witness

pytestmark = pytest.mark.anyio

LEASE_TTL_MS = 600
NODE_IDS = ("a", "b")


def other(node: str) -> str:
    return "b" if node == "a" else "a"


class FakeNetwork:
    """Routes requests by host name ("a", "b", "witness") to the ASGI apps. Can take a process down or cut links."""

    def __init__(self) -> None:
        self.apps: dict[str, Callable[..., Awaitable[None]]] = {}
        self.down: set[str] = set()
        self.cut: set[tuple[str, str]] = set()

    def isolate(self, node: str) -> None:
        for peer in self.apps:
            if peer != node:
                self.cut |= {(node, peer), (peer, node)}

    def heal(self) -> None:
        self.cut.clear()

    def transport_from(self, origin: str) -> httpx.AsyncBaseTransport:
        return _Router(self, origin)


class _Router(httpx.AsyncBaseTransport):
    def __init__(self, network: FakeNetwork, origin: str) -> None:
        self._network = network
        self._origin = origin

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        target, network = request.url.host, self._network
        if self._origin in network.down or target in network.down or (self._origin, target) in network.cut:
            raise httpx.ConnectError(f"{self._origin} cannot reach {target}", request=request)
        return await httpx.ASGITransport(app=network.apps[target]).handle_async_request(request)


class Cluster:
    def __init__(self, root: Path) -> None:
        self.network = FakeNetwork()
        self.runtimes: dict[str, NodeRuntime] = {}
        self.clients: dict[str, httpx.AsyncClient] = {}
        self.fatal: list[BaseException] = []
        witness = create_witness(WitnessConfig(data_dir=str(root / "witness"), lease_ttl_ms=LEASE_TTL_MS))
        self.network.apps["witness"] = witness
        for node in NODE_IDS:
            config = NodeConfig(
                node_id=node,
                peer_url=f"http://{other(node)}/",
                witness_url="http://witness/",
                data_dir=str(root / node),
                lease_ttl_ms=LEASE_TTL_MS,
                renew_interval_ms=LEASE_TTL_MS // 6,
                replication_timeout_ms=LEASE_TTL_MS // 3,
            )
            runtime = NodeRuntime(
                config, MonotonicClock(), self.network.transport_from(node), on_fatal=self.fatal.append
            )
            app = build_app(runtime, run_workers=False)
            self.runtimes[node] = runtime
            self.network.apps[node] = app
            self.clients[node] = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=f"http://{node}")

    async def start(self) -> None:
        for runtime in self.runtimes.values():
            await runtime.start()

    async def stop(self) -> None:
        for node, runtime in self.runtimes.items():
            if node not in self.network.down:
                await runtime.stop()
        for client in self.clients.values():
            await client.aclose()

    async def kill(self, name: str) -> None:
        """Stops the process and makes it unreachable, like kill -9 plus a dead NIC."""
        self.network.down.add(name)
        if name in self.runtimes:
            await self.runtimes[name].stop()

    async def health(self, node: str) -> dict[str, object]:
        return (await self.clients[node].get("/health")).json()

    async def data(self, node: str) -> dict[str, str]:
        return (await self.clients[node].get("/admin/data")).json()["data"]

    async def put(self, node: str, key: str) -> httpx.Response:
        return await self.clients[node].put(f"/kv/{key}", json={"value": key})

    async def write(self, node: str, prefix: str, count: int) -> list[str]:
        """Writes keys (value = key) and asserts each one was acknowledged."""
        keys = []
        for i in range(count):
            key = f"{prefix}-{i}"
            response = await self.put(node, key)
            assert response.status_code == 200, f"{key} on {node}: {response.status_code} {response.text}"
            keys.append(key)
        return keys

    async def stable_primary(self) -> tuple[str, str]:
        """Waits for a primary whose standby is in sync and returns (primary, standby)."""
        found: list[str] = []

        async def ready() -> bool:
            for node in NODE_IDS:
                health = await self.health(node) if node not in self.network.down else {}
                if health.get("role") == "primary" and health.get("peer") == "in-sync":
                    found.append(node)
                    return True
            return False

        await wait_until(ready, "a primary with an in-sync standby")
        return found[0], other(found[0])

    async def wait_for_role(self, node: str, role: str) -> None:
        async def has_role() -> bool:
            return (await self.health(node))["role"] == role

        await wait_until(has_role, f"{node} to be {role}")


async def wait_until(condition: Callable[[], Awaitable[bool]], what: str) -> None:
    deadline = time.monotonic() + LEASE_TTL_MS / 1000 * 10
    while not await condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.02)


@pytest.fixture
async def cluster(tmp_path: Path) -> AsyncIterator[Cluster]:
    running = Cluster(tmp_path)
    await running.start()
    yield running
    await running.stop()
    assert running.fatal == []


async def test_one_primary_replicates_every_acknowledged_write(cluster: Cluster) -> None:
    primary, standby = await cluster.stable_primary()

    keys = await cluster.write(primary, "k", 20)

    assert (await cluster.health(standby))["role"] == "standby"
    on_standby = await cluster.data(standby)
    assert all(on_standby.get(key) == key for key in keys)
    assert (await cluster.put(standby, "nope")).status_code == 503


async def test_standby_takes_over_when_the_primary_dies_with_every_acknowledged_write(cluster: Cluster) -> None:
    primary, standby = await cluster.stable_primary()
    old_epoch = (await cluster.health(primary))["epoch"]
    keys = await cluster.write(primary, "before", 20)

    await cluster.kill(primary)
    await cluster.wait_for_role(standby, "primary")

    health = await cluster.health(standby)
    assert health["epoch"] > old_epoch  # type: ignore[operator]
    assert health["peer"] == "solo"
    data = await cluster.data(standby)
    assert all(data.get(key) == key for key in keys)
    assert (await cluster.put(standby, "after")).status_code == 200


async def test_isolated_primary_stops_acknowledging_and_rejoins_as_standby(cluster: Cluster) -> None:
    primary, standby = await cluster.stable_primary()
    keys = await cluster.write(primary, "before", 10)

    cluster.network.isolate(primary)
    cut_off = await cluster.put(primary, "cut-off")
    await cluster.wait_for_role(standby, "primary")
    accepted = await cluster.write(standby, "after", 10)
    cluster.network.heal()
    await cluster.wait_for_role(primary, "standby")

    async def caught_up() -> bool:
        return (await cluster.health(standby))["peer"] == "in-sync"

    await wait_until(caught_up, "the old primary to catch up")

    assert cut_off.status_code == 503
    new_primary_data = await cluster.data(standby)
    assert new_primary_data == await cluster.data(primary)
    assert all(new_primary_data.get(key) == key for key in keys + accepted)
    assert "cut-off" not in new_primary_data


async def test_primary_keeps_serving_while_the_witness_is_down(cluster: Cluster) -> None:
    primary, _ = await cluster.stable_primary()
    epoch = (await cluster.health(primary))["epoch"]

    await cluster.kill("witness")
    await asyncio.sleep(LEASE_TTL_MS / 1000 * 3)
    keys = await cluster.write(primary, "no-witness", 10)

    health = await cluster.health(primary)
    assert (health["role"], health["epoch"], len(keys)) == ("primary", epoch, 10)


async def test_primary_goes_solo_when_the_standby_dies(cluster: Cluster) -> None:
    primary, standby = await cluster.stable_primary()

    await cluster.kill(standby)
    keys = await cluster.write(primary, "alone", 5)

    assert (await cluster.health(primary))["peer"] == "solo"
    assert len(keys) == 5
