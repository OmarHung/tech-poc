"""Process startup: settings, fail-fast Redis, init.lua, and stopping on a bug."""

import asyncio
import logging
import resource
import socket
from dataclasses import replace

import httpx
import pytest
from redis.asyncio import Redis

from checkout_queue import __main__ as entry
from checkout_queue import app as app_module
from checkout_queue.clock import SystemClock
from checkout_queue.sale_redis import MAX_CONNECTIONS, SaleRedis, connect, warm_up
from support import new_config

pytestmark = pytest.mark.anyio


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_main_rejects_a_bad_setting_naming_the_variable(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("MAX_ACTIVE", "lots")

    assert entry.main() == 2
    assert "MAX_ACTIVE" in caplog.text


async def test_serve_fails_fast_when_redis_is_unreachable(caplog: pytest.LogCaptureFixture) -> None:
    config = replace(new_config(), redis_url="127.0.0.1:1", port=free_port())

    assert await asyncio.wait_for(entry.serve(config), 10) == 1
    assert "Cannot reach Redis at 127.0.0.1:1" in caplog.text


async def test_serve_starts_the_sale_and_stops_when_a_loop_dies_of_a_bug(
    redis: Redis, redis_url: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    config = replace(new_config(), redis_url=redis_url, port=free_port(), initial_stock=7)
    await SaleRedis(redis, config, SystemClock()).initialize(5)  # someone else started this sale
    healthy = asyncio.Event()

    async def maintenance_with_a_bug(*_: object) -> None:
        # Runs from the lifespan, before the socket opens: wait until the server answers.
        async with httpx.AsyncClient() as http:
            while not healthy.is_set():
                try:
                    response = await http.get(f"http://127.0.0.1:{config.port}/health")
                    if response.status_code == 200:
                        healthy.set()
                except httpx.TransportError:
                    await asyncio.sleep(0.02)
        raise RuntimeError("bug in the maintenance loop")

    monkeypatch.setattr(app_module, "run_maintenance_loop", maintenance_with_a_bug)

    exit_code = await asyncio.wait_for(entry.serve(config), 20)

    assert exit_code == 1
    assert healthy.is_set()
    assert "already started with 5 in stock; ignoring INITIAL_STOCK=7" in caplog.text
    assert "Background task 'maintenance' crashed" in caplog.text
    assert f"{MAX_CONNECTIONS} Redis connections" in caplog.text


async def test_warm_up_opens_every_pooled_connection_before_the_crowd_arrives(redis_url: str) -> None:
    client = connect(redis_url)
    try:
        opened = await warm_up(client)
        idle = list(client.connection_pool._available_connections)
        connected = [connection.is_connected for connection in idle]
    finally:
        await client.aclose(close_connection_pool=True)

    assert opened == MAX_CONNECTIONS
    assert connected == [True] * MAX_CONNECTIONS


def test_open_file_limit_is_raised_when_low(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(resource, "getrlimit", lambda _: (256, resource.RLIM_INFINITY))
    monkeypatch.setattr(resource, "setrlimit", lambda _, limits: calls.append(limits))

    entry.raise_open_file_limit()

    assert calls == [(entry.WANTED_OPEN_FILES, resource.RLIM_INFINITY)]


async def test_connections_that_break_are_replaced_without_the_caller_noticing(redis: Redis, redis_url: str) -> None:
    client = connect(redis_url)
    try:
        await warm_up(client)
        sale = SaleRedis(client, new_config(), SystemClock())
        # Like a Redis restart or a network blip: every pooled connection is cut server-side.
        killed = await redis.execute_command("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes")

        started_with = await sale.initialize(3)
    finally:
        await client.aclose(close_connection_pool=True)

    assert killed >= MAX_CONNECTIONS
    assert started_with == 3
