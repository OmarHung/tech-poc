"""Test doubles and harnesses: a fake clock, a fresh sale wired like the app, and the real app
served by uvicorn on a random port (the real HTTP stack, including streaming)."""

from __future__ import annotations

import asyncio
import socket
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace

import httpx
import uvicorn
from redis.asyncio import Redis

from checkout_queue.app import create_app
from checkout_queue.config import Config
from checkout_queue.orders import OrderView
from checkout_queue.services import Services
from checkout_queue.tickets import TicketView

START_MS = 1_793_318_400_000  # 2026-10-30T00:00:00Z

Tweak = Callable[[Config], Config]


class FakeClock:
    """Time stands still until a test moves it. Sleepers wake when the clock passes their deadline,
    so the background loops tick exactly when a test says so."""

    def __init__(self, start_ms: int = START_MS) -> None:
        self._now = start_ms
        self._sleepers: list[tuple[int, asyncio.Future[None]]] = []

    def now_ms(self) -> int:
        return self._now

    async def sleep_ms(self, ms: int) -> None:
        sleeper = (self._now + ms, asyncio.get_running_loop().create_future())
        self._sleepers.append(sleeper)
        try:
            await sleeper[1]
        finally:
            self._sleepers.remove(sleeper)

    def advance(self, ms: int) -> None:
        self._now += ms
        for deadline, future in list(self._sleepers):
            if deadline <= self._now and not future.done():
                future.set_result(None)


def new_config() -> Config:
    """Small numbers so every rule is easy to hit: 10 in stock, 3 at a time, up to 2 each."""
    return Config(
        sale_id=f"t{uuid.uuid4().hex}",
        initial_stock=10,
        max_per_order=2,
        max_active=3,
        admission_window_ms=60_000,
        payment_window_ms=120_000,
        heartbeat_timeout_ms=15_000,
        poll_interval_ms=1_000,
    )


class SaleUnderTest:
    """A fresh sale on the shared Redis, wired the same way as the app and driven by a fake clock."""

    def __init__(self, services: Services, clock: FakeClock) -> None:
        self.services = services
        self.clock = clock
        self.config = services.config
        self.queue = services.queue
        self.orders = services.orders
        self.audit = services.audit
        self.board = services.board
        self.maintenance = services.maintenance

    @classmethod
    async def start(cls, redis: Redis, tweak: Tweak | None = None) -> SaleUnderTest:
        config = (tweak or (lambda c: c))(new_config())
        clock = FakeClock()
        services = Services.build(config, redis, clock)
        await services.redis.initialize(config.initial_stock)
        return cls(services, clock)

    @property
    def now_ms(self) -> int:
        return self.clock.now_ms()

    async def join_and_admit(self, *user_ids: str) -> list[TicketView]:
        """Joins every user and runs one admission pass."""
        for user_id in user_ids:
            await self.queue.join(user_id)
        await self.queue.admit()
        tickets = []
        for user_id in user_ids:
            ticket = (await self.queue.join(user_id)).ticket
            assert ticket is not None
            tickets.append(ticket)
        return tickets

    async def place_order(self, user_id: str, quantity: int = 1) -> OrderView:
        """Puts one user through the line and returns their pending order."""
        ticket = (await self.join_and_admit(user_id))[0]
        result = await self.orders.checkout(ticket.ticket_id, quantity)
        assert result.outcome == "created" and result.order is not None
        return result.order


@dataclass
class AppUnderTest:
    http: httpx.AsyncClient
    clock: FakeClock
    services: Services
    port: int

    @property
    def maintenance(self):  # noqa: ANN201 - SaleMaintenance
        return self.services.maintenance


@asynccontextmanager
async def running_app(
    redis: Redis, tweak: Tweak | None = None, initialize_sale: bool = True
) -> AsyncIterator[AppUnderTest]:
    """The real HTTP pipeline on uvicorn, with the admission tick driven by the test."""
    config = replace((tweak or (lambda c: c))(new_config()), instance_name="test-instance")
    clock = FakeClock()
    crashes: list[BaseException] = []
    app = create_app(config, redis, clock, run_maintenance=False, on_fatal=crashes.append)
    services: Services = app.state.services
    if initialize_sale:
        await services.redis.initialize(config.initial_stock)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="warning", access_log=False, lifespan="on", timeout_graceful_shutdown=1)
    )
    serving = asyncio.create_task(server.serve(sockets=[sock]))
    while not server.started:
        if serving.done():
            serving.result()
        await asyncio.sleep(0.005)
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
            yield AppUnderTest(http, clock, services, port)
    finally:
        server.should_exit = True
        await serving
        sock.close()
    assert not crashes, crashes
