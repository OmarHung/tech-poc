"""The two loops every instance runs: maintenance (expire, then admit) and the event-stream push."""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from checkout_queue.app import create_app
from checkout_queue.config import Config
from checkout_queue.sale_redis import connect
from checkout_queue.services import Services
from support import FakeClock, new_config

pytestmark = pytest.mark.anyio


@asynccontextmanager
async def started(
    redis: Redis,
    *,
    run_maintenance: bool,
    config: Config | None = None,
    before_start: Callable[[Services], None] | None = None,
) -> AsyncIterator[tuple[Services, FakeClock, list[BaseException]]]:
    """The app's lifespan (subscribe, start the loops) without an HTTP server."""
    config = config or new_config()
    clock = FakeClock()
    crashes: list[BaseException] = []
    app = create_app(config, redis, clock, run_maintenance=run_maintenance, on_fatal=crashes.append)
    services: Services = app.state.services
    if before_start is not None:
        before_start(services)
    async with app.router.lifespan_context(app):
        yield services, clock, crashes


async def tick_until(clock: FakeClock, ms: int, condition: Callable[[], object], attempts: int = 50) -> None:
    for _ in range(attempts):
        if condition():
            return
        clock.advance(ms)
        await asyncio.sleep(0.02)


async def test_worker_admits_on_every_tick_without_being_asked(redis: Redis) -> None:
    async with started(redis, run_maintenance=True) as (services, clock, crashes):
        await services.redis.initialize(services.config.initial_stock)
        ticket = (await services.queue.join("alice")).ticket
        status = ticket.status
        for _ in range(50):
            if status != "waiting":
                break
            clock.advance(services.config.tick_ms)
            await asyncio.sleep(0.02)
            status = (await services.queue.get_ticket(ticket.ticket_id)).status

    assert status == "admitted"
    assert not crashes


async def test_worker_survives_redis_errors_and_retries_next_tick(caplog: pytest.LogCaptureFixture) -> None:
    dead = connect("127.0.0.1:1")
    try:
        async with started(dead, run_maintenance=True) as (services, clock, crashes):
            await tick_until(clock, services.config.tick_ms, lambda: "Maintenance tick failed" in caplog.text)
            await tick_until(clock, services.config.tick_ms, lambda: caplog.text.count("Maintenance tick failed") > 1)
    finally:
        await dead.aclose(close_connection_pool=True)

    assert caplog.text.count("Maintenance tick failed") >= 2
    assert not crashes


async def test_a_bug_in_a_loop_is_reported_as_fatal(redis: Redis) -> None:
    async def broken_watch(_: object) -> list[object]:
        raise RuntimeError("bug")

    def break_watch(services: Services) -> None:
        services.queue.watch = broken_watch  # type: ignore[method-assign]

    async with started(redis, run_maintenance=False, before_start=break_watch) as (services, clock, crashes):
        with services.streams.open("a" * 32):
            await tick_until(clock, services.config.poll_interval_ms, lambda: crashes)

    assert [str(e) for e in crashes] == ["bug"]


async def test_a_failed_subscribe_is_retried_on_the_next_refresh(redis: Redis) -> None:
    real_pubsub = redis.pubsub
    calls = 0

    def flaky_pubsub() -> object:
        nonlocal calls
        calls += 1
        pubsub = real_pubsub()
        if calls == 1:

            async def refuse(*_: object) -> None:
                raise RedisConnectionError("refused")

            pubsub.subscribe = refuse  # type: ignore[method-assign]
        return pubsub

    def patch(services: Services) -> None:
        services.redis.client.pubsub = flaky_pubsub  # type: ignore[method-assign]

    config = replace(new_config(), max_active=1)
    async with started(redis, run_maintenance=False, config=config, before_start=patch) as (services, clock, _):
        assert not services.push.subscribed
        await tick_until(clock, config.poll_interval_ms, lambda: services.push.subscribed)
        assert services.push.subscribed

        # Admissions now arrive over pub/sub again, without waiting for a refresh.
        await services.redis.initialize(config.initial_stock)
        ticket = (await services.queue.join("alice")).ticket
        with services.streams.open(ticket.ticket_id) as stream:
            await services.maintenance.run_once()
            pushed = await stream.next(timeout_s=5)

    assert pushed is not None and pushed.status == "admitted"


async def test_push_errors_are_logged_and_retried(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    dead = connect("127.0.0.1:1")
    try:
        async with started(dead, run_maintenance=False) as (services, clock, crashes):
            assert not services.push.subscribed
            with services.streams.open("b" * 32):
                await tick_until(
                    clock, services.config.poll_interval_ms, lambda: "Pushing ticket updates failed" in caplog.text
                )
    finally:
        await dead.aclose(close_connection_pool=True)

    assert "Subscribing to admissions failed" in caplog.text
    assert "Pushing ticket updates failed" in caplog.text
    assert not crashes


async def test_only_changed_views_are_pushed_and_closed_streams_are_forgotten(redis: Redis) -> None:
    async with started(redis, run_maintenance=False) as (services, clock, _):
        await services.redis.initialize(services.config.initial_stock)
        ticket = (await services.queue.join("watcher")).ticket
        with services.streams.open(ticket.ticket_id) as stream:
            clock.advance(services.config.poll_interval_ms)
            first = await stream.next(timeout_s=5)
            clock.advance(services.config.poll_interval_ms)
            unchanged = await stream.next(timeout_s=0.2)
        clock.advance(services.config.poll_interval_ms)
        await asyncio.sleep(0.05)

        assert first == ticket
        assert unchanged is None
        assert services.push._sent == {}


async def test_a_lost_subscription_is_picked_up_again_by_the_next_refresh(redis: Redis) -> None:
    class DeadPubSub:
        async def get_message(self, **_: object) -> None:
            raise RedisConnectionError("connection lost")

        async def aclose(self) -> None:
            pass

    async with started(redis, run_maintenance=False) as (services, clock, crashes):
        push = services.push
        dead = DeadPubSub()
        push._pubsub = dead  # type: ignore[assignment]

        await push._listen(dead)  # type: ignore[arg-type]
        assert not push.subscribed

        await tick_until(clock, services.config.poll_interval_ms, lambda: push.subscribed)
        resubscribed = push.subscribed

    assert resubscribed
    assert not crashes
