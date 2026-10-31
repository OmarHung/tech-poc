"""Entry point: `python -m checkout_queue` (what run.sh serve runs).

Startup order: read and validate the settings, connect to Redis (fail fast), start the sale
with init.lua, then hand over to uvicorn, whose lifespan subscribes to admissions and starts the
background loops before the socket opens.
"""

from __future__ import annotations

import asyncio
import logging
import resource
import sys

import uvicorn
import uvloop

from .app import create_app
from .clock import SystemClock
from .config import Config, ConfigError
from .sale_redis import REDIS_ERRORS, SaleRedis, connect, describe, warm_up

log = logging.getLogger("checkout_queue")

# Kestrel's default. Clients keep one connection per instance and may sit on it while their
# event stream runs on another one (verify/push.py does), so uvicorn's 5 s default is too short.
KEEP_ALIVE_S = 130
# Open event streams never end on their own; give them this long, then close them.
GRACEFUL_SHUTDOWN_S = 3
# Every user may hold a connection to every instance; the default 256 on macOS is too few.
WANTED_OPEN_FILES = 10_240


async def serve(config: Config) -> int:
    redis = connect(config.redis_url)
    try:
        try:
            await redis.ping()
        except REDIS_ERRORS as exc:
            log.error("Cannot reach Redis at %s: %s", config.redis_url, describe(exc))
            return 1
        opened = await warm_up(redis)
        clock = SystemClock()
        started_with = await SaleRedis(redis, config, clock).initialize(config.initial_stock)
        if started_with != config.initial_stock:
            log.warning(
                "Sale %s already started with %d in stock; ignoring INITIAL_STOCK=%d",
                config.sale_id,
                started_with,
                config.initial_stock,
            )

        server: uvicorn.Server | None = None
        crashed: list[BaseException] = []

        def stop_on_bug(error: BaseException) -> None:
            crashed.append(error)
            if server is not None:
                server.should_exit = True

        app = create_app(config, redis, clock, on_fatal=stop_on_bug)
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=config.port,
                access_log=False,
                timeout_keep_alive=KEEP_ALIVE_S,
                timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_S,
            )
        )
        log.info(
            "%s serving sale %s on http://127.0.0.1:%d (max active %d, %d Redis connections)",
            config.instance_name,
            config.sale_id,
            config.port,
            config.max_active,
            opened,
        )
        await server.serve()
        return 1 if crashed or not server.started else 0
    finally:
        await redis.aclose(close_connection_pool=True)


def raise_open_file_limit() -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = WANTED_OPEN_FILES if hard == resource.RLIM_INFINITY else min(WANTED_OPEN_FILES, hard)
    if soft != resource.RLIM_INFINITY and soft < wanted:
        resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        config = Config.from_env()
    except ConfigError as exc:
        log.error("Invalid configuration: %s", exc)
        return 2
    raise_open_file_limit()
    return asyncio.run(serve(config), loop_factory=uvloop.new_event_loop)


if __name__ == "__main__":
    sys.exit(main())
