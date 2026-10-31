"""One throwaway Redis for the whole run (Testcontainers). Every test opens its own sale (a random
SALE_ID), so tests never see each other's keys."""

from collections.abc import AsyncIterator, Iterator

import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from checkout_queue.sale_redis import connect

REDIS_IMAGE = "redis:7.4-alpine"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer(REDIS_IMAGE) as container:
        yield f"{container.get_container_host_ip()}:{container.get_exposed_port(6379)}"


@pytest.fixture
async def redis(redis_url: str) -> AsyncIterator[Redis]:
    client = connect(redis_url)
    yield client
    await client.aclose(close_connection_pool=True)
