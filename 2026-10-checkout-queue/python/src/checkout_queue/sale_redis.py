"""All Redis access for one sale.

Every state change is one Lua script from ../../../shared/lua (shared by every language version
of this PoC), so each is atomic on its own and safe to call from any number of instances.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from .clock import Clock
from .config import Config

SHARED_DIR = Path(__file__).resolve().parents[3] / "shared"
SCRIPT_NAMES = (
    "init", "join", "ticket", "admit", "checkout", "order", "pay", "expire_orders", "audit", "tickets", "watch",
)  # fmt: skip

# What "storage unavailable" looks like: Redis errors (including a script error the server reports,
# as StackExchange.Redis does in the .NET version) and raw network failures.
REDIS_ERRORS: tuple[type[BaseException], ...] = (RedisError, ConnectionError, TimeoutError)

# redis-py has no multiplexer: each in-flight command holds one pooled connection. One event loop
# rarely has more than a few commands in flight, so a small pool, opened up front (warm_up), is plenty.
MAX_CONNECTIONS = 32
POOL_WAIT_S = 5.0
SOCKET_TIMEOUT_S = 5.0
# Broken connections: retry after 0.05 s, 0.1 s, 0.2 s.
RETRIES = 3
RETRY_BASE_S = 0.025
RETRY_CAP_S = 0.2


def load_scripts(lua_dir: Path = SHARED_DIR / "lua") -> dict[str, str]:
    """Each script with common.lua (the shared helpers) in front of it."""
    common = (lua_dir / "common.lua").read_text(encoding="utf-8")
    return {name: common + "\n" + (lua_dir / f"{name}.lua").read_text(encoding="utf-8") for name in SCRIPT_NAMES}


SCRIPTS = load_scripts()


def describe(error: BaseException) -> str:
    """For logs. redis-py's repr hides the message ("network:ConnectionError"), which is the useful part."""
    return f"{type(error).__name__}: {error}"


def connect(redis_url: str) -> Redis:
    """REDIS_URL is `host:port` (what verify/run.sh passes); a full redis:// URL also works.

    A command whose connection breaks is retried on a fresh connection up to three times within
    about 0.35 s, so a Redis restart or a network blip between app and Redis goes unnoticed, while
    a Redis that is really down still turns into a 503 quickly. (StackExchange.Redis in the .NET
    version gets the same effect by queueing commands while it reconnects.) Timeouts are not
    retried: a Redis that is slow is not made slower.
    """
    url = redis_url if "://" in redis_url else f"redis://{redis_url}"
    pool = BlockingConnectionPool.from_url(
        url,
        max_connections=MAX_CONNECTIONS,
        timeout=POOL_WAIT_S,
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=SOCKET_TIMEOUT_S,
        retry=Retry(
            ExponentialBackoff(cap=RETRY_CAP_S, base=RETRY_BASE_S), RETRIES, supported_errors=(RedisConnectionError,)
        ),
    )
    return Redis(connection_pool=pool)


async def warm_up(client: Redis) -> int:
    """Opens every pooled connection now, one at a time, and returns how many.

    Otherwise the first flash crowd opens them all at once, at the moment the process is busiest,
    and the requests behind a slow connect wait for it. The .NET version has one multiplexed
    connection, opened at startup; this is the closest a pool gets to that.
    """
    pool = client.connection_pool
    held = []
    try:
        for _ in range(pool.max_connections):
            held.append(await pool.get_connection())
    finally:
        for connection in held:
            await pool.release(connection)
    return len(held)


class UnexpectedReply(RuntimeError):
    """A script and the Python side disagree. A bug: it becomes a 500."""


@dataclass(frozen=True)
class ScriptReply:
    """A script's reply: an outcome word, then name/value pairs."""

    script: str
    outcome: str
    fields: dict[str, str]

    @classmethod
    def parse(cls, script: str, raw: object) -> ScriptReply:
        if not isinstance(raw, list) or not raw:
            raise UnexpectedReply(f"{script}.lua returned {raw!r}")
        fields = {str(raw[i]): str(raw[i + 1]) for i in range(1, len(raw) - 1, 2)}
        return cls(script, str(raw[0]), fields)

    def find(self, name: str) -> str | None:
        return self.fields.get(name)

    def get(self, name: str) -> str:
        value = self.fields.get(name)
        if value is None:
            raise UnexpectedReply(f"{self.script}.lua reply '{self.outcome}' is missing '{name}'")
        return value

    def get_int(self, name: str) -> int:
        return int(self.get(name))

    def optional_int(self, name: str) -> int | None:
        value = self.find(name)
        return None if value is None else int(value)

    def unexpected(self) -> UnexpectedReply:
        """For outcomes a caller does not handle: these mean the script and the Python side disagree."""
        return UnexpectedReply(f"{self.script}.lua returned unexpected outcome '{self.outcome}'")


class SaleRedis:
    def __init__(self, client: Redis, config: Config, clock: Clock) -> None:
        self.client = client
        self.prefix = f"cq:{{{config.sale_id}}}:"
        # admit.lua publishes the ticket ids it just admitted here.
        self.admitted_channel = f"{self.prefix}admitted"
        self._clock = clock
        # EVALSHA, falling back to loading the script when Redis does not know it yet.
        self._scripts = {name: client.register_script(source) for name, source in SCRIPTS.items()}

    @property
    def now_ms(self) -> int:
        return self._clock.now_ms()

    async def ping(self) -> None:
        await self.client.ping()

    async def initialize(self, initial_stock: int) -> int:
        """Sets the stock if this sale has never started. Returns the stock the sale actually started with."""
        reply = await self.run("init", initial_stock)
        return reply.get_int("initialStock")

    async def run(self, name: str, *args: str | int) -> ScriptReply:
        """Runs a script with the sale prefix as ARGV[1], followed by `args`."""
        return ScriptReply.parse(name, await self._eval(name, args))

    async def run_rows(self, name: str, *args: str | int) -> list[ScriptReply]:
        """For scripts that return a list of replies, each shaped like a single script reply."""
        rows = await self._eval(name, args)
        if not isinstance(rows, list):
            raise UnexpectedReply(f"{name}.lua returned {rows!r}")
        return [ScriptReply.parse(name, row) for row in rows]

    async def _eval(self, name: str, args: Sequence[str | int]) -> object:
        return await self._scripts[name](keys=[], args=[self.prefix, *args])
