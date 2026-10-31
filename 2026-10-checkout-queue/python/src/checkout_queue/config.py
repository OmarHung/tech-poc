"""Settings for one process. Every instance pointed at the same Redis and SALE_ID serves the
same sale, so scaling out is just starting more processes."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass

MAX_DURATION_MS = 24 * 60 * 60 * 1000
_DIGITS = re.compile(r"[0-9]+")
_SALE_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")

Lookup = Callable[[str], str | None]


class ConfigError(ValueError):
    """A bad setting. The message names the environment variable to fix."""


@dataclass(frozen=True)
class Config:
    port: int = 8090
    redis_url: str = "127.0.0.1:6379"
    instance_name: str = "checkout-queue"
    sale_id: str = "demo"
    initial_stock: int = 100
    max_per_order: int = 2
    # How many admitted users may be in checkout at once. This is the knob that flattens the spike.
    max_active: int = 50
    # How long an admitted user has to place the order before the slot goes to the next in line.
    admission_window_ms: int = 120_000
    # How long an order holds its stock waiting for payment.
    payment_window_ms: int = 300_000
    # A waiting user who has not polled for this long is skipped when their turn comes.
    heartbeat_timeout_ms: int = 15_000
    # How often clients are told to poll their ticket; also how often event streams refresh.
    poll_interval_ms: int = 1_000
    # How often each instance runs admission and releases unpaid orders.
    tick_ms: int = 200

    @classmethod
    def from_env(cls) -> Config:
        return cls.from_lookup(os.environ.get)

    @classmethod
    def from_lookup(cls, env: Lookup) -> Config:
        d = cls()
        config = cls(
            port=_int(env, "PORT", d.port, 1, 65535),
            redis_url=_str(env, "REDIS_URL", d.redis_url),
            instance_name=_str(env, "INSTANCE_NAME", d.instance_name),
            sale_id=_str(env, "SALE_ID", d.sale_id),
            initial_stock=_int(env, "INITIAL_STOCK", d.initial_stock, 0, 10_000_000),
            max_per_order=_int(env, "MAX_PER_ORDER", d.max_per_order, 1, 100),
            max_active=_int(env, "MAX_ACTIVE", d.max_active, 1, 1_000_000),
            admission_window_ms=_millis(env, "ADMISSION_WINDOW_MS", d.admission_window_ms),
            payment_window_ms=_millis(env, "PAYMENT_WINDOW_MS", d.payment_window_ms),
            heartbeat_timeout_ms=_millis(env, "HEARTBEAT_TIMEOUT_MS", d.heartbeat_timeout_ms),
            poll_interval_ms=_millis(env, "POLL_INTERVAL_MS", d.poll_interval_ms),
            tick_ms=_millis(env, "TICK_MS", d.tick_ms),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Checks the rules that span several settings. Raises naming the variable to fix."""
        if not self.redis_url.strip():
            raise ConfigError("REDIS_URL must not be empty")
        if not _SALE_ID.fullmatch(self.sale_id):
            raise ConfigError(f"SALE_ID must be 1-64 letters, digits, '-' or '_', got '{self.sale_id}'")
        # A client that misses a poll or two (GC pause, flaky mobile network) must not lose its place.
        if self.heartbeat_timeout_ms < 3 * self.poll_interval_ms:
            raise ConfigError(
                f"HEARTBEAT_TIMEOUT_MS ({self.heartbeat_timeout_ms}) must be at least "
                f"3 x POLL_INTERVAL_MS ({self.poll_interval_ms})"
            )


def _str(env: Lookup, name: str, fallback: str) -> str:
    raw = env(name)
    return fallback if raw is None else raw


def _int(env: Lookup, name: str, fallback: int, low: int, high: int) -> int:
    raw = env(name)
    if raw is None:
        return fallback
    # Plain ASCII digits only: no sign, no spaces, no decimals, no other scripts' digits.
    # Length first: int() refuses strings past ~4300 digits, and no valid value is that long anyway.
    if _DIGITS.fullmatch(raw) and len(raw) <= len(str(high)) and low <= int(raw) <= high:
        return int(raw)
    raise ConfigError(f"{name} must be an integer between {low} and {high}, got '{raw}'")


def _millis(env: Lookup, name: str, fallback: int) -> int:
    return _int(env, name, fallback, 1, MAX_DURATION_MS)
