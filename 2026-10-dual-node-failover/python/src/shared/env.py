"""Reads settings from environment-style lookups and fails with the variable name to fix."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from urllib.parse import urlsplit

from shared.errors import ConfigError

Lookup = Callable[[str], str | None]

_DIGITS = re.compile(r"[0-9]+")


class EnvReader:
    def __init__(self, lookup: Lookup) -> None:
        self._lookup = lookup

    def raw(self, name: str) -> str | None:
        """The value, or None when the variable is unset or empty."""
        return self._lookup(name) or None

    def string(self, name: str, fallback: str) -> str:
        return self.raw(name) or fallback

    def port(self, name: str, fallback: int) -> int:
        return self.integer(name, fallback, 1, 65535)

    def integer(self, name: str, fallback: int, minimum: int, maximum: int) -> int:
        raw = self.raw(name)
        if raw is None:
            return fallback
        if _DIGITS.fullmatch(raw) and minimum <= int(raw) <= maximum:
            return int(raw)
        raise ConfigError(f"{name} must be an integer between {minimum} and {maximum}, got '{raw}'")

    def url(self, name: str, fallback: str) -> str:
        raw = self.raw(name)
        return fallback if raw is None else parse_url(name, raw)

    def choice[T](self, name: str, fallback: T, options: Mapping[str, T]) -> T:
        raw = self.raw(name)
        if raw is None:
            return fallback
        if raw in options:
            return options[raw]
        raise ConfigError(f"{name} must be one of {', '.join(options)}, got '{raw}'")


def parse_url(name: str, raw: str) -> str:
    try:
        parts = urlsplit(raw)
        valid = parts.scheme in ("http", "https") and bool(parts.hostname) and (parts.port or 0) >= 0
    except ValueError:  # e.g. a port that is not a number
        valid = False
    if not valid:
        raise ConfigError(f"{name} must be an http(s) URL, got '{raw}'")
    return raw
