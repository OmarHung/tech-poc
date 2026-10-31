"""Scripts and JSON both use snake_case status words; JSON leaves out fields without a value."""

from typing import Any

from .sale_redis import UnexpectedReply


def parse_status(wire: str, allowed: frozenset[str]) -> str:
    if wire not in allowed:
        raise UnexpectedReply(f"unknown status '{wire}'")
    return wire


def without_nulls(**fields: Any) -> dict[str, Any]:
    return {name: value for name, value in fields.items() if value is not None}
