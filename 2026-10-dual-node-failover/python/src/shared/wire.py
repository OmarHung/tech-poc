"""JSON the way the .NET version reads it, so both implementations accept and refuse the same requests.

Property names match case-insensitively and unknown ones are ignored. Numbers must be whole and fit in 64 bits
("2" or 2.0 is not 2), strings must be strings, and the first wrong type raises WireError with its JSON path.
"""

from __future__ import annotations

import json
import re

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
_NODE_ID = re.compile(r"[a-z0-9-]{1,32}")


class WireError(ValueError):
    def __init__(self, path: str) -> None:
        super().__init__(f"invalid JSON at {path}")
        self.path = path


def is_node_id(value: object) -> bool:
    """Node ids appear in URLs, file names and logs, so they are kept to a safe alphabet."""
    return isinstance(value, str) and _NODE_ID.fullmatch(value) is not None


def _reject_constant(name: str) -> object:
    raise ValueError(f"{name} is not JSON")


def loads(data: bytes | str) -> object:
    """Standard JSON only: no NaN or Infinity, UTF-8 only. Anything else raises WireError("$")."""
    try:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        return json.loads(text, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as ex:  # ValueError covers JSONDecodeError and UnicodeDecodeError
        raise WireError("$") from ex


def dumps(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _is_text(value: str) -> bool:
    """False for lone surrogates ("\\ud800"), which JSON allows but no UTF-8 response can carry."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


class JsonObject:
    """One JSON object, read field by field with strict types."""

    def __init__(self, value: object, path: str = "$") -> None:
        if not isinstance(value, dict):
            raise WireError(path)
        self._fields = {str(name).lower(): field for name, field in value.items()}
        self._path = path

    def path(self, name: str) -> str:
        return f"{self._path}.{name}"

    def has(self, name: str) -> bool:
        return name.lower() in self._fields

    def string(self, name: str) -> str | None:
        value = self._fields.get(name.lower())
        if value is None:
            return None
        if not isinstance(value, str) or not _is_text(value):
            raise WireError(self.path(name))
        return value

    def integer(self, name: str, default: int = 0) -> int:
        if not self.has(name):
            return default
        value = self._fields[name.lower()]
        # type() rather than isinstance(): bool is a subclass of int, and true is not 1.
        if type(value) is not int or not INT64_MIN <= value <= INT64_MAX:
            raise WireError(self.path(name))
        return value

    def boolean(self, name: str, default: bool = False) -> bool:
        if not self.has(name):
            return default
        value = self._fields[name.lower()]
        if not isinstance(value, bool):
            raise WireError(self.path(name))
        return value

    def array(self, name: str) -> list[object] | None:
        value = self._fields.get(name.lower())
        if value is None:
            return None
        if not isinstance(value, list):
            raise WireError(self.path(name))
        return value
