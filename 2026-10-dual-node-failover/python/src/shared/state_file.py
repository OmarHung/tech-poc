"""Small JSON state files that survive a killed process."""

from __future__ import annotations

import os
from pathlib import Path

from shared import wire


def load(path: Path) -> object | None:
    """None if the file does not exist. A file that exists but does not parse is an error, never a guess."""
    if not path.exists():
        return None
    try:
        value = wire.loads(path.read_bytes())
    except wire.WireError as ex:
        raise ValueError(f"{path} is corrupt") from ex
    if value is None:
        raise ValueError(f"{path} is empty")
    return value


def save(path: Path, value: object) -> None:
    """Writes a temp file and renames it over the old one, so a crash leaves the old or the new state, never a mix."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(wire.dumps(value))
    os.replace(temp, path)
