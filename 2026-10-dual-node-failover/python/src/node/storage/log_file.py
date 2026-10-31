from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from node.storage.log_entry import LogEntry
from shared import wire


class CorruptLogError(ValueError):
    pass


class LogFile:
    """JSON-lines file behind ReplicatedLog. Every append goes to the OS with os.write before it returns, so a killed
    process loses nothing it acknowledged. Power loss is out of scope: there is no fsync.

    Unbuffered on purpose: a failed write (disk full) leaves nothing queued in a Python buffer that a later append
    could flush behind the next entry. Instead the file is cut back to its last complete line."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd = _open_for_append(path)
        self._size = os.fstat(self._fd).st_size

    @staticmethod
    def open(path: Path) -> tuple[LogFile, list[LogEntry]]:
        path.parent.mkdir(parents=True, exist_ok=True)
        entries: list[LogEntry] = []
        if path.exists():
            data = path.read_bytes()
            lines = _split_lines(data)
            for number, line in enumerate(lines):
                entry = _parse(line)
                if entry is not None and entry.index == len(entries) + 1:
                    entries.append(entry)
                    continue
                # A process killed mid-write leaves at most one partial line, and only at the end.
                if number != len(lines) - 1:
                    raise CorruptLogError(f"{path} line {number + 1} is corrupt")
            # Rewrite if the last line was dropped, or was complete but lost its newline: the next append must start
            # on a line of its own.
            if len(entries) != len(lines) or (data and not data.endswith(b"\n")):
                _write_all(path, entries)
        return LogFile(path), entries

    def append(self, entries: Sequence[LogEntry]) -> None:
        data = _serialize(entries)
        try:
            _write_fully(self._fd, data)
        except OSError:
            os.ftruncate(self._fd, self._size)  # drop a partial line; the caller's write fails, nothing is half-kept
            raise
        self._size += len(data)

    def rewrite(self, entries: Sequence[LogEntry]) -> None:
        """Replaces the whole file. Used when a rejoining node drops entries the new primary never had."""
        os.close(self._fd)
        _write_all(self._path, entries)
        self._fd = _open_for_append(self._path)
        self._size = os.fstat(self._fd).st_size

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1


def _open_for_append(path: Path) -> int:
    return os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)


def _write_fully(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


def _split_lines(data: bytes) -> list[bytes]:
    lines = data.split(b"\n")
    return lines[:-1] if lines[-1] == b"" else lines


def _parse(line: bytes) -> LogEntry | None:
    try:
        return LogEntry.from_wire(wire.loads(line))
    except ValueError:
        return None


def _serialize(entries: Sequence[LogEntry]) -> bytes:
    return b"".join(wire.dumps(entry.to_wire()) + b"\n" for entry in entries)


def _write_all(path: Path, entries: Sequence[LogEntry]) -> None:
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(_serialize(entries))
    os.replace(temp, path)
