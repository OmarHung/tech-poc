"""The server-sent event streams open on this instance, by ticket.

A stream only ever needs the latest view, so an unsent older one is simply replaced
(a one-slot mailbox that drops the oldest). Everything runs on one event loop: no locks.
"""

from __future__ import annotations

import asyncio

from .tickets import TicketView


class TicketStream:
    """One open stream's mailbox. Close it (or use it as a context manager) to unregister."""

    def __init__(self, streams: TicketStreams, ticket_id: str) -> None:
        self.ticket_id = ticket_id
        self._streams = streams
        self._latest: TicketView | None = None
        self._ready = asyncio.Event()
        self._closed = False

    def offer(self, view: TicketView) -> None:
        self._latest = view
        self._ready.set()

    async def next(self, timeout_s: float) -> TicketView | None:
        """The next pushed view, or None when `timeout_s` passes with nothing (time for a keep-alive)."""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout_s)
        except TimeoutError:
            return None
        self._ready.clear()
        view, self._latest = self._latest, None
        return view

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._streams._close(self)

    def __enter__(self) -> TicketStream:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class TicketStreams:
    def __init__(self) -> None:
        self._open: dict[str, list[TicketStream]] = {}

    def open(self, ticket_id: str) -> TicketStream:
        stream = TicketStream(self, ticket_id)
        self._open.setdefault(ticket_id, []).append(stream)
        return stream

    @property
    def ticket_ids(self) -> list[str]:
        return list(self._open)

    def publish(self, view: TicketView) -> None:
        for stream in self._open.get(view.ticket_id, ()):
            stream.offer(view)

    def _close(self, stream: TicketStream) -> None:
        streams = self._open.get(stream.ticket_id)
        if streams is None:
            return
        streams.remove(stream)
        if not streams:
            del self._open[stream.ticket_id]
