"""GET /queue/tickets/{ticketId}/events: the ticket as server-sent events, the push alternative to
polling. Each event's data is the same JSON as GET /queue/tickets/{ticketId}. The stream ends once
the ticket can no longer change, or as soon as the client goes away.
"""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

from .api import ticket_not_found
from .services import Services
from .streams import TicketStream
from .tickets import TicketView
from .web import valid_id

# Proxies and load balancers drop connections that stay silent for too long.
KEEP_ALIVE_S = 15.0
KEEP_ALIVE = b": keep-alive\n\n"
HEADERS = {"content-type": "text/event-stream", "cache-control": "no-cache", "x-accel-buffering": "no"}


def create_events_router(services: Services) -> APIRouter:
    router = APIRouter()

    @router.get("/queue/tickets/{ticket_id}/events")
    async def ticket_events(ticket_id: str) -> Response:
        tid = valid_id(ticket_id, "ticket id")
        # Open the stream before reading the ticket, so no change can slip in between the two.
        stream = services.streams.open(tid)
        try:
            view = await services.queue.get_ticket(tid)
        except BaseException:
            stream.close()
            raise
        if view is None:
            stream.close()
            raise ticket_not_found()
        return TicketEventStream(stream, view)

    return router


class TicketEventStream(Response):
    """Writes events until the ticket is final, while watching for the client to disconnect.

    Starlette's StreamingResponse only notices a disconnect on some ASGI versions; here the
    disconnect watch is explicit, because an open stream counts as the ticket's heartbeat.
    """

    def __init__(self, stream: TicketStream, first: TicketView) -> None:
        # Not Response.__init__: that would add Content-Length, and this body is open-ended (chunked).
        self.status_code = 200
        self.background = None
        self.raw_headers = [(name.encode(), value.encode()) for name, value in HEADERS.items()]
        self._stream = stream
        self._first = first

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        with self._stream:
            await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
            writer = asyncio.ensure_future(self._write_events(send))
            watcher = asyncio.ensure_future(_wait_for_disconnect(receive))
            try:
                done, _ = await asyncio.wait({writer, watcher}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                writer.cancel()
                watcher.cancel()
                await asyncio.gather(writer, watcher, return_exceptions=True)
            if writer in done:
                writer.result()  # a bug in writing surfaces here
                await send({"type": "http.response.body", "body": b"", "more_body": False})
            # Otherwise the client went away; closing the stream is all there is to do.

    async def _write_events(self, send: Send) -> None:
        view = self._first
        await _send_chunk(send, _event(view))
        while not view.is_final:
            pushed = await self._stream.next(KEEP_ALIVE_S)
            if pushed is None:
                await _send_chunk(send, KEEP_ALIVE)
            elif pushed != view:
                view = pushed
                await _send_chunk(send, _event(view))


def _event(view: TicketView) -> bytes:
    return b"data: " + json.dumps(view.to_json(), separators=(",", ":")).encode() + b"\n\n"


async def _send_chunk(send: Send, chunk: bytes) -> None:
    await send({"type": "http.response.body", "body": chunk, "more_body": True})


async def _wait_for_disconnect(receive: Receive) -> None:
    while (await receive())["type"] != "http.disconnect":
        pass
