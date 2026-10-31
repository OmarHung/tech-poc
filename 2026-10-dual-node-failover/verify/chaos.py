"""Fault injection between processes on one machine.

Every link between the nodes and the witness goes through a TCP proxy, so a scenario can cut it
(blackhole: bytes vanish, connections hang until their timeouts fire, like a real partition) or
slow it down, without the implementation knowing. Clients and the gateway talk to the nodes directly.
Standard library only.
"""

from __future__ import annotations

import asyncio
import random
import threading
from dataclasses import dataclass, field

CHUNK = 65536
CALL_TIMEOUT_S = 5.0


@dataclass(eq=False)
class _Conn:
    client: asyncio.StreamWriter
    upstream: asyncio.StreamWriter | None = None
    frozen: bool = False

    def close(self) -> None:
        for writer in (self.client, self.upstream):
            if writer is not None:
                writer.close()


@dataclass
class Link:
    name: str
    listen_port: int
    target_port: int
    blocked: bool = False
    delay_s: tuple[float, float] = (0.0, 0.0)
    conns: set[_Conn] = field(default_factory=set)


class ChaosProxy:
    """One TCP proxy per link, all on a private event loop thread."""

    def __init__(self, links: dict[str, tuple[int, int]]) -> None:
        self.links = {name: Link(name, listen, target) for name, (listen, target) in links.items()}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="chaos-proxy", daemon=True)
        self._servers: list[asyncio.Server] = []

    def start(self) -> None:
        self._thread.start()
        self._call(self._start_servers())

    def stop(self) -> None:
        self._call(self._stop_servers())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=CALL_TIMEOUT_S)

    def block(self, *names: str) -> None:
        """Cuts the links: open connections stop delivering, new ones are accepted and then ignored."""
        self._call(self._set_blocked(names, True))

    def unblock(self, *names: str) -> None:
        """Restores the links. Connections that lived through the cut are closed, as a real peer would reset them."""
        self._call(self._set_blocked(names, False))

    def set_delay(self, names: tuple[str, ...], low_s: float, high_s: float) -> None:
        """Delays every chunk on the links by a random time in [low_s, high_s]. (0, 0) turns it off."""
        self._call(self._set_delay(names, (low_s, high_s)))

    def heal_all(self) -> None:
        names = tuple(self.links)
        self.set_delay(names, 0.0, 0.0)
        self.unblock(*names)

    def _call(self, coro) -> object:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(CALL_TIMEOUT_S)

    async def _start_servers(self) -> None:
        for link in self.links.values():
            server = await asyncio.start_server(
                lambda r, w, link=link: self._handle(link, r, w), "127.0.0.1", link.listen_port
            )
            self._servers.append(server)

    async def _stop_servers(self) -> None:
        for server in self._servers:
            server.close()
        for link in self.links.values():
            for conn in list(link.conns):
                conn.close()

    async def _set_blocked(self, names: tuple[str, ...], blocked: bool) -> None:
        for name in names:
            link = self.links[name]
            link.blocked = blocked
            for conn in list(link.conns):
                if blocked:
                    conn.frozen = True
                elif conn.frozen:
                    conn.close()

    async def _set_delay(self, names: tuple[str, ...], delay: tuple[float, float]) -> None:
        for name in names:
            self.links[name].delay_s = delay

    async def _handle(self, link: Link, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _Conn(writer, frozen=link.blocked)
        link.conns.add(conn)
        try:
            if conn.frozen:
                await self._drain_until_closed(reader)
                return
            try:
                up_reader, up_writer = await asyncio.open_connection("127.0.0.1", link.target_port)
            except OSError:
                return  # target is down: the caller sees the connection drop
            conn.upstream = up_writer
            pipes = [
                asyncio.ensure_future(self._pipe(link, conn, reader, up_writer)),
                asyncio.ensure_future(self._pipe(link, conn, up_reader, writer)),
            ]
            _, pending = await asyncio.wait(pipes, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
        finally:
            link.conns.discard(conn)
            conn.close()

    @staticmethod
    async def _drain_until_closed(reader: asyncio.StreamReader) -> None:
        try:
            while await reader.read(CHUNK):
                pass
        except OSError:
            pass

    @staticmethod
    async def _pipe(link: Link, conn: _Conn, src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
        """Copies src to dst. A delay works like a long cable: every chunk arrives `delay` after it was read, and
        chunks sent back to back still arrive back to back (sleeping per chunk instead would add the delay once per
        TCP write, penalising clients that send headers and body separately)."""
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[tuple[float, bytes] | None] = asyncio.Queue()
        sender = asyncio.ensure_future(ChaosProxy._deliver(conn, queue, dst))
        try:
            last_due = 0.0
            while data := await src.read(CHUNK):
                if conn.frozen:
                    continue  # blackholed
                low, high = link.delay_s
                delay = random.uniform(low, high) if high > 0 else 0.0
                last_due = max(loop.time() + delay, last_due)  # never overtake an earlier chunk
                queue.put_nowait((last_due, data))
            queue.put_nowait(None)
            await sender
        except OSError:
            pass
        finally:
            sender.cancel()

    @staticmethod
    async def _deliver(conn: _Conn, queue: asyncio.Queue[tuple[float, bytes] | None], dst: asyncio.StreamWriter) -> None:
        loop = asyncio.get_running_loop()
        try:
            while (item := await queue.get()) is not None:
                due, data = item
                if (wait := due - loop.time()) > 0:
                    await asyncio.sleep(wait)
                if conn.frozen:
                    continue
                dst.write(data)
                await dst.drain()
        except OSError:
            pass
