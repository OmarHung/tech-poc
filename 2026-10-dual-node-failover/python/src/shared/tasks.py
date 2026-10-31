"""Background tasks that cannot fail unnoticed."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)

FatalHandler = Callable[[BaseException], None]

# asyncio keeps only weak references to tasks; these keep running ones alive until they finish.
_running: set[asyncio.Task[Any]] = set()


def spawn[T](coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
    """Starts a task that logs its own failure, since nobody may be awaiting it."""
    task = asyncio.create_task(coro, name=name)
    _running.add(task)
    task.add_done_callback(_finished)
    return task


def _finished(task: asyncio.Task[Any]) -> None:
    _running.discard(task)
    if not task.cancelled() and (error := task.exception()) is not None:
        logger.error("Task %s failed: %r", task.get_name(), error, exc_info=error)


def stop_process(error: BaseException) -> None:
    """What ASP.NET does when a hosted service throws: shut the whole process down (gracefully, via SIGTERM).
    A node whose election loop died would otherwise look alive while never renewing or stepping down."""
    os.kill(os.getpid(), signal.SIGTERM)


async def supervise(name: str, loop: Callable[[], Awaitable[None]], on_fatal: FatalHandler) -> None:
    """Runs a loop that should only end when its own task is cancelled (shutdown). Anything else, including a
    CancelledError that was not aimed at this task, is logged and handed to on_fatal."""
    try:
        await loop()
        error: BaseException = RuntimeError(f"{name} returned")
    except asyncio.CancelledError as ex:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
        error = ex
    except Exception as ex:
        error = ex
    logger.critical("%s stopped unexpectedly: %r; shutting down", name, error, exc_info=error)
    on_fatal(error)
