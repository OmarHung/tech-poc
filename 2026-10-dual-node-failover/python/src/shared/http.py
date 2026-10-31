"""HTTP plumbing shared by every server here (the gateway tags its own responses)."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from shared import wire
from shared.errors import ApiError, InvalidInput, PayloadTooLarge
from shared.wire import INT64_MAX, INT64_MIN

SERVED_BY = "X-Served-By"
MAX_BODY_BYTES = 30_000_000
"""Kestrel's default request body limit; anything bigger is a 413 instead of a process out of memory."""
_SERVED_BY_RAW = SERVED_BY.lower().encode()
# What .NET's long.TryParse accepts for query values and route segments: an optional sign, digits, spaces around.
_QUERY_INTEGER = re.compile(r"\s*[+-]?[0-9]+\s*")

logger = logging.getLogger(__name__)


class ServedBy:
    """Tags every response with X-Served-By so callers can tell who answered."""

    def __init__(self, app: ASGIApp, name: str) -> None:
        self._app = app
        self._value = name.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        async def tagged(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [(k, v) for k, v in message.get("headers", []) if k.lower() != _SERVED_BY_RAW]
                message = {**message, "headers": [*headers, (_SERVED_BY_RAW, self._value)]}
            await send(message)

        await self._app(scope, receive, tagged)


def error_response(status: int, message: str, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status, headers=headers)


def install_error_handlers(app: FastAPI) -> None:
    """Every error is {"error": "..."}: ApiError with its status, framework errors (404, 405, validation) too."""

    async def api_error(request: Request, ex: Exception) -> JSONResponse:
        if isinstance(ex, ApiError):
            return error_response(ex.status_code, ex.message)
        return await unexpected(request, ex)

    async def validation_error(_: Request, __: Exception) -> JSONResponse:
        return error_response(400, "invalid request")

    async def http_error(request: Request, ex: Exception) -> JSONResponse:
        if isinstance(ex, StarletteHTTPException):
            return error_response(ex.status_code, str(ex.detail), dict(ex.headers or {}))
        return await unexpected(request, ex)

    async def unexpected(request: Request, ex: Exception) -> JSONResponse:
        logger.error("Unhandled error on %s %s: %r", request.method, request.url.path, ex, exc_info=ex)
        return error_response(500, "internal error")

    app.add_exception_handler(ApiError, api_error)
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(StarletteHTTPException, http_error)
    app.add_exception_handler(Exception, unexpected)


def is_json_content_type(value: str | None) -> bool:
    media = (value or "").split(";", 1)[0].strip().lower()
    return media == "application/json" or media.endswith("+json")


async def read_capped_body(request: Request) -> bytes:
    limit = MAX_BODY_BYTES
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > limit:
        raise PayloadTooLarge("request body too large")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise PayloadTooLarge("request body too large")
        chunks.append(chunk)
    return b"".join(chunks)


async def read_body[T](request: Request, parse: Callable[[object], T]) -> T:
    """Reads a JSON body into a contract, turning every way it can be wrong into a 400 (or 413 if it is huge)."""
    if not is_json_content_type(request.headers.get("content-type")):
        raise InvalidInput("request body must be JSON")
    try:
        value = wire.loads(await read_capped_body(request))
        if value is None:
            raise InvalidInput("request body is required")
        return parse(value)
    except wire.WireError as ex:
        raise InvalidInput(f"invalid JSON body at {ex.path}") from ex


def parse_integer(raw: str, minimum: int = INT64_MIN, maximum: int = INT64_MAX) -> int | None:
    """A query value or route segment as an integer in [minimum, maximum], or None if it is not one."""
    if _QUERY_INTEGER.fullmatch(raw) is None:
        return None
    value = int(raw)
    return value if minimum <= value <= maximum else None


def query_integer(request: Request, name: str, default: int, minimum: int = INT64_MIN, maximum: int = INT64_MAX) -> int:
    """An optional integer query parameter. Names match case-insensitively and an empty value counts as absent, the
    way ASP.NET binds `long? after`; anything else that is not an integer is a 400."""
    raw = next((value for key, value in request.query_params.multi_items() if key.lower() == name.lower()), "")
    if raw == "":
        return default
    value = parse_integer(raw, minimum, maximum)
    if value is None:
        raise InvalidInput(f"{name} must be an integer between {minimum} and {maximum}")
    return value


async def until_disconnected[T](request: Request, work: Awaitable[T]) -> T | None:
    """Runs a long-poll, but cancels it if the caller hangs up first, like ASP.NET's RequestAborted. Without this a
    task could be leased to a worker that already gave up waiting (or died), and sit there until its lease ran out.
    None means the caller is gone; whatever is sent back then goes nowhere."""
    working = asyncio.ensure_future(work)
    hung_up = asyncio.ensure_future(_disconnect(request))
    try:
        await asyncio.wait((working, hung_up), return_when=asyncio.FIRST_COMPLETED)
    finally:
        hung_up.cancel()
        if not working.done():
            working.cancel()
        await asyncio.wait((working, hung_up))
    return None if working.cancelled() else working.result()


async def _disconnect(request: Request) -> None:
    """Returns once the server reports the client gone. Only call after the body was read: until then receive()
    hands out body chunks."""
    while (await request.receive())["type"] != "http.disconnect":
        pass
