"""HTTP plumbing: the error envelope, the served-by header, strict request parsing, JSON replies."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .ids import is_valid_id
from .sale_redis import REDIS_ERRORS, describe

# Same cap as the Rust version (axum's default). Every valid body here is a few dozen bytes.
MAX_BODY_BYTES = 2 * 1024 * 1024

log = logging.getLogger(__name__)

JSON_MEDIA_TYPE = "application/json; charset=utf-8"
_USER_ID = re.compile(r"[A-Za-z0-9._@-]{1,64}")


class ApiError(Exception):
    """An error that maps to an HTTP status. The message is safe to show callers; the code is
    what a client branches on (several different 409s need different handling)."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message

    @classmethod
    def bad_request(cls, message: str) -> ApiError:
        return cls(400, "invalid_request", message)

    @classmethod
    def forbidden(cls, code: str, message: str) -> ApiError:
        return cls(403, code, message)

    @classmethod
    def not_found(cls, code: str, message: str) -> ApiError:
        return cls(404, code, message)

    @classmethod
    def conflict(cls, code: str, message: str) -> ApiError:
        return cls(409, code, message)

    @classmethod
    def gone(cls, code: str, message: str) -> ApiError:
        return cls(410, code, message)


def json_response(body: Any, status_code: int = 200, headers: dict[str, str] | None = None) -> Response:
    content = json.dumps(body, separators=(",", ":")).encode()
    return Response(content, status_code, headers, media_type=JSON_MEDIA_TYPE)


def error_response(status_code: int, code: str, message: str) -> Response:
    return json_response({"error": message, "code": code}, status_code)


def install_error_handlers(app: FastAPI) -> None:
    """Every error is `{"error": "...", "code": "..."}`. A Redis outage is a 503, not a 500, so
    clients know to retry rather than give up. FastAPI's 422 never happens: bodies are parsed by hand."""

    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, exc: ApiError) -> Response:
        return error_response(exc.status_code, exc.code, exc.message)

    async def handle_unavailable(request: Request, exc: Exception) -> Response:
        log.warning("Redis unavailable while serving %s %s: %s", request.method, request.url.path, describe(exc))
        return error_response(503, "unavailable", "storage unavailable, retry shortly")

    for error_type in REDIS_ERRORS:
        app.add_exception_handler(error_type, handle_unavailable)

    @app.exception_handler(HTTPException)
    async def handle_http_error(_: Request, exc: HTTPException) -> Response:
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return error_response(exc.status_code, code, str(exc.detail).lower())

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError) -> Response:
        return error_response(400, "invalid_request", "invalid request")


class ServedByMiddleware:
    """Tags every response with X-Served-By so callers can tell which instance answered.

    Plain ASGI rather than @app.middleware("http"), which adds measurable latency and buffers streams.
    """

    def __init__(self, app: ASGIApp, name: str) -> None:
        self._app = app
        self._header = (b"x-served-by", name.encode("latin-1"))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        async def send_tagged(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), self._header]}
            await send(message)

        await self._app(scope, receive, send_tagged)


class UnhandledErrorMiddleware:
    """A bug (say, a script outcome nobody handles) is a 500 with the usual envelope, inside the
    served-by middleware so it still names the instance. The exception is logged and re-raised."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        started = False

        async def send_tracked(message: Message) -> None:
            nonlocal started
            started = started or message["type"] == "http.response.start"
            await send(message)

        try:
            await self._app(scope, receive, send_tracked)
        except Exception:
            log.exception("Unhandled error serving %s %s", scope.get("method"), scope.get("path"))
            if started:
                raise
            await error_response(500, "internal", "internal server error")(scope, receive, send)


async def read_json_object(request: Request) -> dict[str, Any]:
    """Reads a JSON object body, turning every way it can be wrong into a 400. No type coercion."""
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media_type != "application/json" and not media_type.endswith("+json"):
        raise ApiError.bad_request("request body must be JSON")
    raw = await _read_capped(request)
    try:
        body = json.loads(raw, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:  # includes JSONDecodeError and UnicodeDecodeError
        raise ApiError.bad_request("invalid JSON body") from exc
    if body is None:
        raise ApiError.bad_request("request body is required")
    if not isinstance(body, dict):
        raise ApiError.bad_request("request body must be a JSON object")
    return body


def field(body: dict[str, Any], name: str) -> object:
    """A body field looked up ignoring case, like ASP.NET's JSON defaults in the .NET reference."""
    wanted = name.casefold()
    return next((value for key, value in body.items() if key.casefold() == wanted), None)


async def _read_capped(request: Request) -> bytes:
    """The body, refused once it passes MAX_BODY_BYTES instead of being held in memory whole."""
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and (len(declared) > len(str(MAX_BODY_BYTES)) or int(declared) > MAX_BODY_BYTES):
        raise ApiError.bad_request("request body is too large")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise ApiError.bad_request("request body is too large")
        chunks.append(chunk)
    return b"".join(chunks)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def valid_id(value: object, name: str) -> str:
    """Ticket and order ids are 32 lowercase hex characters; anything else is a 400, not a lookup."""
    if not is_valid_id(value):
        raise ApiError.bad_request(f"{name} must be 32 lowercase hex characters")
    return value  # type: ignore[return-value]


def valid_user_id(value: object) -> str:
    """Stands in for the authenticated user. Restricted to characters that cannot reshape a Redis key."""
    if not isinstance(value, str) or not _USER_ID.fullmatch(value):
        raise ApiError.bad_request("userId must be 1-64 letters, digits, '.', '_', '-' or '@'")
    return value


def valid_quantity(value: object, max_per_order: int) -> int:
    # bool is an int in Python; JSON true is not a quantity.
    if type(value) is not int or not 1 <= value <= max_per_order:
        raise ApiError.bad_request(f"quantity must be an integer from 1 to {max_per_order}")
    return value
