"""Errors that map to an HTTP status. Their messages are safe to return to callers."""

from __future__ import annotations


class ApiError(Exception):
    status_code = 500

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class InvalidInput(ApiError):
    status_code = 400


class NotFound(ApiError):
    status_code = 404


class Conflict(ApiError):
    status_code = 409


class PayloadTooLarge(ApiError):
    status_code = 413


class Unavailable(ApiError):
    """The request was valid but this process cannot serve it right now; retrying (maybe elsewhere) can work."""

    status_code = 503


class ConfigError(ValueError):
    """A setting is wrong. The message names the environment variable to fix."""
