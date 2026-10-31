import re
import secrets

_ID = re.compile(r"[0-9a-f]{32}")


def new_id() -> str:
    """128 random bits as 32 lowercase hex characters. Holding a ticket id is what lets you
    check out, so it must not be guessable from someone else's."""
    return secrets.token_hex(16)


def is_valid_id(value: object) -> bool:
    return isinstance(value, str) and _ID.fullmatch(value) is not None
