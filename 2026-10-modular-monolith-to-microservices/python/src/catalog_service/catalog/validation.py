from .errors import InvalidInput

MAX_QUANTITY = 100


def validate_quantity(quantity: int) -> int:
    if 1 <= quantity <= MAX_QUANTITY:
        return quantity
    raise InvalidInput(f"quantity must be between 1 and {MAX_QUANTITY}")


def validate_id(value: int, field: str) -> int:
    if value >= 1:
        return value
    raise InvalidInput(f"{field} must be a positive integer")


def parse_id(raw: str, field: str) -> int:
    """Parses an id taken from a URL path segment."""
    if not (raw.isascii() and raw.isdigit()):
        raise InvalidInput(f"{field} must be a positive integer")
    return validate_id(int(raw), field)
