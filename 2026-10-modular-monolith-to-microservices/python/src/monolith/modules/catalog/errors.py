"""Each error knows its HTTP status, so the remote client can turn a status code
plus message back into the same exception the in-process catalog would raise."""


class CatalogError(Exception):
    status_code = 500

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class InvalidInput(CatalogError):
    status_code = 400


class NotFound(CatalogError):
    status_code = 404


class InsufficientStock(CatalogError):
    status_code = 409


class CatalogUnavailable(CatalogError):
    status_code = 502


def product_not_found(product_id: int) -> NotFound:
    return NotFound(f"product {product_id} not found")


def insufficient_stock(product_id: int, requested: int, available: int) -> InsufficientStock:
    return InsufficientStock(
        f"insufficient stock for product {product_id}: requested {requested}, available {available}"
    )
