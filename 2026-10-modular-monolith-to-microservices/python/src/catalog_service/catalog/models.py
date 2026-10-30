from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class ApiModel(BaseModel):
    """camelCase on the wire, strict types (no "2" -> 2 coercion), immutable."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
        strict=True,
        frozen=True,
    )


class Product(ApiModel):
    id: int
    name: str
    price: int
    stock: int


class ReserveRequest(ApiModel):
    quantity: int


def seed_products() -> list[Product]:
    """Same seed data in every language, so the verify scenario sees the same catalog.
    Product 3 has a huge stock so load tests never run out."""
    return [
        Product(id=1, name="Mechanical Keyboard", price=3200, stock=50),
        Product(id=2, name="Wireless Mouse", price=1500, stock=100),
        Product(id=3, name="USB-C Cable", price=290, stock=1_000_000),
    ]
