"""S：單一職責原則（Single Responsibility Principle）。

一個模組只該為一種角色（actor）改變。這裡有兩種角色：
財務在意「發票金額」，物流在意「免不免運」。

情境：財務要求發票金額改成含 5% 營業稅。物流的規則沒變：未稅小計滿 1000 元免運。
"""

from collections.abc import Iterable
from dataclasses import dataclass

TAX_RATE_PERCENT = 5
FREE_SHIPPING_THRESHOLD = 1000


@dataclass(frozen=True)
class LineItem:
    name: str
    unit_price: int
    quantity: int


# --- 違反：一個類別同時服務財務和物流，兩邊共用 _amount() ---


@dataclass(frozen=True)
class LegacyOrder:
    items: tuple[LineItem, ...]

    def invoice_amount(self) -> int:  # 財務
        return self._amount()

    def ships_free(self) -> bool:  # 物流
        return self._amount() >= FREE_SHIPPING_THRESHOLD

    def _amount(self) -> int:
        # 為了財務的需求在這裡加上稅，用到這個函式的免運判斷也跟著變了
        base = sum(item.unit_price * item.quantity for item in self.items)
        return base + base * TAX_RATE_PERCENT // 100


# --- 遵守：財務和物流各有自己的類別，改一邊不會動到另一邊 ---


def subtotal(items: Iterable[LineItem]) -> int:
    """未稅小計。兩種角色都認同的事實，不帶任何一方的規則，所以可以共用。"""
    return sum(item.unit_price * item.quantity for item in items)


class InvoiceCalculator:  # 財務
    def amount(self, items: Iterable[LineItem]) -> int:
        base = subtotal(items)
        return base + base * TAX_RATE_PERCENT // 100


class ShippingPolicy:  # 物流
    def ships_free(self, items: Iterable[LineItem]) -> bool:
        return subtotal(items) >= FREE_SHIPPING_THRESHOLD
