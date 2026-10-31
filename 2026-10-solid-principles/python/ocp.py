"""O：開放封閉原則（Open-Closed Principle）。

對擴充開放、對修改封閉：加新功能時是寫新的程式碼，不是回去改已經能用的程式碼。

情境：原本有「VIP 九折」「員工八折」，行銷部新增「滿 1000 折 100」。
"""

from dataclasses import dataclass
from typing import Protocol

# --- 違反：每多一種折扣，就要回來改這串 if ---


def legacy_total_after_discount(subtotal: int, kind: str) -> int:
    if kind == "none":
        return subtotal
    if kind == "vip":
        return subtotal - subtotal // 10
    if kind == "staff":
        return subtotal - subtotal // 5
    raise ValueError(f"不支援的折扣：{kind}")


# --- 遵守：折扣是一個介面，新折扣就是新類別，total_after_discount() 不用改 ---


class DiscountRule(Protocol):
    """契約：0 <= discount(subtotal) <= subtotal。lsp.py 會檢查這件事。"""

    def discount(self, subtotal: int) -> int: ...


@dataclass(frozen=True)
class NoDiscount:
    def discount(self, subtotal: int) -> int:
        return 0


@dataclass(frozen=True)
class PercentOff:
    percent: int

    def discount(self, subtotal: int) -> int:
        return subtotal * self.percent // 100


def total_after_discount(subtotal: int, rule: DiscountRule) -> int:
    return subtotal - rule.discount(subtotal)
