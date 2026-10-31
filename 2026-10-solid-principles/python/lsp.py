"""L：里氏替換原則（Liskov Substitution Principle）。

子型別要能替換父型別，呼叫端不用知道拿到的是哪一種。
DiscountRule 的契約是 0 <= discount(subtotal) <= subtotal，
total_after_discount() 靠這個契約保證總額不會是負數。

情境：新增「折 200 元」。小計只有 150 元的訂單套用後，總額是多少？
"""

from collections.abc import Iterable
from dataclasses import dataclass

from ocp import DiscountRule

SAMPLE_SUBTOTALS = (0, 150, 999, 1000, 5000)


# --- 違反：不管小計多少都折 amount，小計不夠折時總額變負數 ---


@dataclass(frozen=True)
class UncappedAmountOff:
    amount: int

    def discount(self, subtotal: int) -> int:
        return self.amount


# --- 遵守：最多折到 0 元 ---


@dataclass(frozen=True)
class FixedAmountOff:
    amount: int

    def discount(self, subtotal: int) -> int:
        return min(self.amount, subtotal)


def contract_violations(
    rule: DiscountRule, subtotals: Iterable[int] = SAMPLE_SUBTOTALS
) -> tuple[int, ...]:
    """回傳讓 rule 違反契約的小計。空的代表這組樣本都守住了契約。"""
    return tuple(
        subtotal
        for subtotal in subtotals
        if not 0 <= rule.discount(subtotal) <= subtotal
    )
