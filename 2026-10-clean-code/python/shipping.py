"""運費計算，legacy.py 的 calc() 照 Clean Code 原則重構後的版本。

規則：
- 重量 1000 公克以內 60 元，5000 公克以內 100 元，超過的部分每 1000 公克（不足也算）加 20 元
- 寄離島加 80 元
- 本島訂單滿門檻免運：一般會員 1000 元、金卡會員 500 元。離島不提供免運
- 冷藏另外收費：本島 50 元、離島 100 元，免運也照收
"""

from dataclasses import dataclass
from enum import Enum


class Region(Enum):
    MAIN_ISLAND = "main"
    OUTLYING_ISLAND = "outlying"


class Membership(Enum):
    NORMAL = "normal"
    GOLD = "gold"


@dataclass(frozen=True)
class Parcel:
    weight_grams: int
    region: Region
    membership: Membership
    order_amount: int
    needs_cold_chain: bool


class InvalidParcelError(ValueError):
    pass


LIGHT_PARCEL_MAX_GRAMS = 1_000
LIGHT_PARCEL_FEE = 60
STANDARD_PARCEL_MAX_GRAMS = 5_000
STANDARD_PARCEL_FEE = 100
GRAMS_PER_KILOGRAM = 1_000
FEE_PER_EXTRA_KILOGRAM = 20
OUTLYING_ISLAND_SURCHARGE = 80
FREE_SHIPPING_THRESHOLD = {Membership.NORMAL: 1_000, Membership.GOLD: 500}
COLD_CHAIN_FEE = {Region.MAIN_ISLAND: 50, Region.OUTLYING_ISLAND: 100}


def shipping_fee(parcel: Parcel) -> int:
    _reject_invalid(parcel)
    return _delivery_fee(parcel) + _cold_chain_fee(parcel)


def _reject_invalid(parcel: Parcel) -> None:
    if parcel.weight_grams <= 0:
        raise InvalidParcelError(f"重量必須大於 0 公克，收到 {parcel.weight_grams}")
    if parcel.order_amount < 0:
        raise InvalidParcelError(f"訂單金額不能是負數，收到 {parcel.order_amount}")


def _delivery_fee(parcel: Parcel) -> int:
    if _ships_free(parcel):
        return 0
    return _weight_fee(parcel.weight_grams) + _region_surcharge(parcel.region)


def _ships_free(parcel: Parcel) -> bool:
    # 離島的物流成本高，行銷部決定不開放免運
    if parcel.region is Region.OUTLYING_ISLAND:
        return False
    return parcel.order_amount >= FREE_SHIPPING_THRESHOLD[parcel.membership]


def _weight_fee(weight_grams: int) -> int:
    if weight_grams <= LIGHT_PARCEL_MAX_GRAMS:
        return LIGHT_PARCEL_FEE
    if weight_grams <= STANDARD_PARCEL_MAX_GRAMS:
        return STANDARD_PARCEL_FEE
    extra_kilograms = _ceil_div(
        weight_grams - STANDARD_PARCEL_MAX_GRAMS, GRAMS_PER_KILOGRAM
    )
    return STANDARD_PARCEL_FEE + extra_kilograms * FEE_PER_EXTRA_KILOGRAM


def _region_surcharge(region: Region) -> int:
    return OUTLYING_ISLAND_SURCHARGE if region is Region.OUTLYING_ISLAND else 0


def _cold_chain_fee(parcel: Parcel) -> int:
    # 冷藏車是另外跟物流商計費的，所以免運也要收
    return COLD_CHAIN_FEE[parcel.region] if parcel.needs_cold_chain else 0


def _ceil_div(dividend: int, divisor: int) -> int:
    return -(-dividend // divisor)
