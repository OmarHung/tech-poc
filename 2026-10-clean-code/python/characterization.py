"""Characterization test（又叫 golden master）：不管規格寫什麼，只比對舊版和重構版
在同一批輸入下的結果是不是一模一樣。

舊版用 -1 表示輸入無效，重構版改成丟 InvalidParcelError。比對時把這兩種視為同一個結果。
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import product

from legacy import calc
from shipping import InvalidParcelError, Membership, Parcel, Region, shipping_fee

# 每個邊界都取「剛好、少 1、多 1」
WEIGHTS = (-1, 0, 1, 999, 1000, 1001, 4999, 5000, 5001, 5999, 6000, 6001, 12345)
REGIONS = ("main", "outlying")
MEMBERSHIPS = ("normal", "gold")
ORDER_AMOUNTS = (-1, 0, 499, 500, 999, 1000, 5000)
COLD_CHAIN = (False, True)

LEGACY_ERROR_CODE = -1
INVALID = "invalid"

Case = tuple[int, str, str, int, bool]
Outcome = int | str


@dataclass(frozen=True)
class Comparison:
    total: int
    invalid: int  # 兩版都判定為無效輸入的組數
    mismatches: tuple[Case, ...]


def input_grid() -> Iterator[Case]:
    return product(WEIGHTS, REGIONS, MEMBERSHIPS, ORDER_AMOUNTS, COLD_CHAIN)


def legacy_outcome(case: Case) -> Outcome:
    fee = calc(*case)
    return INVALID if fee == LEGACY_ERROR_CODE else fee


def refactored_outcome(case: Case) -> Outcome:
    weight, region, membership, amount, cold = case
    parcel = Parcel(weight, Region(region), Membership(membership), amount, cold)
    try:
        return shipping_fee(parcel)
    except InvalidParcelError:
        return INVALID


def compare(candidate: Callable[[Case], Outcome] = refactored_outcome) -> Comparison:
    """拿 candidate 的結果去比對舊版。預設比對重構版；測試會傳入故意改壞的版本。"""
    cases = tuple(input_grid())
    outcomes = tuple((legacy_outcome(case), candidate(case)) for case in cases)
    return Comparison(
        total=len(cases),
        invalid=sum(1 for old, new in outcomes if old == new == INVALID),
        mismatches=tuple(
            case for case, (old, new) in zip(cases, outcomes, strict=True) if old != new
        ),
    )
