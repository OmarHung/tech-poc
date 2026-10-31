from dataclasses import dataclass

import pytest

from ocp import (
    NoDiscount,
    PercentOff,
    legacy_total_after_discount,
    total_after_discount,
)


@dataclass(frozen=True)
class SpendThresholdOff:
    """定義在測試檔裡的新折扣：證明不用改 ocp.py 就能擴充。"""

    threshold: int
    amount: int

    def discount(self, subtotal: int) -> int:
        return self.amount if subtotal >= self.threshold else 0


@pytest.mark.parametrize(
    ("kind", "expected"), [("none", 1200), ("vip", 1080), ("staff", 960)]
)
def test_legacy_supports_the_kinds_it_was_written_for(kind, expected):
    assert legacy_total_after_discount(1200, kind) == expected


def test_legacy_rejects_a_new_kind_until_someone_edits_it():
    with pytest.raises(ValueError, match="不支援的折扣：threshold"):
        legacy_total_after_discount(1200, "threshold")


@pytest.mark.parametrize("subtotal", [0, 1, 999, 1200, 12345])
@pytest.mark.parametrize(
    ("kind", "rule"),
    [("none", NoDiscount()), ("vip", PercentOff(10)), ("staff", PercentOff(20))],
)
def test_rules_match_legacy_for_existing_kinds(subtotal, kind, rule):
    assert total_after_discount(subtotal, rule) == legacy_total_after_discount(
        subtotal, kind
    )


def test_new_rule_works_without_editing_the_module():
    rule = SpendThresholdOff(1000, 100)

    assert total_after_discount(1200, rule) == 1100
    assert total_after_discount(999, rule) == 999
