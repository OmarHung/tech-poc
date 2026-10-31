import pytest

from lsp import FixedAmountOff, UncappedAmountOff, contract_violations
from ocp import NoDiscount, PercentOff, total_after_discount


def test_uncapped_amount_off_makes_the_total_negative():
    assert total_after_discount(150, UncappedAmountOff(200)) == -50


def test_contract_check_catches_the_uncapped_rule():
    assert contract_violations(UncappedAmountOff(200)) == (0, 150)


def test_fixed_amount_off_stops_at_zero():
    assert total_after_discount(150, FixedAmountOff(200)) == 0
    assert total_after_discount(1000, FixedAmountOff(200)) == 800


@pytest.mark.parametrize(
    "rule", [NoDiscount(), PercentOff(10), PercentOff(20), FixedAmountOff(200)]
)
def test_every_shipped_rule_honors_the_contract(rule):
    assert contract_violations(rule) == ()
