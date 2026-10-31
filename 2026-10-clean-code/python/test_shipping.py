import pytest

from shipping import InvalidParcelError, Membership, Parcel, Region, shipping_fee

MAIN = Region.MAIN_ISLAND
OUTLYING = Region.OUTLYING_ISLAND
NORMAL = Membership.NORMAL
GOLD = Membership.GOLD


def parcel(
    weight_grams: int = 2500,
    region: Region = MAIN,
    membership: Membership = NORMAL,
    order_amount: int = 800,
    needs_cold_chain: bool = False,
) -> Parcel:
    return Parcel(weight_grams, region, membership, order_amount, needs_cold_chain)


@pytest.mark.parametrize(
    ("weight_grams", "expected"),
    [
        (1, 60),
        (1000, 60),
        (1001, 100),
        (5000, 100),
        (5001, 120),
        (6000, 120),
        (6001, 140),
    ],
)
def test_weight_tiers(weight_grams, expected):
    assert shipping_fee(parcel(weight_grams=weight_grams)) == expected


def test_outlying_island_adds_a_surcharge():
    assert shipping_fee(parcel(region=OUTLYING)) == 100 + 80


@pytest.mark.parametrize(
    ("membership", "order_amount", "expected"),
    [(NORMAL, 999, 100), (NORMAL, 1000, 0), (GOLD, 499, 100), (GOLD, 500, 0)],
)
def test_free_shipping_threshold_depends_on_membership(
    membership, order_amount, expected
):
    fee = shipping_fee(parcel(membership=membership, order_amount=order_amount))

    assert fee == expected


def test_outlying_island_never_ships_free():
    assert (
        shipping_fee(parcel(region=OUTLYING, membership=GOLD, order_amount=5000)) == 180
    )


@pytest.mark.parametrize(
    ("region", "expected"), [(MAIN, 100 + 50), (OUTLYING, 180 + 100)]
)
def test_cold_chain_fee_depends_on_region(region, expected):
    assert shipping_fee(parcel(region=region, needs_cold_chain=True)) == expected


def test_cold_chain_fee_is_charged_even_when_shipping_is_free():
    assert shipping_fee(parcel(order_amount=1000, needs_cold_chain=True)) == 50


@pytest.mark.parametrize("weight_grams", [0, -1])
def test_rejects_non_positive_weight(weight_grams):
    with pytest.raises(InvalidParcelError, match="重量必須大於 0 公克"):
        shipping_fee(parcel(weight_grams=weight_grams))


def test_rejects_negative_order_amount():
    with pytest.raises(InvalidParcelError, match="訂單金額不能是負數"):
        shipping_fee(parcel(order_amount=-1))
