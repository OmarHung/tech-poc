from srp import InvoiceCalculator, LegacyOrder, LineItem, ShippingPolicy, subtotal

UNDER_THRESHOLD = (LineItem("滑鼠", 480, 2),)  # 未稅 960，含稅 1008
AT_THRESHOLD = (LineItem("鍵盤", 1000, 1),)


def test_subtotal_multiplies_unit_price_by_quantity():
    assert subtotal((LineItem("滑鼠", 480, 2), LineItem("墊子", 40, 1))) == 1000


def test_legacy_order_tax_change_leaks_into_free_shipping():
    order = LegacyOrder(UNDER_THRESHOLD)

    assert order.invoice_amount() == 1008
    assert order.ships_free()  # 物流規則沒變，未稅 960 卻變成免運


def test_invoice_calculator_adds_tax():
    assert InvoiceCalculator().amount(UNDER_THRESHOLD) == 1008


def test_shipping_policy_is_unaffected_by_tax():
    assert not ShippingPolicy().ships_free(UNDER_THRESHOLD)


def test_shipping_policy_ships_free_from_threshold():
    assert ShippingPolicy().ships_free(AT_THRESHOLD)
