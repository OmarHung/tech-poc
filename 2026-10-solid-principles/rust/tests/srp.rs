use solid_principles::srp::{InvoiceCalculator, LegacyOrder, LineItem, ShippingPolicy, subtotal};

/// 未稅 960，含稅 1008
fn under_threshold() -> Vec<LineItem> {
    vec![LineItem::new("滑鼠", 480, 2)]
}

fn at_threshold() -> Vec<LineItem> {
    vec![LineItem::new("鍵盤", 1000, 1)]
}

#[test]
fn subtotal_multiplies_unit_price_by_quantity() {
    let items = [LineItem::new("滑鼠", 480, 2), LineItem::new("墊子", 40, 1)];

    assert_eq!(subtotal(&items), 1000);
}

#[test]
fn legacy_order_tax_change_leaks_into_free_shipping() {
    let order = LegacyOrder::new(under_threshold());

    assert_eq!(order.invoice_amount(), 1008);
    assert!(order.ships_free()); // 物流規則沒變，未稅 960 卻變成免運
}

#[test]
fn invoice_calculator_adds_tax() {
    assert_eq!(InvoiceCalculator.amount(&under_threshold()), 1008);
}

#[test]
fn shipping_policy_is_unaffected_by_tax() {
    assert!(!ShippingPolicy.ships_free(&under_threshold()));
}

#[test]
fn shipping_policy_ships_free_from_threshold() {
    assert!(ShippingPolicy.ships_free(&at_threshold()));
}
