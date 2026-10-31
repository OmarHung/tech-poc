use solid_principles::isp::{
    CashOnDelivery, Chargeable, CreditCard, LegacyCashOnDelivery, LegacyCreditCard,
    LegacyPaymentMethod, NotSupported, Refundable, Rememberable, refund_order,
};

#[test]
fn legacy_credit_card_supports_every_method() {
    let card = LegacyCreditCard;

    assert_eq!(card.charge(300), "信用卡扣款 300 元");
    assert_eq!(card.refund(300), Ok("信用卡退款 300 元".to_owned()));
    assert_eq!(card.remember(), Ok("已記住這張信用卡".to_owned()));
}

#[test]
fn legacy_cash_on_delivery_charges() {
    assert_eq!(LegacyCashOnDelivery.charge(300), "送達時收現 300 元");
}

#[test]
fn legacy_cash_on_delivery_is_forced_to_stub_two_methods() {
    let cash = LegacyCashOnDelivery;

    assert_eq!(cash.refund(300), Err(NotSupported("貨到付款不能線上退款")));
    assert_eq!(
        cash.remember(),
        Err(NotSupported("貨到付款沒有付款資訊可以記住"))
    );
}

#[test]
fn cash_on_delivery_only_implements_charging() {
    // 能轉成 &dyn Chargeable 就代表有實作，等於 Python 的 isinstance 檢查。
    // 「沒有實作 Refundable、Rememberable」執行期查不了，由 src/isp.rs 的 compile_fail doctest 保證。
    let cash: &dyn Chargeable = &CashOnDelivery;

    assert_eq!(cash.charge(300), "送達時收現 300 元");
}

#[test]
fn credit_card_implements_every_role() {
    let chargeable: &dyn Chargeable = &CreditCard;
    let refundable: &dyn Refundable = &CreditCard;
    let rememberable: &dyn Rememberable = &CreditCard;

    assert_eq!(chargeable.charge(300), "信用卡扣款 300 元");
    assert_eq!(refundable.refund(300), "信用卡退款 300 元");
    assert_eq!(rememberable.remember(), "已記住這張信用卡");
}

#[test]
fn refund_order_refunds_a_refundable_method() {
    assert_eq!(refund_order(&CreditCard, 300), "信用卡退款 300 元");
}
