use clean_code::shipping::{InvalidParcel, Membership, Parcel, Region, shipping_fee};

/// 預設：本島、一般會員、訂單 800 元、2500 公克、常溫。
/// 每個測試只用 `..parcel()` 覆寫要測的欄位。
fn parcel() -> Parcel {
    Parcel {
        weight_grams: 2500,
        region: Region::MainIsland,
        membership: Membership::Normal,
        order_amount: 800,
        needs_cold_chain: false,
    }
}

#[test]
fn weight_tiers() {
    for (weight_grams, expected) in [
        (1, 60),
        (1000, 60),
        (1001, 100),
        (5000, 100),
        (5001, 120),
        (6000, 120),
        (6001, 140),
    ] {
        let fee = shipping_fee(&Parcel {
            weight_grams,
            ..parcel()
        });

        assert_eq!(fee, Ok(expected), "重量 {weight_grams} 公克");
    }
}

#[test]
fn outlying_island_adds_a_surcharge() {
    let fee = shipping_fee(&Parcel {
        region: Region::OutlyingIsland,
        ..parcel()
    });

    assert_eq!(fee, Ok(100 + 80));
}

#[test]
fn free_shipping_threshold_depends_on_membership() {
    for (membership, order_amount, expected) in [
        (Membership::Normal, 999, 100),
        (Membership::Normal, 1000, 0),
        (Membership::Gold, 499, 100),
        (Membership::Gold, 500, 0),
    ] {
        let fee = shipping_fee(&Parcel {
            membership,
            order_amount,
            ..parcel()
        });

        assert_eq!(fee, Ok(expected), "{membership:?} 訂單 {order_amount} 元");
    }
}

#[test]
fn outlying_island_never_ships_free() {
    let fee = shipping_fee(&Parcel {
        region: Region::OutlyingIsland,
        membership: Membership::Gold,
        order_amount: 5000,
        ..parcel()
    });

    assert_eq!(fee, Ok(180));
}

#[test]
fn cold_chain_fee_depends_on_region() {
    for (region, expected) in [
        (Region::MainIsland, 100 + 50),
        (Region::OutlyingIsland, 180 + 100),
    ] {
        let fee = shipping_fee(&Parcel {
            region,
            needs_cold_chain: true,
            ..parcel()
        });

        assert_eq!(fee, Ok(expected), "{region:?}");
    }
}

#[test]
fn cold_chain_fee_is_charged_even_when_shipping_is_free() {
    let fee = shipping_fee(&Parcel {
        order_amount: 1000,
        needs_cold_chain: true,
        ..parcel()
    });

    assert_eq!(fee, Ok(50));
}

#[test]
fn rejects_non_positive_weight() {
    for weight_grams in [0, -1] {
        let error = shipping_fee(&Parcel {
            weight_grams,
            ..parcel()
        })
        .unwrap_err();

        assert_eq!(error, InvalidParcel::NonPositiveWeight(weight_grams));
        assert_eq!(
            error.to_string(),
            format!("重量必須大於 0 公克，收到 {weight_grams}")
        );
    }
}

#[test]
fn rejects_negative_order_amount() {
    let error = shipping_fee(&Parcel {
        order_amount: -1,
        ..parcel()
    })
    .unwrap_err();

    assert_eq!(error, InvalidParcel::NegativeOrderAmount(-1));
    assert_eq!(error.to_string(), "訂單金額不能是負數，收到 -1");
}
