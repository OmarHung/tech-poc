use solid_principles::ocp::{
    DiscountRule, NoDiscount, PercentOff, UnsupportedDiscount, legacy_total_after_discount,
    total_after_discount,
};

/// 定義在測試檔裡的新折扣：證明不用改 ocp 模組就能擴充。
struct SpendThresholdOff {
    threshold: i64,
    amount: i64,
}

impl DiscountRule for SpendThresholdOff {
    fn discount(&self, subtotal: i64) -> i64 {
        if subtotal >= self.threshold {
            self.amount
        } else {
            0
        }
    }
}

#[test]
fn legacy_supports_the_kinds_it_was_written_for() {
    for (kind, expected) in [("none", 1200), ("vip", 1080), ("staff", 960)] {
        assert_eq!(
            legacy_total_after_discount(1200, kind),
            Ok(expected),
            "kind={kind}"
        );
    }
}

#[test]
fn legacy_rejects_a_new_kind_until_someone_edits_it() {
    let error = legacy_total_after_discount(1200, "threshold").unwrap_err();

    assert_eq!(
        error,
        UnsupportedDiscount {
            kind: "threshold".to_owned()
        }
    );
    assert_eq!(error.to_string(), "不支援的折扣：threshold");
}

#[test]
fn rules_match_legacy_for_existing_kinds() {
    let rules: [(&str, &dyn DiscountRule); 3] = [
        ("none", &NoDiscount),
        ("vip", &PercentOff(10)),
        ("staff", &PercentOff(20)),
    ];

    for subtotal in [0, 1, 999, 1200, 12345] {
        for (kind, rule) in rules {
            assert_eq!(
                Ok(total_after_discount(subtotal, rule)),
                legacy_total_after_discount(subtotal, kind),
                "kind={kind}, subtotal={subtotal}"
            );
        }
    }
}

#[test]
fn new_rule_works_without_editing_the_module() {
    let rule = SpendThresholdOff {
        threshold: 1000,
        amount: 100,
    };

    assert_eq!(total_after_discount(1200, &rule), 1100);
    assert_eq!(total_after_discount(999, &rule), 999);
}
