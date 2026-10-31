use solid_principles::lsp::{
    FixedAmountOff, SAMPLE_SUBTOTALS, UncappedAmountOff, contract_violations,
};
use solid_principles::ocp::{DiscountRule, NoDiscount, PercentOff, total_after_discount};

#[test]
fn uncapped_amount_off_makes_the_total_negative() {
    assert_eq!(total_after_discount(150, &UncappedAmountOff(200)), -50);
}

#[test]
fn contract_check_catches_the_uncapped_rule() {
    assert_eq!(
        contract_violations(&UncappedAmountOff(200), &SAMPLE_SUBTOTALS),
        vec![0, 150]
    );
}

#[test]
fn fixed_amount_off_stops_at_zero() {
    assert_eq!(total_after_discount(150, &FixedAmountOff(200)), 0);
    assert_eq!(total_after_discount(1000, &FixedAmountOff(200)), 800);
}

#[test]
fn every_shipped_rule_honors_the_contract() {
    let rules: [&dyn DiscountRule; 4] = [
        &NoDiscount,
        &PercentOff(10),
        &PercentOff(20),
        &FixedAmountOff(200),
    ];

    for (index, rule) in rules.into_iter().enumerate() {
        assert_eq!(
            contract_violations(rule, &SAMPLE_SUBTOTALS),
            Vec::<i64>::new(),
            "rules[{index}]"
        );
    }
}
