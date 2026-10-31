use std::collections::HashSet;

use clean_code::characterization::{
    Case, Outcome, compare, compare_with, input_grid, legacy_outcome, refactored_outcome,
};

#[test]
fn refactoring_preserves_behavior_on_every_case_in_the_grid() {
    let result = compare();

    assert_eq!(result.total, 728);
    assert_eq!(result.mismatches, vec![]);
}

#[test]
fn grid_covers_both_valid_and_invalid_inputs() {
    let result = compare();

    assert_eq!(result.invalid, 200);
    assert_eq!(result.total - result.invalid, 528);
}

#[test]
fn legacy_error_code_maps_to_refactored_error() {
    let case: Case = (0, "main", "normal", 800, false);

    assert_eq!(legacy_outcome(case), Outcome::Invalid);
    assert_eq!(refactored_outcome(case), Outcome::Invalid);
}

#[test]
fn grid_has_no_duplicate_cases() {
    let cases: Vec<Case> = input_grid().collect();
    let unique: HashSet<Case> = cases.iter().copied().collect();

    assert_eq!(cases.len(), unique.len());
}

#[test]
fn comparison_catches_a_one_dollar_change() {
    fn off_by_one_on_outlying_cold_chain(case: Case) -> Outcome {
        let (_, region, _, _, cold) = case;
        match refactored_outcome(case) {
            Outcome::Fee(fee) if region == "outlying" && cold => Outcome::Fee(fee + 1),
            outcome => outcome,
        }
    }

    let result = compare_with(off_by_one_on_outlying_cold_chain);

    // 離島冷藏的有效輸入：11 種重量 × 2 種會員 × 6 種有效金額
    assert_eq!(result.mismatches.len(), 11 * 2 * 6);
}
