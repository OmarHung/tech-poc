from characterization import (
    INVALID,
    Case,
    Outcome,
    compare,
    input_grid,
    legacy_outcome,
    refactored_outcome,
)


def test_refactoring_preserves_behavior_on_every_case_in_the_grid():
    result = compare()

    assert result.total == 728
    assert result.mismatches == ()


def test_grid_covers_both_valid_and_invalid_inputs():
    result = compare()

    assert result.invalid == 200
    assert result.total - result.invalid == 528


def test_legacy_error_code_maps_to_refactored_error():
    case = (0, "main", "normal", 800, False)

    assert legacy_outcome(case) == INVALID
    assert refactored_outcome(case) == INVALID


def test_grid_has_no_duplicate_cases():
    cases = tuple(input_grid())

    assert len(cases) == len(set(cases))


def test_comparison_catches_a_one_dollar_change():
    def off_by_one_on_outlying_cold_chain(case: Case) -> Outcome:
        outcome = refactored_outcome(case)
        _, region, _, _, cold = case
        if outcome != INVALID and region == "outlying" and cold:
            return outcome + 1
        return outcome

    result = compare(off_by_one_on_outlying_cold_chain)

    # 離島冷藏的有效輸入：11 種重量 × 2 種會員 × 6 種有效金額
    assert len(result.mismatches) == 11 * 2 * 6
