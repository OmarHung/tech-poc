namespace CleanCode.Tests;

public sealed class CharacterizationTests
{
    [Fact]
    public void Refactoring_PreservesBehavior_OnEveryCaseInTheGrid()
    {
        var result = Characterization.Compare();

        Assert.Equal(728, result.Total);
        Assert.Empty(result.Mismatches);
    }

    [Fact]
    public void Grid_CoversBothValidAndInvalidInputs()
    {
        var result = Characterization.Compare();

        Assert.Equal(200, result.Invalid);
        Assert.Equal(528, result.Total - result.Invalid);
    }

    [Fact]
    public void LegacyErrorCode_MapsToRefactoredError()
    {
        var input = new Case(0, "main", "normal", 800, false);

        Assert.Equal(Outcome.Invalid, Characterization.LegacyOutcome(input));
        Assert.Equal(Outcome.Invalid, Characterization.RefactoredOutcome(input));
    }

    [Fact]
    public void Grid_HasNoDuplicateCases()
    {
        Assert.Distinct(Characterization.InputGrid());
    }

    [Fact]
    public void Comparison_CatchesAOneDollarChange()
    {
        static Outcome OffByOneOnOutlyingColdChain(Case input)
        {
            var outcome = Characterization.RefactoredOutcome(input);
            if (!outcome.IsInvalid && input.Region == "outlying" && input.Cold)
            {
                return new Outcome(outcome.Fee + 1);
            }
            return outcome;
        }

        var result = Characterization.Compare(OffByOneOnOutlyingColdChain);

        // 離島冷藏的有效輸入：11 種重量 × 2 種會員 × 6 種有效金額
        Assert.Equal(11 * 2 * 6, result.Mismatches.Length);
    }
}
