using System.Collections.Immutable;

namespace CleanCode;

/// <summary>
/// Characterization test（又叫 golden master）：不管規格寫什麼，只比對舊版和重構版
/// 在同一批輸入下的結果是不是一模一樣。
/// <para>舊版用 -1 表示輸入無效，重構版改成丟 InvalidParcelException。比對時把這兩種視為同一個結果。</para>
/// </summary>
public static class Characterization
{
    // 每個邊界都取「剛好、少 1、多 1」
    private static readonly ImmutableArray<int> Weights =
        [-1, 0, 1, 999, 1000, 1001, 4999, 5000, 5001, 5999, 6000, 6001, 12345];
    private static readonly ImmutableArray<string> Regions = ["main", "outlying"];
    private static readonly ImmutableArray<string> Memberships = ["normal", "gold"];
    private static readonly ImmutableArray<int> OrderAmounts = [-1, 0, 499, 500, 999, 1000, 5000];
    private static readonly ImmutableArray<bool> ColdChain = [false, true];

    private const int LegacyErrorCode = -1;

    // 順序和 Python 的 itertools.product 一樣：最後一個維度變化最快
    public static IEnumerable<Case> InputGrid() =>
        from weight in Weights
        from region in Regions
        from membership in Memberships
        from amount in OrderAmounts
        from cold in ColdChain
        select new Case(weight, region, membership, amount, cold);

    public static Outcome LegacyOutcome(Case input)
    {
        var fee = Legacy.Calc(input.Weight, input.Region, input.Membership, input.Amount, input.Cold);
        return fee == LegacyErrorCode ? Outcome.Invalid : new Outcome(fee);
    }

    public static Outcome RefactoredOutcome(Case input)
    {
        var parcel = new Parcel(
            input.Weight,
            LegacyCodes.ToRegion(input.Region),
            LegacyCodes.ToMembership(input.Membership),
            input.Amount,
            input.Cold);
        try
        {
            return new Outcome(Shipping.ShippingFee(parcel));
        }
        catch (InvalidParcelException)
        {
            return Outcome.Invalid;
        }
    }

    public static Comparison Compare() => Compare(RefactoredOutcome);

    /// <summary>拿 candidate 的結果去比對舊版。不帶參數時比對重構版；測試會傳入故意改壞的版本。</summary>
    public static Comparison Compare(Func<Case, Outcome> candidate)
    {
        var results = InputGrid()
            .Select(input => (Case: input, Old: LegacyOutcome(input), New: candidate(input)))
            .ToImmutableArray();
        return new Comparison(
            Total: results.Length,
            Invalid: results.Count(result => result.Old.IsInvalid && result.New.IsInvalid),
            Mismatches: [.. results.Where(result => result.Old != result.New).Select(result => result.Case)]);
    }
}
