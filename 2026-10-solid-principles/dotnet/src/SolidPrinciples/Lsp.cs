// L：里氏替換原則（Liskov Substitution Principle）。
//
// 子型別要能替換父型別，呼叫端不用知道拿到的是哪一種。
// IDiscountRule 的契約是 0 ≤ Discount(subtotal) ≤ subtotal，
// Pricing.TotalAfterDiscount() 靠這個契約保證總額不會是負數。
//
// 情境：新增「折 200 元」。小計只有 150 元的訂單套用後，總額是多少？

using System.Collections.Immutable;
using SolidPrinciples.Ocp;

namespace SolidPrinciples.Lsp;

// --- 違反：不管小計多少都折 Amount，小計不夠折時總額變負數 ---

public sealed record UncappedAmountOff(int Amount) : IDiscountRule
{
    public int Discount(int subtotal) => Amount;
}

// --- 遵守：最多折到 0 元 ---

public sealed record FixedAmountOff(int Amount) : IDiscountRule
{
    public int Discount(int subtotal) => Math.Min(Amount, subtotal);
}

public static class DiscountContract
{
    public static ImmutableArray<int> SampleSubtotals { get; } = [0, 150, 999, 1000, 5000];

    /// <summary>回傳讓 rule 違反契約的小計。空的代表這組樣本都守住了契約。</summary>
    public static ImmutableArray<int> Violations(IDiscountRule rule) => Violations(rule, SampleSubtotals);

    /// <inheritdoc cref="Violations(IDiscountRule)"/>
    public static ImmutableArray<int> Violations(IDiscountRule rule, IEnumerable<int> subtotals) =>
        [.. subtotals.Where(subtotal => !Honors(rule, subtotal))];

    private static bool Honors(IDiscountRule rule, int subtotal)
    {
        var discount = rule.Discount(subtotal);
        return 0 <= discount && discount <= subtotal;
    }
}
