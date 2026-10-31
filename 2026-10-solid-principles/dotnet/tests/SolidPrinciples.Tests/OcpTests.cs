using SolidPrinciples.Ocp;

namespace SolidPrinciples.Tests;

public sealed class OcpTests
{
    private static readonly int[] Subtotals = [0, 1, 999, 1200, 12345];

    /// <summary>每個小計 × 每種舊折扣和對應的新規則（5 × 3 = 15 組）。</summary>
    public static TheoryData<int, string, IDiscountRule> SubtotalsWithEquivalentRules()
    {
        var data = new TheoryData<int, string, IDiscountRule>();
        foreach (var subtotal in Subtotals)
        {
            data.Add(subtotal, "none", new NoDiscount());
            data.Add(subtotal, "vip", new PercentOff(10));
            data.Add(subtotal, "staff", new PercentOff(20));
        }
        return data;
    }

    [Theory]
    [InlineData("none", 1200)]
    [InlineData("vip", 1080)]
    [InlineData("staff", 960)]
    public void Legacy_SupportsTheKindsItWasWrittenFor(string kind, int expected)
    {
        Assert.Equal(expected, LegacyPricing.TotalAfterDiscount(1200, kind));
    }

    [Fact]
    public void Legacy_RejectsANewKind_UntilSomeoneEditsIt()
    {
        var error = Assert.Throws<ArgumentException>(() => LegacyPricing.TotalAfterDiscount(1200, "threshold"));

        Assert.Equal("不支援的折扣：threshold", error.Message);
    }

    [Theory]
    [MemberData(nameof(SubtotalsWithEquivalentRules))]
    public void Rules_MatchLegacy_ForExistingKinds(int subtotal, string kind, IDiscountRule rule)
    {
        Assert.Equal(LegacyPricing.TotalAfterDiscount(subtotal, kind), Pricing.TotalAfterDiscount(subtotal, rule));
    }

    [Fact]
    public void NewRule_WorksWithoutEditingTheModule()
    {
        var rule = new SpendThresholdOff(1000, 100);

        Assert.Equal(1100, Pricing.TotalAfterDiscount(1200, rule));
        Assert.Equal(999, Pricing.TotalAfterDiscount(999, rule));
    }
}

/// <summary>定義在測試檔裡的新折扣：證明不用改 Ocp.cs 就能擴充。</summary>
file sealed record SpendThresholdOff(int Threshold, int Amount) : IDiscountRule
{
    public int Discount(int subtotal) => subtotal >= Threshold ? Amount : 0;
}
