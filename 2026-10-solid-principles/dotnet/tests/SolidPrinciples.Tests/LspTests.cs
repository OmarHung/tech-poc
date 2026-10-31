using SolidPrinciples.Lsp;
using SolidPrinciples.Ocp;

namespace SolidPrinciples.Tests;

public sealed class LspTests
{
    public static TheoryData<IDiscountRule> ShippedRules =>
        [new NoDiscount(), new PercentOff(10), new PercentOff(20), new FixedAmountOff(200)];

    [Fact]
    public void UncappedAmountOff_MakesTheTotalNegative()
    {
        Assert.Equal(-50, Pricing.TotalAfterDiscount(150, new UncappedAmountOff(200)));
    }

    [Fact]
    public void ContractCheck_CatchesTheUncappedRule()
    {
        // 期望值寫成 int[]：寫成 [0, 150] 會被推成 ImmutableArray，它的相等比的是內部陣列參考，不是內容
        Assert.Equal(new[] { 0, 150 }, DiscountContract.Violations(new UncappedAmountOff(200)));
    }

    [Fact]
    public void FixedAmountOff_StopsAtZero()
    {
        Assert.Equal(0, Pricing.TotalAfterDiscount(150, new FixedAmountOff(200)));
        Assert.Equal(800, Pricing.TotalAfterDiscount(1000, new FixedAmountOff(200)));
    }

    [Theory]
    [MemberData(nameof(ShippedRules))]
    public void EveryShippedRule_HonorsTheContract(IDiscountRule rule)
    {
        Assert.Empty(DiscountContract.Violations(rule));
    }
}
