using static CleanCode.Shipping;

namespace CleanCode.Tests;

public sealed class ShippingTests
{
    // 每個測試用 with 只改自己關心的欄位，其他維持這組預設
    private static readonly Parcel Baseline = new(
        WeightGrams: 2500,
        Region: Region.MainIsland,
        Membership: Membership.Normal,
        OrderAmount: 800,
        NeedsColdChain: false);

    [Theory]
    [InlineData(1, 60)]
    [InlineData(1000, 60)]
    [InlineData(1001, 100)]
    [InlineData(5000, 100)]
    [InlineData(5001, 120)]
    [InlineData(6000, 120)]
    [InlineData(6001, 140)]
    public void ChargesByWeightTier(int weightGrams, int expected)
    {
        Assert.Equal(expected, ShippingFee(Baseline with { WeightGrams = weightGrams }));
    }

    [Fact]
    public void OutlyingIsland_AddsASurcharge()
    {
        Assert.Equal(100 + 80, ShippingFee(Baseline with { Region = Region.OutlyingIsland }));
    }

    [Theory]
    [InlineData(Membership.Normal, 999, 100)]
    [InlineData(Membership.Normal, 1000, 0)]
    [InlineData(Membership.Gold, 499, 100)]
    [InlineData(Membership.Gold, 500, 0)]
    public void FreeShippingThreshold_DependsOnMembership(Membership membership, int orderAmount, int expected)
    {
        var fee = ShippingFee(Baseline with { Membership = membership, OrderAmount = orderAmount });

        Assert.Equal(expected, fee);
    }

    [Fact]
    public void OutlyingIsland_NeverShipsFree()
    {
        var parcel = Baseline with { Region = Region.OutlyingIsland, Membership = Membership.Gold, OrderAmount = 5000 };

        Assert.Equal(180, ShippingFee(parcel));
    }

    [Theory]
    [InlineData(Region.MainIsland, 100 + 50)]
    [InlineData(Region.OutlyingIsland, 180 + 100)]
    public void ColdChainFee_DependsOnRegion(Region region, int expected)
    {
        Assert.Equal(expected, ShippingFee(Baseline with { Region = region, NeedsColdChain = true }));
    }

    [Fact]
    public void ColdChainFee_IsChargedEvenWhenShippingIsFree()
    {
        Assert.Equal(50, ShippingFee(Baseline with { OrderAmount = 1000, NeedsColdChain = true }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Rejects_NonPositiveWeight(int weightGrams)
    {
        var error = Assert.Throws<InvalidParcelException>(
            () => ShippingFee(Baseline with { WeightGrams = weightGrams }));

        Assert.Equal($"重量必須大於 0 公克，收到 {weightGrams}", error.Message);
    }

    [Fact]
    public void Rejects_NegativeOrderAmount()
    {
        var error = Assert.Throws<InvalidParcelException>(
            () => ShippingFee(Baseline with { OrderAmount = -1 }));

        Assert.Equal("訂單金額不能是負數，收到 -1", error.Message);
    }
}
