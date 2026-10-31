using System.Collections.Immutable;
using SolidPrinciples.Srp;

namespace SolidPrinciples.Tests;

public sealed class SrpTests
{
    private static readonly ImmutableArray<LineItem> UnderThreshold = [new("滑鼠", 480, 2)]; // 未稅 960，含稅 1008
    private static readonly ImmutableArray<LineItem> AtThreshold = [new("鍵盤", 1000, 1)];

    [Fact]
    public void Subtotal_MultipliesUnitPriceByQuantity()
    {
        LineItem[] items = [new("滑鼠", 480, 2), new("墊子", 40, 1)];

        Assert.Equal(1000, items.Subtotal());
    }

    [Fact]
    public void LegacyOrder_TaxChange_LeaksIntoFreeShipping()
    {
        var order = new LegacyOrder(UnderThreshold);

        Assert.Equal(1008, order.InvoiceAmount());
        Assert.True(order.ShipsFree()); // 物流規則沒變，未稅 960 卻變成免運
    }

    [Fact]
    public void InvoiceCalculator_AddsTax()
    {
        Assert.Equal(1008, new InvoiceCalculator().Amount(UnderThreshold));
    }

    [Fact]
    public void ShippingPolicy_IsUnaffectedByTax()
    {
        Assert.False(new ShippingPolicy().ShipsFree(UnderThreshold));
    }

    [Fact]
    public void ShippingPolicy_ShipsFree_FromThreshold()
    {
        Assert.True(new ShippingPolicy().ShipsFree(AtThreshold));
    }
}
