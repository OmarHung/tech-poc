using CatalogService.Catalog;

namespace CatalogService.Tests;

public sealed class CatalogRulesTests
{
    [Fact]
    public void ParseId_ReturnsValue_ForPositiveInteger()
    {
        Assert.Equal(42, CatalogRules.ParseId("42", "product id"));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("+1")]
    [InlineData(" 1")]
    [InlineData("abc")]
    [InlineData("")]
    [InlineData("1.5")]
    public void ParseId_Rejects_AnythingButPositiveDigits(string raw)
    {
        var error = Assert.Throws<InvalidInputException>(() => CatalogRules.ParseId(raw, "product id"));

        Assert.Equal("product id must be a positive integer", error.Message);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(CatalogRules.MaxQuantity)]
    public void ValidateQuantity_AcceptsBounds(long quantity)
    {
        Assert.Equal(quantity, CatalogRules.ValidateQuantity(quantity));
    }
}
