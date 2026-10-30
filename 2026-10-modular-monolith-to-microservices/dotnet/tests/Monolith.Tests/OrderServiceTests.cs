using Monolith.Modules.Catalog;
using Monolith.Modules.Orders;

namespace Monolith.Tests;

public sealed class OrderServiceTests
{
    private static OrderService Service() => new(new InMemoryCatalog([new Product(1, "Keyboard", 300, 5)]));

    [Fact]
    public async Task PlaceAsync_SnapshotsProduct_AndComputesTotal()
    {
        var order = await Service().PlaceAsync(1, 2);

        Assert.Equal(("Keyboard", 300L, 600L), (order.ProductName, order.UnitPrice, order.Total));
    }

    [Fact]
    public async Task PlacedOrders_CanBeFound_AndHaveSequentialIds()
    {
        var service = Service();

        var first = await service.PlaceAsync(1, 1);
        var second = await service.PlaceAsync(1, 1);

        Assert.Equal(first, service.Find(first.Id));
        Assert.Equal(first.Id + 1, second.Id);
    }

    [Fact]
    public async Task PlaceAsync_PassesCatalogErrorsThrough()
    {
        await Assert.ThrowsAsync<InsufficientStockException>(() => Service().PlaceAsync(1, 6));
        await Assert.ThrowsAsync<NotFoundException>(() => Service().PlaceAsync(9, 1));
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 0)]
    public async Task PlaceAsync_RejectsInvalidInput(long productId, long quantity)
    {
        await Assert.ThrowsAsync<InvalidInputException>(() => Service().PlaceAsync(productId, quantity));
    }

    [Fact]
    public void Find_Throws_WhenOrderIsUnknown()
    {
        var error = Assert.Throws<NotFoundException>(() => Service().Find(42));

        Assert.Equal("order 42 not found", error.Message);
    }

    [Theory]
    [InlineData("inprocess", CatalogMode.InProcess)]
    [InlineData("remote", CatalogMode.Remote)]
    public void ParseMode_AcceptsKnownModes(string value, CatalogMode expected)
    {
        Assert.Equal(expected, MonolithConfig.ParseMode(value));
    }

    [Fact]
    public void ParseMode_RejectsUnknownMode()
    {
        Assert.Throws<InvalidOperationException>(() => MonolithConfig.ParseMode("sideways"));
    }
}
