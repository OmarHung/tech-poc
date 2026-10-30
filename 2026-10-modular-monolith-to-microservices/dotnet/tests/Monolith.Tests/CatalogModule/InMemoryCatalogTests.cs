using Monolith.Modules.Catalog;

namespace Monolith.Tests.CatalogModule;

public sealed class InMemoryCatalogTests
{
    private static InMemoryCatalog CatalogWithStock(long stock) => new([new Product(1, "Keyboard", 100, stock)]);

    [Fact]
    public async Task ListAsync_ReturnsProductsSortedById()
    {
        var products = await InMemoryCatalog.Seeded().ListAsync();

        Assert.Equal([1L, 2L, 3L], products.Select(p => p.Id));
    }

    [Fact]
    public async Task GetAsync_Throws_WhenProductIsUnknown()
    {
        var error = await Assert.ThrowsAsync<NotFoundException>(() => CatalogWithStock(5).GetAsync(99));

        Assert.Equal("product 99 not found", error.Message);
    }

    [Fact]
    public async Task ReserveAsync_ReturnsNewProduct_AndKeepsPreviousSnapshotUnchanged()
    {
        var catalog = CatalogWithStock(5);
        var before = await catalog.GetAsync(1);

        var updated = await catalog.ReserveAsync(1, 2);

        Assert.Equal(3, updated.Stock);
        Assert.Equal(5, before.Stock);
        Assert.Equal(3, (await catalog.GetAsync(1)).Stock);
    }

    [Fact]
    public async Task ReserveAsync_RejectsMoreThanStock_WithoutChangingIt()
    {
        var catalog = CatalogWithStock(1);

        await Assert.ThrowsAsync<InsufficientStockException>(() => catalog.ReserveAsync(1, 2));

        Assert.Equal(1, (await catalog.GetAsync(1)).Stock);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(CatalogRules.MaxQuantity + 1)]
    public async Task ReserveAsync_RejectsOutOfRangeQuantity(long quantity)
    {
        await Assert.ThrowsAsync<InvalidInputException>(() => CatalogWithStock(5).ReserveAsync(1, quantity));
    }

    [Fact]
    public async Task ReserveAsync_NeverOversells_UnderConcurrency()
    {
        var catalog = CatalogWithStock(10);

        var attempts = Enumerable.Range(0, 50).Select(_ => Task.Run(async () =>
        {
            try
            {
                await catalog.ReserveAsync(1, 1);
                return true;
            }
            catch (InsufficientStockException)
            {
                return false;
            }
        }));
        var results = await Task.WhenAll(attempts);

        Assert.Equal(10, results.Count(succeeded => succeeded));
        Assert.Equal(0, (await catalog.GetAsync(1)).Stock);
    }
}
