using System.Collections.Immutable;

namespace CatalogService.Catalog;

/// <summary>
/// Products live in an immutable dictionary that is swapped as a whole on every
/// reservation, so reads never lock and never see a half-applied change.
/// </summary>
public sealed class InMemoryCatalog(IEnumerable<Product> products) : ICatalog
{
    private readonly Lock _writeGate = new();
    private ImmutableSortedDictionary<long, Product> _products = products.ToImmutableSortedDictionary(p => p.Id, p => p);

    public static InMemoryCatalog Seeded() => new(CatalogSeed.Products);

    public Task<IReadOnlyList<Product>> ListAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<Product>>([.. Volatile.Read(ref _products).Values]);

    public Task<Product> GetAsync(long id, CancellationToken cancellationToken = default) =>
        Volatile.Read(ref _products).TryGetValue(id, out var product)
            ? Task.FromResult(product)
            : throw CatalogErrors.ProductNotFound(id);

    public Task<Product> ReserveAsync(long id, long quantity, CancellationToken cancellationToken = default)
    {
        CatalogRules.ValidateQuantity(quantity);
        lock (_writeGate)
        {
            if (!_products.TryGetValue(id, out var current))
            {
                throw CatalogErrors.ProductNotFound(id);
            }
            if (current.Stock < quantity)
            {
                throw CatalogErrors.InsufficientStock(id, quantity, current.Stock);
            }
            var updated = current with { Stock = current.Stock - quantity };
            Volatile.Write(ref _products, _products.SetItem(id, updated));
            return Task.FromResult(updated);
        }
    }
}
