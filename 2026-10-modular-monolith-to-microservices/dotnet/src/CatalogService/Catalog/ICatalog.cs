namespace CatalogService.Catalog;

/// <summary>
/// Everything the rest of the system may ask of the catalog.
/// Orders depend on this interface only, which is the seam that lets the catalog
/// move out of process: swap the in-memory implementation for an HTTP client and
/// nothing else changes.
/// </summary>
public interface ICatalog
{
    Task<IReadOnlyList<Product>> ListAsync(CancellationToken cancellationToken = default);

    Task<Product> GetAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>Atomically takes <paramref name="quantity"/> units out of stock and returns the updated product.</summary>
    Task<Product> ReserveAsync(long id, long quantity, CancellationToken cancellationToken = default);
}
