using System.Collections.Concurrent;
using Monolith.Modules.Catalog;

namespace Monolith.Modules.Orders;

/// <summary>Orders module. Stays in the monolith; depends on the catalog only through <see cref="ICatalog"/>.</summary>
public sealed class OrderService(ICatalog catalog)
{
    private readonly ConcurrentDictionary<long, Order> _orders = new();
    private long _lastId;

    public async Task<Order> PlaceAsync(long productId, long quantity, CancellationToken cancellationToken = default)
    {
        CatalogRules.ValidateId(productId, "productId");
        CatalogRules.ValidateQuantity(quantity);
        var product = await catalog.ReserveAsync(productId, quantity, cancellationToken);
        var order = new Order(
            Interlocked.Increment(ref _lastId),
            productId,
            product.Name,
            product.Price,
            quantity,
            product.Price * quantity);
        _orders[order.Id] = order;
        return order;
    }

    public Order Find(long id) =>
        _orders.TryGetValue(id, out var order) ? order : throw new NotFoundException($"order {id} not found");
}
