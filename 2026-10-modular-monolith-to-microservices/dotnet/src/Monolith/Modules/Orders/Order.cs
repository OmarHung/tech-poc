namespace Monolith.Modules.Orders;

/// <summary>
/// Name and price are copied at order time, so later catalog changes (or the
/// catalog being unreachable) never alter an existing order.
/// </summary>
public sealed record Order(long Id, long ProductId, string ProductName, long UnitPrice, long Quantity, long Total);

public sealed record CreateOrderRequest(long ProductId, long Quantity);
