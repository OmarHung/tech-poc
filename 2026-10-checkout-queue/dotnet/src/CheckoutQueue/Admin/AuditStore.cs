using CheckoutQueue.Redis;

namespace CheckoutQueue.Admin;

/// <summary>
/// The numbers the PoC is judged on, recounted from the orders themselves rather than from
/// running counters, so a bug in the counting cannot hide a bug in the selling.
/// </summary>
public sealed record AuditReport(
    int InitialStock,
    int Stock,
    int PendingOrders,
    int PendingQuantity,
    int PaidOrders,
    int PaidQuantity,
    int ExpiredOrders,
    int ExpiredQuantity,
    int Charges,
    int DuplicateCharges,
    int UsersWithMultipleLiveOrders,
    int Tickets,
    int Waiting,
    int Active)
{
    /// <summary>Units promised to buyers beyond what existed. Must be 0.</summary>
    public int Oversold => Math.Max(0, PendingQuantity + PaidQuantity - InitialStock);

    /// <summary>Every unit is on the shelf, held by an unpaid order, or sold. Nothing lost, nothing made up.</summary>
    public bool StockBalanced => Stock >= 0 && Stock + PendingQuantity + PaidQuantity == InitialStock;
}

public sealed class AuditStore(SaleRedis redis)
{
    public async Task<AuditReport> GetAsync(CancellationToken ct = default)
    {
        var r = await redis.RunAsync(SaleScripts.Audit, ct);
        return new AuditReport(
            r.Int("initialStock"),
            r.Int("stock"),
            r.Int("pendingOrders"),
            r.Int("pendingQuantity"),
            r.Int("paidOrders"),
            r.Int("paidQuantity"),
            r.Int("expiredOrders"),
            r.Int("expiredQuantity"),
            r.Int("charges"),
            r.Int("duplicateCharges"),
            r.Int("usersWithMultipleLiveOrders"),
            r.Int("tickets"),
            r.Int("waiting"),
            r.Int("active"));
    }
}
