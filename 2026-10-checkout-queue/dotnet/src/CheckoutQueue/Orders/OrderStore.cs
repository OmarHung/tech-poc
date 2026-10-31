using CheckoutQueue.Redis;

namespace CheckoutQueue.Orders;

/// <summary>Reserving stock for admitted users, taking (simulated) payment, and releasing what goes unpaid.</summary>
public sealed class OrderStore(SaleRedis redis, CheckoutQueueConfig config)
{
    /// <summary>Upper bound on orders one sweep releases, so a mass expiry cannot stall Redis.</summary>
    private const int MaxReleasePerTick = 1_000;

    public async Task<CheckoutResult> CheckoutAsync(string ticketId, int quantity, CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(
            SaleScripts.Checkout, ct, ticketId, quantity, Ids.New(), redis.NowMs, (long)config.PaymentWindow.TotalMilliseconds);
        return reply.Outcome switch
        {
            "created" => new CheckoutResult(CheckoutOutcome.Created, OrderView.From(reply)),
            "existing" => new CheckoutResult(CheckoutOutcome.Existing, OrderView.From(reply)),
            "ticket_not_found" => new CheckoutResult(CheckoutOutcome.TicketNotFound),
            "not_admitted" => new CheckoutResult(CheckoutOutcome.NotAdmitted, Detail: reply.Get("detail")),
            "admission_expired" => new CheckoutResult(CheckoutOutcome.AdmissionExpired),
            "ticket_used" => new CheckoutResult(CheckoutOutcome.TicketUsed, Detail: reply.Get("detail")),
            "sold_out" => new CheckoutResult(CheckoutOutcome.SoldOut),
            "insufficient_stock" => new CheckoutResult(CheckoutOutcome.InsufficientStock, Detail: reply.Get("detail")),
            _ => throw reply.Unexpected(),
        };
    }

    public async Task<OrderView?> GetOrderAsync(string orderId, CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(SaleScripts.Order, ct, orderId, redis.NowMs);
        return reply.Outcome switch
        {
            "ok" => OrderView.From(reply),
            "not_found" => null,
            _ => throw reply.Unexpected(),
        };
    }

    public async Task<PayResult> PayAsync(string orderId, CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(SaleScripts.Pay, ct, orderId, redis.NowMs);
        return reply.Outcome switch
        {
            "paid" => new PayResult(PayOutcome.Paid, OrderView.From(reply)),
            "already_paid" => new PayResult(PayOutcome.AlreadyPaid, OrderView.From(reply)),
            "order_expired" => new PayResult(PayOutcome.OrderExpired, OrderView.From(reply)),
            "order_not_found" => new PayResult(PayOutcome.OrderNotFound),
            _ => throw reply.Unexpected(),
        };
    }

    /// <summary>Puts the stock of unpaid, overdue orders back. Returns how many orders were released.</summary>
    public async Task<int> ExpireOrdersAsync(CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(SaleScripts.ExpireOrders, ct, redis.NowMs, MaxReleasePerTick);
        return reply.Int("released");
    }
}
