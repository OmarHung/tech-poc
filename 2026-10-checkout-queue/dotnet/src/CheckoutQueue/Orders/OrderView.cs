using CheckoutQueue.Redis;

namespace CheckoutQueue.Orders;

public enum OrderStatus
{
    /// <summary>Stock is held until <see cref="OrderView.PaymentExpiresAtMs"/>.</summary>
    PendingPayment,
    Paid,

    /// <summary>Not paid in time; the stock went back on the shelf.</summary>
    Expired,
}

public sealed record OrderView(
    string OrderId,
    string TicketId,
    string UserId,
    int Quantity,
    OrderStatus Status,
    long CreatedAtMs,
    long PaymentExpiresAtMs,
    long? PaidAtMs)
{
    public static OrderView From(ScriptReply reply) => new(
        reply.Get("orderId"),
        reply.Get("ticketId"),
        reply.Get("userId"),
        reply.Int("quantity"),
        WireName.Parse<OrderStatus>(reply.Get("status")),
        reply.Long("createdAt"),
        reply.Long("paymentExpiresAt"),
        reply.OptionalLong("paidAt"));
}

public enum CheckoutOutcome
{
    Created,

    /// <summary>This ticket already ordered the same quantity: the original order, replayed.</summary>
    Existing,
    TicketNotFound,

    /// <summary>Still waiting (or skipped, or sold out). <see cref="CheckoutResult.Detail"/> holds the ticket status.</summary>
    NotAdmitted,
    AdmissionExpired,

    /// <summary>This ticket already ordered a different quantity. <see cref="CheckoutResult.Detail"/> holds that order id.</summary>
    TicketUsed,

    /// <summary>Nothing on the shelf and nothing held by unpaid orders. Final.</summary>
    SoldOut,

    /// <summary>
    /// Less than asked is on the shelf right now, possibly 0 while unpaid orders hold the rest.
    /// <see cref="CheckoutResult.Detail"/> holds how much; retrying while admitted may succeed.
    /// </summary>
    InsufficientStock,
}

public sealed record CheckoutResult(CheckoutOutcome Outcome, OrderView? Order = null, string? Detail = null);

public enum PayOutcome
{
    /// <summary>Charged now.</summary>
    Paid,

    /// <summary>Charged by an earlier request; not charged again.</summary>
    AlreadyPaid,
    OrderNotFound,
    OrderExpired,
}

public sealed record PayResult(PayOutcome Outcome, OrderView? Order = null);
