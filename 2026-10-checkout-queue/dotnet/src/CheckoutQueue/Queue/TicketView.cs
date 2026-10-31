using System.Globalization;
using CheckoutQueue.Redis;

namespace CheckoutQueue.Queue;

public enum TicketStatus
{
    /// <summary>In line. Keep polling; position and estimate tell how far.</summary>
    Waiting,

    /// <summary>Allowed to check out until <see cref="TicketView.AdmissionExpiresAtMs"/>.</summary>
    Admitted,

    /// <summary>Placed an order; see <see cref="TicketView.OrderId"/>.</summary>
    Ordered,

    /// <summary>Was admitted but did not order in time. May join again, at the back.</summary>
    Expired,

    /// <summary>Stopped polling and was skipped. May join again, at the back.</summary>
    Abandoned,

    /// <summary>Nothing left to sell and nothing held by unpaid orders. Final: stop polling.</summary>
    SoldOut,
}

/// <summary>What a client sees when it polls its ticket.</summary>
public sealed record TicketView(
    string TicketId,
    string UserId,
    long Seq,
    TicketStatus Status,
    long? Position,
    int? EstimatedWaitSeconds,
    long? AdmittedAtMs,
    long? AdmissionExpiresAtMs,
    string? OrderId,
    int PollAfterMs)
{
    /// <summary>Below this many admissions per second the estimate would be noise (or hours), so none is given.</summary>
    private const double MinAdmitRatePerSecond = 0.01;

    public static TicketView From(ScriptReply reply, CheckoutQueueConfig config)
    {
        var position = reply.OptionalLong("position");
        return new TicketView(
            reply.Get("ticketId"),
            reply.Get("userId"),
            reply.Long("seq"),
            WireName.Parse<TicketStatus>(reply.Get("status")),
            position,
            Estimate(position, reply.Find("admitRate")),
            reply.OptionalLong("admittedAt"),
            reply.OptionalLong("admissionExpiresAt"),
            reply.Find("orderId"),
            (int)config.PollInterval.TotalMilliseconds);
    }

    private static int? Estimate(long? position, string? admitRate)
    {
        if (position is not { } ahead || admitRate is null)
        {
            return null;
        }
        var rate = double.Parse(admitRate, NumberStyles.Float, CultureInfo.InvariantCulture);
        return double.IsFinite(rate) && rate >= MinAdmitRatePerSecond ? (int)Math.Ceiling(ahead / rate) : null;
    }
}

public enum JoinOutcome
{
    Created,
    Existing,
    SoldOut,
}

public sealed record JoinResult(JoinOutcome Outcome, TicketView? Ticket);

public sealed record AdmitResult(int Admitted, int Abandoned, int Expired);
