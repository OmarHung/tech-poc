using CheckoutQueue.Orders;
using CheckoutQueue.Queue;
using CheckoutQueue.Redis;

namespace CheckoutQueue.Admin;

public sealed record BoardEntry(TicketView Ticket, OrderView? Order);

/// <summary>
/// Every user's current ticket and order, newest first. The demo page draws its lanes from this,
/// so every tab on every instance shows the same line.
/// </summary>
public sealed class TicketBoardStore(SaleRedis redis, CheckoutQueueConfig config)
{
    /// <summary>Enough for a hand-driven demo; a real line would page through it instead.</summary>
    public const int MaxEntries = 300;

    public async Task<IReadOnlyList<BoardEntry>> ListAsync(CancellationToken ct = default)
    {
        var rows = await redis.RunRowsAsync(SaleScripts.Tickets, ct, redis.NowMs, MaxEntries);
        var entries = new List<BoardEntry>();
        foreach (var row in rows)
        {
            if (row.Outcome == "ticket")
            {
                entries.Add(new BoardEntry(TicketView.From(row, config), null));
            }
            else if (row.Outcome == "order" && entries.Count > 0)
            {
                entries[^1] = entries[^1] with { Order = OrderView.From(row) };
            }
            else
            {
                throw row.Unexpected();
            }
        }
        return entries;
    }
}
