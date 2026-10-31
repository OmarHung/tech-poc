using CheckoutQueue.Redis;
using StackExchange.Redis;

namespace CheckoutQueue.Queue;

/// <summary>The virtual waiting room: take a number, poll, get let in when a slot frees up.</summary>
public sealed class QueueStore(SaleRedis redis, CheckoutQueueConfig config)
{
    /// <summary>Upper bound on tickets one admission pass looks at, so a long run of no-shows cannot stall Redis.</summary>
    private const int MaxScanPerTick = 1_000;

    /// <summary>Wait estimates use the admission rate over this many most recent seconds.</summary>
    private const int AdmitRateWindowSeconds = 10;

    /// <summary>Tickets per watch.lua call, so one call never blocks Redis for long.</summary>
    private const int MaxWatchBatch = 500;

    public async Task<JoinResult> JoinAsync(string userId, CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(SaleScripts.Join, ct, userId, Ids.New(), redis.NowMs);
        return reply.Outcome switch
        {
            "created" => new JoinResult(JoinOutcome.Created, TicketView.From(reply, config)),
            "existing" => new JoinResult(JoinOutcome.Existing, TicketView.From(reply, config)),
            "sold_out" => new JoinResult(JoinOutcome.SoldOut, null),
            _ => throw reply.Unexpected(),
        };
    }

    /// <summary>Current view of a ticket. Also counts as the ticket's heartbeat.</summary>
    public async Task<TicketView?> GetTicketAsync(string ticketId, CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(SaleScripts.Ticket, ct, ticketId, redis.NowMs);
        return reply.Outcome switch
        {
            "ok" => TicketView.From(reply, config),
            "not_found" => null,
            _ => throw reply.Unexpected(),
        };
    }

    /// <summary>Views of many tickets in a few round trips, for event streams. Counts as their heartbeat.</summary>
    public async Task<IReadOnlyList<TicketView>> WatchAsync(IReadOnlyCollection<string> ticketIds, CancellationToken ct = default)
    {
        var views = new List<TicketView>();
        foreach (var batch in ticketIds.Chunk(MaxWatchBatch))
        {
            RedisValue[] args = [redis.NowMs, .. batch.Select(id => (RedisValue)id)];
            var rows = await redis.RunRowsAsync(SaleScripts.Watch, ct, args);
            views.AddRange(rows.Select(row => TicketView.From(row, config)));
        }
        return views;
    }

    /// <summary>One admission pass: drop lapsed admissions, then let in as many as there are free slots.</summary>
    public async Task<AdmitResult> AdmitAsync(CancellationToken ct = default)
    {
        var reply = await redis.RunAsync(
            SaleScripts.Admit,
            ct,
            redis.NowMs,
            config.MaxActiveSessions,
            (long)config.AdmissionWindow.TotalMilliseconds,
            (long)config.HeartbeatTimeout.TotalMilliseconds,
            MaxScanPerTick,
            AdmitRateWindowSeconds);
        return new AdmitResult(reply.Int("admitted"), reply.Int("abandoned"), reply.Int("expired"));
    }
}
