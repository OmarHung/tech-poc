using System.Threading.Channels;
using CheckoutQueue.Redis;
using StackExchange.Redis;

namespace CheckoutQueue.Queue;

/// <summary>
/// Feeds this instance's event streams. Admissions arrive over Redis pub/sub and are pushed at once;
/// everything else (position, wait estimate) is refreshed once per poll interval in one batched
/// call, so a stream never sends more often than polling would.
/// </summary>
public sealed class TicketPushWorker(
    SaleRedis redis,
    QueueStore queue,
    TicketStreams streams,
    CheckoutQueueConfig config,
    TimeProvider time,
    ILogger<TicketPushWorker> logger) : BackgroundService
{
    /// <summary>null asks for a refresh of every open stream; an array names the tickets just admitted.</summary>
    private readonly Channel<string[]?> _work = Channel.CreateUnbounded<string[]?>(new UnboundedChannelOptions { SingleReader = true });

    /// <summary>Last view pushed per ticket. Only the single consumer loop touches it.</summary>
    private readonly Dictionary<string, TicketView> _sent = [];

    private bool _subscribed;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // Subscribe before the app takes requests, so no admission can be missed in between.
        await TrySubscribeAsync();
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var refresh = RefreshEveryPollIntervalAsync(stoppingToken);
        try
        {
            await foreach (var admitted in _work.Reader.ReadAllAsync(stoppingToken))
            {
                try
                {
                    if (!_subscribed)
                    {
                        await TrySubscribeAsync();
                    }
                    await PushAsync(admitted, stoppingToken);
                }
                catch (Exception ex) when (ex is RedisException or TimeoutException)
                {
                    logger.LogWarning(ex, "Pushing ticket updates failed for sale {SaleId}; retrying next refresh", config.SaleId);
                }
            }
        }
        finally
        {
            await refresh;
        }
    }

    private async Task TrySubscribeAsync()
    {
        try
        {
            await redis.Subscriber.SubscribeAsync(redis.AdmittedChannel, (_, message) => _work.Writer.TryWrite(message.ToString().Split(' ')));
            _subscribed = true;
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            logger.LogWarning(ex, "Subscribing to admissions failed for sale {SaleId}; streams fall back to the periodic refresh", config.SaleId);
        }
    }

    private async Task RefreshEveryPollIntervalAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(config.PollInterval, time);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                _work.Writer.TryWrite(null);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutting down.
        }
    }

    private async Task PushAsync(string[]? admitted, CancellationToken ct)
    {
        var open = streams.TicketIds;
        foreach (var closed in _sent.Keys.Except(open).ToList())
        {
            _sent.Remove(closed);
        }
        var wanted = admitted is null ? open : admitted.Intersect(open).ToArray();
        if (wanted.Count == 0)
        {
            return;
        }
        foreach (var view in await queue.WatchAsync(wanted, ct))
        {
            if (_sent.TryGetValue(view.TicketId, out var last) && last == view)
            {
                continue;
            }
            _sent[view.TicketId] = view;
            streams.Publish(view);
        }
    }
}
