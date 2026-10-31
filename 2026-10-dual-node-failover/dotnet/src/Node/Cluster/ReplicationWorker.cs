namespace Node.Cluster;

/// <summary>
/// While this node is primary, ships its log to the standby one batch at a time and heartbeats it when idle.
/// Every answer from the standby renews the peer lease and moves <see cref="PrimaryTerm.MatchIndex"/>.
/// </summary>
public sealed class ReplicationWorker(
    NodeState state,
    RpcClient rpc,
    NodeConfig config,
    ILogger<ReplicationWorker> logger) : BackgroundService
{
    public const int MaxBatch = 256;

    private readonly Uri _replicateUrl = new(config.PeerUrl, "internal/replicate");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var term = await state.WaitForTermAsync(stoppingToken);
            using var running = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, term.Ended);
            try
            {
                await ReplicateTermAsync(term, running.Token);
            }
            // Checked on the sources: term.Ended is cancelled asynchronously, so the linked token can lag behind it.
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested || term.Ended.IsCancellationRequested)
            {
                // Term over or shutting down.
            }
        }
    }

    private async Task ReplicateTermAsync(PrimaryTerm term, CancellationToken ct)
    {
        var log = state.Log;
        var clock = state.Clock;
        var retryDelay = config.RenewInterval / 5;
        var nextIndex = log.LastIndex + 1; // optimistic; a mismatch walks it back
        var lastSent = clock.Now - config.RenewInterval; // heartbeat right away
        Task inSyncCall = Task.CompletedTask;
        if (term.PeerState != PeerState.InSync)
        {
            _ = term.TryGoSoloAsync(); // the witness says the standby is behind; see TryGoSoloAsync
        }
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var appended = term.AppendSignal;
            // Null only if the log shrank, which happens after a newer primary deposed this one and ended the term.
            var slice = log.Read(nextIndex, MaxBatch) ?? throw new OperationCanceledException("log truncated under a deposed primary");
            var idle = clock.Now - lastSent;
            if (slice.Entries.Count == 0 && idle < config.RenewInterval)
            {
                await WaitQuietlyAsync(appended, config.RenewInterval - idle, clock.Time, ct);
                continue;
            }

            var sentAt = clock.Now;
            lastSent = sentAt;
            var request = new ReplicateRequest(config.NodeId, term.Epoch, nextIndex - 1, slice.PrevEpoch, slice.Entries, slice.LastIndex);
            var reply = await SendAsync(request, ct);
            if (reply is null || reply.Status == ReplicateStatus.Rejected)
            {
                if (reply is not null && reply.Epoch > term.Epoch)
                {
                    state.ObserveEpoch(reply.Epoch, "standby reply");
                    return;
                }
                await Task.Delay(retryDelay, clock.Time, ct);
                continue;
            }

            // The standby accepted this node as primary and promised not to seek the lease for a while.
            state.ExtendPeerLease(term, sentAt + TimeSpan.FromMilliseconds(reply.PromiseMs) - config.LeaseSafetyMargin);
            if (reply.Status == ReplicateStatus.Mismatch)
            {
                nextIndex = Math.Max(1, Math.Min(nextIndex - 1, reply.MatchIndex + 1));
                continue;
            }
            nextIndex = reply.MatchIndex + 1;
            if (term.RecordReplicated(reply.MatchIndex) && inSyncCall.IsCompleted)
            {
                // In the background: a slow witness must not hold up replication.
                inSyncCall = term.TryMarkInSyncAsync(ct);
            }
        }
    }

    private async Task<ReplicateReply?> SendAsync(ReplicateRequest request, CancellationToken ct)
    {
        using var response = await rpc.PostAsync(_replicateUrl, request, ct);
        if (response is null)
        {
            return null;
        }
        if (!response.IsSuccessStatusCode)
        {
            logger.LogWarning("Standby answered replication with {Status}", (int)response.StatusCode);
            return null;
        }
        return await RpcClient.ReadAsync<ReplicateReply>(response, ct);
    }

    private static async Task WaitQuietlyAsync(Task signal, TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        try
        {
            await signal.WaitAsync(timeout, time, ct);
        }
        catch (TimeoutException)
        {
            // Time for a heartbeat.
        }
    }
}
