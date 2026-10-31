using Shared;

namespace Node.Cluster;

/// <summary>Decides when this node becomes or stops being primary. Runs every RENEW_INTERVAL_MS.</summary>
public sealed class ElectionWorker(NodeState state, WitnessClient witness, NodeConfig config, ILogger<ElectionWorker> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(config.RenewInterval, state.Clock.Time);
        do
        {
            if (config.Arbiter == ArbiterMode.Witness)
            {
                await TickWithWitnessAsync(stoppingToken);
            }
            else
            {
                TickWithoutWitness();
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    internal async Task TickWithWitnessAsync(CancellationToken ct)
    {
        state.StepDownIfLeaseExpired();
        if (state.CurrentTerm is { } term)
        {
            await RenewAsync(term, ct);
            return;
        }
        if (!state.TryBeginCandidacy())
        {
            return;
        }
        try
        {
            var sentAt = state.Clock.Now;
            var reply = await witness.AcquireAsync(ct);
            if (reply is null)
            {
                return;
            }
            if (!reply.Granted)
            {
                state.ObserveEpoch(reply.Epoch, "witness");
                logger.LogDebug("Witness refused the lease: {Reason}", reply.Reason);
                return;
            }
            state.TryPromote(reply.Epoch, LeaseEnd(sentAt, reply), reply.StandbyInSync, reply.SyncVersion);
        }
        finally
        {
            state.EndCandidacy();
        }
    }

    /// <summary>The classic setup: the standby promotes itself once the primary has been quiet for a full lease.</summary>
    internal void TickWithoutWitness()
    {
        if (state.CurrentTerm is not null || !state.TryBeginCandidacy())
        {
            return;
        }
        try
        {
            state.TryPromote(state.Epoch + 1, TimeSpan.MaxValue, standbyInSync: false);
        }
        finally
        {
            state.EndCandidacy();
        }
    }

    private async Task RenewAsync(PrimaryTerm term, CancellationToken ct)
    {
        var sentAt = state.Clock.Now;
        var reply = await witness.AcquireAsync(ct);
        if (reply is null)
        {
            return; // the peer lease may still carry this node
        }
        if (reply.Granted && reply.Epoch == term.Epoch)
        {
            state.ExtendWitnessLease(term, LeaseEnd(sentAt, reply));
            term.ObserveWitness(reply.SyncVersion, reply.StandbyInSync);
            return;
        }
        if (reply.Granted)
        {
            state.TryPromote(reply.Epoch, LeaseEnd(sentAt, reply), reply.StandbyInSync, reply.SyncVersion);
            return;
        }
        state.ObserveEpoch(reply.Epoch, "witness");
        state.StepDown(term, $"witness refused renewal: {reply.Reason}");
    }

    /// <summary>The lease started no later than when the request was sent; end it a margin early for clock drift.</summary>
    private TimeSpan LeaseEnd(TimeSpan sentAt, LeaseReply reply) =>
        sentAt + TimeSpan.FromMilliseconds(reply.TtlMs) - config.LeaseSafetyMargin;
}
