using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Node.Cluster;
using Node.Storage;
using Shared;

namespace Node.Tests;

public sealed class NodeStateTests : IDisposable
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(3);

    private readonly TempDir _dir = new();
    private readonly FakeTimeProvider _time = new();
    private readonly List<ReplicatedLog> _logs = [];

    public void Dispose()
    {
        _logs.ForEach(log => log.Dispose());
        _dir.Dispose();
    }

    [Fact]
    public void Standby_RefusesWrites()
    {
        var state = NewState();

        Assert.Null(state.ServingTerm());
        var ex = Assert.Throws<UnavailableException>(() => state.AppendAsPrimary("k", "v", out _));
        Assert.Equal("not primary", ex.Message);
        Assert.Equal("standby", state.Health().Role);
    }

    [Fact]
    public void Primary_ServesUntilItsLeaseRunsOut()
    {
        var state = NewState();
        state.TryPromote(1, state.Clock.Now + Ttl, standbyInSync: false);

        var entry = state.AppendAsPrimary("k", "v", out var term);
        Assert.Equal(new LogEntry(1, 1, "k", "v"), entry);
        Assert.Equal("primary", state.Health().Role);

        _time.Advance(Ttl);
        Assert.Null(state.ServingTerm());
        Assert.Throws<UnavailableException>(() => state.AppendAsPrimary("k", "v2", out _));
        state.StepDownIfLeaseExpired();
        Assert.Null(state.CurrentTerm);
        Assert.True(term.Ended.IsCancellationRequested);
    }

    [Fact]
    public void PeerLease_KeepsThePrimaryServing_WithoutTheWitness()
    {
        var state = NewState();
        state.TryPromote(1, state.Clock.Now + Ttl, standbyInSync: true);
        var term = state.CurrentTerm!;

        _time.Advance(Ttl - TimeSpan.FromSeconds(1));
        state.ExtendPeerLease(term, state.Clock.Now + Ttl);
        _time.Advance(TimeSpan.FromSeconds(2));

        Assert.Same(term, state.ServingTerm());
    }

    [Fact]
    public void LeaseExtensions_ForAnOldTerm_AreIgnored()
    {
        var state = NewState();
        state.TryPromote(1, state.Clock.Now + Ttl, standbyInSync: false);
        var old = state.CurrentTerm!;
        state.TryPromote(2, state.Clock.Now + Ttl, standbyInSync: false);

        state.ExtendWitnessLease(old, state.Clock.Now + Ttl * 10);
        state.ExtendPeerLease(old, state.Clock.Now + Ttl * 10);
        state.StepDown(old, "ignored");
        _time.Advance(Ttl);

        Assert.True(old.Ended.IsCancellationRequested);
        Assert.Null(state.ServingTerm());
        Assert.NotNull(state.CurrentTerm);
    }

    [Fact]
    public void TryPromote_RefusesAnOlderEpoch()
    {
        var state = NewState();
        state.ObserveEpoch(5, "test");

        Assert.False(state.TryPromote(4, state.Clock.Now + Ttl, standbyInSync: false));
        Assert.True(state.TryPromote(5, state.Clock.Now + Ttl, standbyInSync: false));
    }

    [Fact]
    public async Task WaitForTermAsync_CompletesOnPromotion()
    {
        var state = NewState();
        var waiting = state.WaitForTermAsync(CancellationToken.None);
        Assert.False(waiting.IsCompleted);

        state.TryPromote(1, state.Clock.Now + Ttl, standbyInSync: false);

        Assert.Same(state.CurrentTerm, await waiting);
        Assert.Same(state.CurrentTerm, await state.WaitForTermAsync(CancellationToken.None));
    }

    [Fact]
    public void Replicate_AppliesEntriesFromTheCurrentPrimary()
    {
        var state = NewState();

        var reply = state.HandleReplicate(Request(epoch: 1, prevIndex: 0, [new LogEntry(1, 1, "k", "v")]));

        Assert.Equal(new ReplicateReply(ReplicateStatus.Ok, 1, 1, 3000, null), reply);
        Assert.Equal("v", state.Log.Get("k")?.Value);
    }

    [Fact]
    public void Replicate_ReportsAMismatch()
    {
        var state = NewState();

        var reply = state.HandleReplicate(Request(epoch: 1, prevIndex: 3, []));

        Assert.Equal(ReplicateStatus.Mismatch, reply.Status);
        Assert.Equal(0, reply.MatchIndex);
    }

    [Fact]
    public void Replicate_FromADeposedPrimary_IsRejectedWithTheNewerEpoch()
    {
        var state = NewState();
        state.ObserveEpoch(3, "test");

        var reply = state.HandleReplicate(Request(epoch: 2, prevIndex: 0, [new LogEntry(1, 2, "k", "v")]));

        Assert.Equal(ReplicateStatus.Rejected, reply.Status);
        Assert.Equal(3, reply.Epoch);
        Assert.Null(state.Log.Get("k"));
    }

    [Fact]
    public void Replicate_WithANewerEpoch_DeposesThisPrimary()
    {
        var state = NewState();
        state.TryPromote(1, state.Clock.Now + Ttl, standbyInSync: false);
        var term = state.CurrentTerm!;

        var reply = state.HandleReplicate(Request(epoch: 2, prevIndex: 0, []));

        Assert.Equal(ReplicateStatus.Ok, reply.Status);
        Assert.Null(state.CurrentTerm);
        Assert.True(term.Ended.IsCancellationRequested);
        Assert.Equal(2, state.Epoch);
    }

    [Fact]
    public void Replicate_InTheSameEpoch_WhilePrimary_IsRejected()
    {
        var state = NewState();
        state.TryPromote(1, TimeSpan.MaxValue, standbyInSync: false);

        var reply = state.HandleReplicate(Request(epoch: 1, prevIndex: 0, []));

        Assert.Equal(ReplicateStatus.Rejected, reply.Status);
        Assert.NotNull(state.ServingTerm());
    }

    [Fact]
    public void Candidacy_StartsOnlyAfterAFullLeaseOfSilence()
    {
        var state = NewState();

        Assert.False(state.TryBeginCandidacy()); // just started: the old primary may still count on us
        _time.Advance(Ttl);
        state.HandleReplicate(Request(epoch: 1, prevIndex: 0, []));
        _time.Advance(Ttl - TimeSpan.FromMilliseconds(1));
        Assert.False(state.TryBeginCandidacy());
        _time.Advance(TimeSpan.FromMilliseconds(1));

        Assert.True(state.TryBeginCandidacy());
        Assert.False(state.TryBeginCandidacy());
    }

    [Fact]
    public void Candidate_RefusesReplication_SoNoPrimaryCanCountOnIt()
    {
        var state = NewState();
        _time.Advance(Ttl);
        Assert.True(state.TryBeginCandidacy());

        var reply = state.HandleReplicate(Request(epoch: 1, prevIndex: 0, [new LogEntry(1, 1, "k", "v")]));

        Assert.Equal(ReplicateStatus.Rejected, reply.Status);
        Assert.Equal(1, state.Epoch);
        Assert.Null(state.Log.Get("k"));
        state.EndCandidacy();
        Assert.Equal(ReplicateStatus.Ok, state.HandleReplicate(Request(epoch: 1, prevIndex: 0, [])).Status);
    }

    [Fact]
    public void ElectionDelay_PostponesCandidacy()
    {
        var state = NewState(config => config with { ElectionDelay = TimeSpan.FromSeconds(1) });

        _time.Advance(Ttl);
        Assert.False(state.TryBeginCandidacy());
        _time.Advance(TimeSpan.FromSeconds(1));

        Assert.True(state.TryBeginCandidacy());
    }

    [Fact]
    public void Epoch_SurvivesARestart()
    {
        NewState().ObserveEpoch(7, "test");

        Assert.Equal(7, NewState().Epoch);
    }

    [Fact]
    public void Health_DescribesAPrimary()
    {
        var state = NewState();
        state.TryPromote(4, state.Clock.Now + Ttl, standbyInSync: false);
        state.AppendAsPrimary("k", "v", out _);

        var health = state.Health();

        Assert.Equal(new NodeHealth("ok", "a", "primary", 4, 1, 0, "catching-up", 3000, 0, "witness", "sync"), health);
    }

    private NodeState NewState(Func<NodeConfig, NodeConfig>? tweak = null)
    {
        var config = (tweak ?? (c => c))(new NodeConfig { DataDir = _dir.Path, LeaseTtl = Ttl });
        var log = ReplicatedLog.Open(Path.Combine(_dir.Path, $"log-{_logs.Count}.jsonl"));
        _logs.Add(log);
        return new NodeState(config, log, new NoArbiter(), _time, NullLogger<NodeState>.Instance);
    }

    private static ReplicateRequest Request(long epoch, long prevIndex, IReadOnlyList<LogEntry> entries) =>
        new("b", epoch, prevIndex, prevIndex == 0 ? 0 : epoch, entries, prevIndex + entries.Count);
}
