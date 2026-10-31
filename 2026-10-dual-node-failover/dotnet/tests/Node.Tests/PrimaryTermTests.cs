using Microsoft.Extensions.Time.Testing;
using Node.Cluster;
using Node.Storage;
using Shared;

namespace Node.Tests;

public sealed class PrimaryTermTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(1);

    private readonly TempDir _dir = new();
    private readonly ReplicatedLog _log;
    private readonly FakeTimeProvider _time = new();
    private readonly MonotonicClock _clock;
    private readonly FakeWitness _witness = new();

    public PrimaryTermTests()
    {
        _log = ReplicatedLog.Open(_dir.File("log.jsonl"));
        _clock = new MonotonicClock(_time);
    }

    public void Dispose()
    {
        _log.Dispose();
        _dir.Dispose();
    }

    [Fact]
    public async Task InSync_WriteIsDurableOnceTheStandbyHasIt()
    {
        var term = NewTerm(standbyInSync: true);
        var entry = _log.Append(1, "k", "v");

        var wait = term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
        Assert.False(wait.IsCompleted);
        term.RecordReplicated(entry.Index);

        await wait;
        Assert.Empty(_witness.Requests);
    }

    [Fact]
    public async Task InSync_SlowStandby_GoesSoloWhenTheWitnessAgrees()
    {
        var term = NewTerm(standbyInSync: true);
        var entry = _log.Append(1, "k", "v");

        var wait = term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
        _time.Advance(Timeout);

        await wait;
        Assert.Equal(PeerState.Solo, term.PeerState);
        Assert.Equal(new[] { (false, 10L) }, _witness.Requests);
    }

    [Fact]
    public async Task InSync_SlowStandby_FailsTheWriteWhenTheWitnessRefuses()
    {
        _witness.Reachable = false;
        var term = NewTerm(standbyInSync: true);
        var entry = _log.Append(1, "k", "v");

        var wait = term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
        _time.Advance(Timeout);

        var ex = await Assert.ThrowsAsync<UnavailableException>(() => wait);
        Assert.Contains("witness", ex.Message);
        Assert.Equal(PeerState.InSync, term.PeerState);
    }

    [Fact]
    public async Task StaleStart_WritesWaitUntilTheWitnessAgreesToSolo()
    {
        var term = NewTerm(standbyInSync: false);
        Assert.Equal(PeerState.CatchingUp, term.PeerState);
        var entry = _log.Append(1, "k", "v");

        var wait = term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
        Assert.False(wait.IsCompleted);
        Assert.True(await term.TryGoSoloAsync());

        await wait;
        Assert.Equal(PeerState.Solo, term.PeerState);
    }

    [Fact]
    public async Task Solo_WriteIsDurableRightAway()
    {
        var term = await SoloTermAsync();
        var entry = _log.Append(1, "k", "v");

        await term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
    }

    [Fact]
    public async Task Solo_CatchesUp_ThenReportsInSyncAgainstTheLatestVersion()
    {
        var term = await SoloTermAsync(); // witness version 10 -> 11
        _log.Append(1, "a", "1");
        _log.Append(1, "b", "2");

        Assert.True(term.RecordReplicated(2));
        Assert.Equal(PeerState.CatchingUp, term.PeerState);
        await term.TryMarkInSyncAsync(CancellationToken.None);

        Assert.Equal(PeerState.InSync, term.PeerState);
        Assert.Equal((true, 11L), _witness.Requests[^1]);
        Assert.True(_witness.StandbyInSync);
    }

    [Fact]
    public async Task InSyncReport_IsRefused_WhenTheWitnessChangedInBetween()
    {
        var term = await SoloTermAsync();
        _log.Append(1, "a", "1");
        term.RecordReplicated(1);
        _witness.Version += 5; // e.g. a late "not in sync" from an older process got there first

        await term.TryMarkInSyncAsync(CancellationToken.None);

        Assert.Equal(PeerState.CatchingUp, term.PeerState);
        Assert.False(_witness.StandbyInSync);
    }

    [Fact]
    public async Task CatchingUp_NewWritesWaitForTheStandbyAgain()
    {
        var term = await SoloTermAsync();
        _log.Append(1, "a", "1");
        term.RecordReplicated(0); // within the slack: catch-up target is index 1
        Assert.Equal(PeerState.CatchingUp, term.PeerState);
        var entry = _log.Append(1, "b", "2");

        var wait = term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
        Assert.True(term.RecordReplicated(1));
        await term.TryMarkInSyncAsync(CancellationToken.None);
        Assert.Equal(PeerState.InSync, term.PeerState);
        Assert.False(wait.IsCompleted);
        term.RecordReplicated(2);

        await wait;
    }

    [Fact]
    public async Task CatchingUp_DoesNotReportBeforeTheTarget()
    {
        var term = await SoloTermAsync();
        _log.Append(1, "a", "1");
        _log.Append(1, "b", "2");

        Assert.False(term.RecordReplicated(1));
        await term.TryMarkInSyncAsync(CancellationToken.None);

        Assert.Equal(PeerState.CatchingUp, term.PeerState);
        Assert.DoesNotContain(_witness.Requests, r => r.InSync);
    }

    [Fact]
    public async Task Solo_StaysSolo_WhileTheStandbyIsFarBehind()
    {
        var term = await SoloTermAsync(catchUpSlack: 1);
        _log.Append(1, "a", "1");
        _log.Append(1, "b", "2");
        _log.Append(1, "c", "3");

        Assert.False(term.RecordReplicated(1));
        Assert.Equal(PeerState.Solo, term.PeerState);
    }

    [Fact]
    public void WitnessSayingBehind_SendsAnInSyncStandbyThroughCatchingUp()
    {
        var term = NewTerm(standbyInSync: true);
        _log.Append(1, "a", "1");

        term.ObserveWitness(11, standbyInSync: false); // a "go on alone" whose reply was lost

        Assert.Equal(PeerState.CatchingUp, term.PeerState);
        Assert.True(term.RecordReplicated(1));
    }

    [Fact]
    public void WitnessReplies_OlderThanOneAlreadySeen_AreIgnored()
    {
        var term = NewTerm(standbyInSync: true);

        term.ObserveWitness(9, standbyInSync: false);

        Assert.Equal(PeerState.InSync, term.PeerState);
    }

    [Fact]
    public async Task EndedTerm_FailsWritesStillWaiting()
    {
        var term = NewTerm(standbyInSync: true);
        var entry = _log.Append(1, "k", "v");

        var wait = term.WaitDurableAsync(entry.Index, Timeout, _clock, CancellationToken.None);
        term.End();

        await Assert.ThrowsAsync<UnavailableException>(() => wait);
        Assert.True(term.Ended.IsCancellationRequested);
        Assert.False(await term.TryGoSoloAsync());
    }

    [Fact]
    public async Task AppendSignal_FiresOnTheNextAppend()
    {
        var term = NewTerm(standbyInSync: true);
        var signal = term.AppendSignal;

        Assert.False(signal.IsCompleted);
        term.NotifyAppended();

        await signal;
        Assert.False(term.AppendSignal.IsCompleted);
    }

    private PrimaryTerm NewTerm(bool standbyInSync, int catchUpSlack = 16) =>
        new(1, standbyInSync, _witness.Version, _log, _witness, catchUpSlack);

    private async Task<PrimaryTerm> SoloTermAsync(int catchUpSlack = 16)
    {
        var term = NewTerm(standbyInSync: false, catchUpSlack);
        Assert.True(await term.TryGoSoloAsync());
        return term;
    }

    /// <summary>Behaves like <c>LeaseTable.SetStandbyInSync</c> for the holder in its own epoch.</summary>
    private sealed class FakeWitness : IArbiter
    {
        public long Version { get; set; } = 10;
        public bool StandbyInSync { get; private set; }
        public bool Reachable { get; set; } = true;
        public List<(bool InSync, long ExpectedVersion)> Requests { get; } = [];

        public Task<SyncReply?> SetStandbyInSyncAsync(long epoch, bool inSync, long expectedVersion, CancellationToken ct)
        {
            Requests.Add((inSync, expectedVersion));
            if (!Reachable || (inSync && expectedVersion != Version))
            {
                return Task.FromResult<SyncReply?>(null);
            }
            Version++;
            StandbyInSync = inSync;
            return Task.FromResult<SyncReply?>(new SyncReply(epoch, inSync, Version));
        }
    }
}
