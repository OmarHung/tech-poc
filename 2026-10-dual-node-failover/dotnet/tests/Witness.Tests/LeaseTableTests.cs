using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shared;

namespace Witness.Tests;

public sealed class LeaseTableTests : IDisposable
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(3);

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "witness-tests-" + Guid.NewGuid().ToString("N"));
    private readonly FakeTimeProvider _time = new();

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    [Fact]
    public void FirstRequest_GetsEpochOne_AndTheOtherNodeIsNotYetEligible()
    {
        var reply = NewTable().Acquire("a");

        Assert.True(reply.Granted);
        Assert.Equal("a", reply.Holder);
        Assert.Equal(1, reply.Epoch);
        Assert.Equal(3000, reply.TtlMs);
        Assert.False(reply.StandbyInSync);
    }

    [Fact]
    public void Holder_RenewsInTheSameEpoch_EvenAfterExpiry()
    {
        var table = NewTable();
        table.Acquire("a");

        _time.Advance(Ttl * 2);
        var reply = table.Acquire("a");

        Assert.True(reply.Granted);
        Assert.Equal(1, reply.Epoch);
    }

    [Fact]
    public void OtherNode_IsRefused_WhileTheLeaseIsValid()
    {
        var table = NewTable();
        table.Acquire("a");
        MarkInSync(table, "a", 1);

        _time.Advance(Ttl - TimeSpan.FromMilliseconds(1));
        var reply = table.Acquire("b");

        Assert.False(reply.Granted);
        Assert.Equal("a", reply.Holder);
        Assert.Contains("held by a", reply.Reason);
    }

    [Fact]
    public void InSyncStandby_TakesOver_AfterExpiry_WithANewEpoch()
    {
        var table = NewTable();
        table.Acquire("a");
        MarkInSync(table, "a", 1);

        _time.Advance(Ttl);
        var reply = table.Acquire("b");

        Assert.True(reply.Granted);
        Assert.Equal("b", reply.Holder);
        Assert.Equal(2, reply.Epoch);
        Assert.False(reply.StandbyInSync); // a is behind until b says otherwise
    }

    [Fact]
    public void StaleStandby_IsNeverPromoted()
    {
        var table = NewTable();
        table.Acquire("a"); // standby starts out not in sync

        _time.Advance(Ttl * 10);
        var reply = table.Acquire("b");

        Assert.False(reply.Granted);
        Assert.Equal("a", reply.Holder);
        Assert.Contains("missing acknowledged writes", reply.Reason);
    }

    [Fact]
    public void DeposedHolder_IsRefused_AndCannotChangeTheSyncFlag()
    {
        var table = NewTable();
        table.Acquire("a");
        MarkInSync(table, "a", 1);
        _time.Advance(Ttl);
        table.Acquire("b");

        Assert.False(table.Acquire("a").Granted);
        Assert.Throws<ConflictException>(() => table.SetStandbyInSync(new SyncRequest("a", 1, InSync: false)));
        Assert.Throws<ConflictException>(() => table.SetStandbyInSync(new SyncRequest("b", 1, InSync: true)));
    }

    [Fact]
    public void State_SurvivesARestart_AndTheHolderKeepsTheLeaseForOneMoreTtl()
    {
        var before = NewTable();
        before.Acquire("a");
        MarkInSync(before, "a", 1);
        _time.Advance(Ttl * 5);

        var after = NewTable();

        Assert.Equal(new WitnessState(1, "a", StandbyInSync: true, SyncVersion: 2), after.State);
        Assert.False(after.Acquire("b").Granted);
        _time.Advance(Ttl);
        Assert.True(after.Acquire("b").Granted);
    }

    [Fact]
    public void InSyncReport_IsRefused_IfTheStateChangedSinceTheHolderLooked()
    {
        var table = NewTable();
        var seen = table.Acquire("a").SyncVersion;
        table.SetStandbyInSync(new SyncRequest("a", 1, InSync: false)); // e.g. sent after the report below, arrived first

        Assert.Throws<ConflictException>(() => table.SetStandbyInSync(new SyncRequest("a", 1, InSync: true, seen)));
        Assert.False(table.State.StandbyInSync);
    }

    [Fact]
    public void NotInSyncReports_AreAlwaysAccepted_AndEveryReportBumpsTheVersion()
    {
        var table = NewTable();
        var granted = table.Acquire("a").SyncVersion;

        var first = table.SetStandbyInSync(new SyncRequest("a", 1, InSync: false, ExpectedVersion: 999));
        var second = table.SetStandbyInSync(new SyncRequest("a", 1, InSync: true, first.SyncVersion));

        Assert.Equal(granted + 1, first.SyncVersion);
        Assert.Equal(new SyncReply(1, StandbyInSync: true, granted + 2), second);
    }

    [Fact]
    public void Handover_BumpsTheVersion_SoOldReportsCannotApply()
    {
        var table = NewTable();
        table.Acquire("a");
        MarkInSync(table, "a", 1);
        var before = table.State.SyncVersion;
        _time.Advance(Ttl);

        var reply = table.Acquire("b");

        Assert.Equal(before + 1, reply.SyncVersion);
    }

    [Fact]
    public void FailedWrite_LeavesTheLeaseWhereItWas()
    {
        var table = NewTable();
        table.Acquire("a");
        MarkInSync(table, "a", 1);
        var statePath = Path.Combine(_dataDir, "witness.json");
        File.Delete(statePath);
        Directory.CreateDirectory(statePath); // the next write fails
        _time.Advance(Ttl);

        Assert.ThrowsAny<Exception>(() => table.Acquire("b"));

        Assert.Equal(new WitnessState(1, "a", StandbyInSync: true, SyncVersion: 2), table.State);
    }

    [Theory]
    [InlineData("c")]
    [InlineData("")]
    [InlineData(null)]
    public void UnknownNodes_AreRejected(string? node)
    {
        var table = NewTable();

        Assert.Throws<InvalidInputException>(() => table.Acquire(node));
        Assert.Throws<InvalidInputException>(() => table.SetStandbyInSync(new SyncRequest(node, 0, InSync: true)));
    }

    private static void MarkInSync(LeaseTable table, string node, long epoch) =>
        table.SetStandbyInSync(new SyncRequest(node, epoch, InSync: true, table.State.SyncVersion));

    private LeaseTable NewTable() =>
        new(new WitnessConfig { DataDir = _dataDir, LeaseTtl = Ttl }, _time, NullLogger<LeaseTable>.Instance);
}
