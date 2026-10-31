using Node.Storage;
using Shared;

namespace Node.Tests;

public sealed class ReplicatedLogTests : IDisposable
{
    private const long LeaderEpoch = 5;

    private readonly TempDir _dir = new();

    public void Dispose() => _dir.Dispose();

    [Fact]
    public void Append_NumbersEntries_AndTheySurviveAReopen()
    {
        using (var log = Open())
        {
            Assert.Equal(new LogEntry(1, 1, "k1", "v1"), log.Append(1, "k1", "v1"));
            Assert.Equal(new LogEntry(2, 2, "k1", "v2"), log.Append(2, "k1", "v2"));
        }

        using var reopened = Open();

        Assert.Equal(2, reopened.LastIndex);
        Assert.Equal(2, reopened.LastEpoch);
        Assert.Equal("v2", reopened.Get("k1")?.Value);
    }

    [Fact]
    public void Read_ReturnsTheBatchWithItsSurroundings()
    {
        using var log = Open();
        log.Append(1, "a", "1");
        log.Append(2, "b", "2");
        log.Append(2, "c", "3");

        var slice = log.Read(2, max: 1)!;

        Assert.Equal(1, slice.PrevEpoch);
        Assert.Equal([new LogEntry(2, 2, "b", "2")], slice.Entries);
        Assert.Equal(3, slice.LastIndex);
        Assert.Empty(log.Read(4, 10)!.Entries);
        Assert.Null(log.Read(5, 10));
    }

    [Fact]
    public void AppendFrom_AddsEntriesThatContinueTheLog()
    {
        using var log = Open();

        var outcome = log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 1, "a"), Entry(2, 1, "b")], leaderLastIndex: 2);

        Assert.Equal(new AppendOutcome(AppendStatus.Ok, 2), outcome);
        Assert.Equal(2, log.LastIndex);
    }

    [Fact]
    public void AppendFrom_ReportsAMismatch_WhenThePreviousEntryIsMissingOrDifferent()
    {
        using var log = Open();
        log.Append(1, "a", "1");
        log.Append(1, "b", "2");

        Assert.Equal(new AppendOutcome(AppendStatus.Mismatch, 2), log.AppendFrom(LeaderEpoch, 5, 3, [], leaderLastIndex: 5));
        Assert.Equal(new AppendOutcome(AppendStatus.Mismatch, 0), log.AppendFrom(LeaderEpoch, 2, 2, [], leaderLastIndex: 2));
        Assert.Equal(2, log.LastIndex);
    }

    [Fact]
    public void AppendFrom_MalformedStartOfLog_IsAMismatchNotACrash()
    {
        using var log = Open();
        log.Append(1, "a", "1");

        Assert.Equal(new AppendOutcome(AppendStatus.Mismatch, 0), log.AppendFrom(LeaderEpoch, 0, 3, [], leaderLastIndex: 0));
    }

    [Fact]
    public void AppendFrom_Mismatch_SkipsTheWholeDisagreeingEpoch()
    {
        using var log = Open();
        log.Append(1, "a", "1");
        log.Append(1, "b", "2");
        log.Append(2, "x", "3"); // written alone by a deposed primary
        log.Append(2, "y", "4");
        log.Append(2, "z", "5");

        var outcome = log.AppendFrom(LeaderEpoch, 5, 3, [], leaderLastIndex: 6);

        Assert.Equal(new AppendOutcome(AppendStatus.Mismatch, 2), outcome);
    }

    [Fact]
    public void AppendFrom_IgnoresEntriesItAlreadyHas()
    {
        using var log = Open();
        log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 1, "a"), Entry(2, 1, "b")], leaderLastIndex: 2);

        var outcome = log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 1, "a"), Entry(2, 1, "b"), Entry(3, 1, "c")], leaderLastIndex: 3);

        Assert.Equal(new AppendOutcome(AppendStatus.Ok, 3), outcome);
        Assert.Equal(3, log.LastIndex);
        Assert.Equal(0, log.TruncatedEntries);
    }

    [Fact]
    public void AppendFrom_ReplacesEntriesANewerPrimaryNeverHad()
    {
        using (var log = Open())
        {
            log.Append(1, "k", "old");
            log.Append(1, "k", "unacknowledged"); // the old primary wrote this, then crashed

            var outcome = log.AppendFrom(LeaderEpoch, 1, 1, [Entry(2, 2, "other")], leaderLastIndex: 2);

            Assert.Equal(new AppendOutcome(AppendStatus.Ok, 2), outcome);
            Assert.Equal(1, log.TruncatedEntries);
            Assert.Equal("old", log.Get("k")?.Value);
            Assert.Equal("other", log.Get("other")?.Value);
        }

        using var reopened = Open();
        Assert.Equal(new Dictionary<string, string> { ["k"] = "old", ["other"] = "other" }, reopened.Snapshot());
    }

    [Fact]
    public void AppendFrom_DropsEverythingPastTheLeadersLastEntry()
    {
        using var log = Open();
        log.Append(1, "a", "1");
        log.Append(1, "stale", "2");
        log.Append(1, "stale2", "3");

        var outcome = log.AppendFrom(LeaderEpoch, 1, 1, [], leaderLastIndex: 1);

        Assert.Equal(new AppendOutcome(AppendStatus.Ok, 1), outcome);
        Assert.Equal(1, log.LastIndex);
        Assert.Equal(2, log.TruncatedEntries);
        Assert.Null(log.Get("stale"));
    }

    [Fact]
    public void AppendFrom_LateRequestFromTheSameEpoch_DropsNothing()
    {
        using var log = Open();
        log.AppendFrom(3, 0, 0, [Entry(1, 3, "a"), Entry(2, 3, "b"), Entry(3, 3, "c")], leaderLastIndex: 3);

        // A heartbeat the primary sent when its log ended at 1, arriving after the batch above.
        var outcome = log.AppendFrom(3, 1, 3, [], leaderLastIndex: 1);

        Assert.Equal(new AppendOutcome(AppendStatus.Ok, 1), outcome);
        Assert.Equal(3, log.LastIndex);
        Assert.Equal(0, log.TruncatedEntries);
    }

    [Fact]
    public void AppendFrom_KeepsEntriesPastAPartialBatch()
    {
        using var log = Open();
        log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 1, "a"), Entry(2, 1, "b"), Entry(3, 1, "c")], leaderLastIndex: 3);

        var outcome = log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 1, "a")], leaderLastIndex: 3);

        Assert.Equal(new AppendOutcome(AppendStatus.Ok, 1), outcome);
        Assert.Equal(3, log.LastIndex);
    }

    [Fact]
    public void AppendFrom_RejectsBatchesThatDoNotFitTogether()
    {
        using var log = Open();

        Assert.Throws<InvalidInputException>(() => log.AppendFrom(LeaderEpoch, 0, 0, [Entry(2, 1, "a")], leaderLastIndex: 2));
        Assert.Throws<InvalidInputException>(() => log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 0, "a")], leaderLastIndex: 1));
        Assert.Throws<InvalidInputException>(() => log.AppendFrom(LeaderEpoch, 0, 0, [Entry(1, 1, "a")], leaderLastIndex: 0));
        Assert.Throws<InvalidInputException>(() => log.AppendFrom(LeaderEpoch, -1, 0, [], leaderLastIndex: 0));
        Assert.Throws<InvalidInputException>(() => log.AppendFrom(1, 0, 0, [Entry(1, 2, "a")], leaderLastIndex: 1));
    }

    [Fact]
    public void Open_DropsAPartialLastLine()
    {
        var path = _dir.File("log.jsonl");
        Directory.CreateDirectory(_dir.Path);
        File.WriteAllText(path, "{\"index\":1,\"epoch\":1,\"key\":\"a\",\"value\":\"1\"}\n{\"index\":2,\"ep");

        using (var log = ReplicatedLog.Open(path))
        {
            Assert.Equal(1, log.LastIndex);
            log.Append(1, "b", "2");
        }

        using var reopened = ReplicatedLog.Open(path);
        Assert.Equal(2, reopened.LastIndex);
    }

    [Fact]
    public void Open_CompletesALastLineThatLostOnlyItsNewline()
    {
        var path = _dir.File("log.jsonl");
        Directory.CreateDirectory(_dir.Path);
        File.WriteAllText(path, "{\"index\":1,\"epoch\":1,\"key\":\"a\",\"value\":\"1\"}");

        using (var log = ReplicatedLog.Open(path))
        {
            Assert.Equal(1, log.LastIndex);
            log.Append(1, "b", "2");
        }

        using var reopened = ReplicatedLog.Open(path);
        Assert.Equal(2, reopened.LastIndex);
    }

    [Fact]
    public void Open_DropsATornMultiByteCharacter()
    {
        var path = _dir.File("log.jsonl");
        Directory.CreateDirectory(_dir.Path);
        var full = System.Text.Encoding.UTF8.GetBytes("{\"index\":1,\"epoch\":1,\"key\":\"a\",\"value\":\"1\"}\n{\"index\":2,\"epoch\":1,\"key\":\"b\",\"value\":\"中");
        File.WriteAllBytes(path, full[..^1]); // killed in the middle of a 3-byte character

        using var log = ReplicatedLog.Open(path);

        Assert.Equal(1, log.LastIndex);
    }

    [Fact]
    public void Open_RefusesCorruptionBeforeTheEnd()
    {
        var path = _dir.File("log.jsonl");
        Directory.CreateDirectory(_dir.Path);
        File.WriteAllText(path, "garbage\n{\"index\":1,\"epoch\":1,\"key\":\"a\",\"value\":\"1\"}\n");

        Assert.Throws<InvalidDataException>(() => ReplicatedLog.Open(path));
    }

    private ReplicatedLog Open() => ReplicatedLog.Open(_dir.File("log.jsonl"));

    private static LogEntry Entry(long index, long epoch, string key) => new(index, epoch, key, key);
}
