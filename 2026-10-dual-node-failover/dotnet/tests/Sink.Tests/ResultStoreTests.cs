using Microsoft.Extensions.Time.Testing;
using Shared;

namespace Sink.Tests;

public sealed class ResultStoreTests
{
    private readonly ResultStore _store = new(new FakeTimeProvider(DateTimeOffset.FromUnixTimeMilliseconds(1_000)));

    [Fact]
    public void FirstCopy_IsApplied_TheSameValueAgainIsADuplicate()
    {
        Assert.Equal(ResultStatus.Applied, _store.Record(new ResultRecord("event-1", "a", 0, "acct:5")));
        Assert.Equal(ResultStatus.Duplicate, _store.Record(new ResultRecord("event-1", "b", 0, "acct:5")));

        var snapshot = _store.Snapshot();
        Assert.Equal(new SinkSummary(2, 1, 1, 0, 0), snapshot.Summary);
        Assert.Equal(new ResultView("acct:5", "a", 0, 1_000, 2, 0, 0), snapshot.Results["event-1"]);
    }

    [Fact]
    public void ADifferentValue_IsAConflict_AndTheFirstStays()
    {
        _store.Record(new ResultRecord("event-1", "a", 0, "acct:5"));

        Assert.Equal(ResultStatus.Conflict, _store.Record(new ResultRecord("event-1", "b", 0, "acct:7")));

        var snapshot = _store.Snapshot();
        Assert.Equal("acct:5", snapshot.Results["event-1"].Value);
        Assert.Equal(1, snapshot.Summary.Conflicts);
    }

    [Fact]
    public void AnOlderAttempt_AfterANewerOne_IsStale()
    {
        _store.Record(new ResultRecord("task-1", "b", 2, "1^2=1"));

        Assert.Equal(ResultStatus.Stale, _store.Record(new ResultRecord("task-1", "a", 1, "1^2=1")));
        Assert.Equal(ResultStatus.Duplicate, _store.Record(new ResultRecord("task-1", "a", 3, "1^2=1")));
        Assert.Equal(1, _store.Snapshot().Summary.Stale);
    }

    [Theory]
    [InlineData(null, "a", 0, "v")]
    [InlineData("k", "A", 0, "v")]
    [InlineData("k", "a", -1, "v")]
    [InlineData("k", "a", 0, null)]
    public void BadRecords_AreRejected(string? key, string? node, int attempt, string? value)
    {
        Assert.Throws<InvalidInputException>(() => _store.Record(new ResultRecord(key, node, attempt, value)));
    }
}
