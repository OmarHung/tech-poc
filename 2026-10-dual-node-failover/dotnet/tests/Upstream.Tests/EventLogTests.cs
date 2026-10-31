using Microsoft.Extensions.Time.Testing;
using Shared;

namespace Upstream.Tests;

public sealed class EventLogTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(1);

    private readonly EventLog _log = new();
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void Append_NumbersEventsFromOne()
    {
        Assert.Equal(new LoggedEvent(1, "acct-1", 10), _log.Append(new NewEvent("acct-1", 10)));
        Assert.Equal(new LoggedEvent(2, "acct-2", -5), _log.Append(new NewEvent("acct-2", -5)));
        Assert.Equal(2, _log.Last);
    }

    [Fact]
    public async Task Read_ReturnsEventsAfterThePosition()
    {
        for (var i = 1; i <= 5; i++)
        {
            _log.Append(new NewEvent("acct", i));
        }

        var page = await _log.ReadAsync(after: 2, limit: 2, TimeSpan.Zero, _time, CancellationToken.None);

        Assert.Equal([3L, 4L], page.Events.Select(e => e.Seq));
        Assert.Equal(5, page.Last);
    }

    [Fact]
    public async Task Read_WaitsForTheNextEvent()
    {
        var reading = _log.ReadAsync(after: 0, limit: 10, Wait, _time, CancellationToken.None);
        Assert.False(reading.IsCompleted);

        _log.Append(new NewEvent("acct", 1));

        var page = await reading;
        Assert.Single(page.Events);
    }

    [Fact]
    public async Task Read_ReturnsAnEmptyPage_WhenNothingArrives()
    {
        var reading = _log.ReadAsync(after: 0, limit: 10, Wait, _time, CancellationToken.None);

        _time.Advance(Wait);

        var page = await reading;
        Assert.Empty(page.Events);
        Assert.Equal(0, page.Last);
    }

    [Theory]
    [InlineData("", 1)]
    [InlineData("bad account", 1)]
    [InlineData("acct", EventLog.MaxAmount + 1)]
    [InlineData("acct", long.MinValue)]
    public void Append_RejectsBadEvents(string account, long amount)
    {
        Assert.Throws<InvalidInputException>(() => _log.Append(new NewEvent(account, amount)));
    }

    [Fact]
    public async Task Read_RejectsANegativePosition()
    {
        await Assert.ThrowsAsync<InvalidInputException>(() => _log.ReadAsync(-1, 10, TimeSpan.Zero, _time, CancellationToken.None));
    }
}
