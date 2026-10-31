using Microsoft.Extensions.Time.Testing;
using Shared;
using Worker.Dispatch;

namespace Worker.Tests;

public sealed class LeaseTrackerTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly MonotonicClock _clock;

    public LeaseTrackerTests() => _clock = new MonotonicClock(_time);

    [Fact]
    public void End_IsNinetyPercentOfTheTtlAfterTheSend()
    {
        Assert.Equal(TimeSpan.FromMilliseconds(1_900), LeaseTracker.End(TimeSpan.FromSeconds(1), 1_000));
    }

    [Fact]
    public void Lease_RunsOut_UnlessExtended()
    {
        var tracker = new LeaseTracker(_clock, LeaseTracker.End(_clock.Now, 1_000));

        _time.Advance(TimeSpan.FromMilliseconds(800));
        Assert.True(tracker.IsValid);
        tracker.ExtendTo(LeaseTracker.End(_clock.Now, 1_000));
        tracker.ExtendTo(TimeSpan.Zero); // an older renewal never shortens it
        _time.Advance(TimeSpan.FromMilliseconds(800));
        Assert.True(tracker.IsValid);
        _time.Advance(TimeSpan.FromMilliseconds(100));

        Assert.False(tracker.IsValid);
    }

    [Fact]
    public void Remaining_CountsDownToTheEnd()
    {
        var tracker = new LeaseTracker(_clock, LeaseTracker.End(_clock.Now, 1_000));

        _time.Advance(TimeSpan.FromMilliseconds(600));

        Assert.Equal(TimeSpan.FromMilliseconds(300), tracker.Remaining);
    }

    [Fact]
    public void Revoke_EndsTheLeaseAtOnce()
    {
        var tracker = new LeaseTracker(_clock, LeaseTracker.End(_clock.Now, 1_000));

        tracker.Revoke();

        Assert.False(tracker.IsValid);
        Assert.Equal(TimeSpan.Zero, tracker.Remaining);
    }

    [Fact]
    public void TaskResult_IsDeterministic()
    {
        Assert.Equal("12^2=144", DispatchWorker.Compute(12));
    }
}
