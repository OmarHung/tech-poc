using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shared;

namespace Upstream.Tests;

public sealed class TaskQueueTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(2);

    private readonly FakeTimeProvider _time = new();
    private readonly TaskQueue _queue;

    public TaskQueueTests()
    {
        _queue = new TaskQueue(new UpstreamConfig { TaskLeaseTtl = Ttl }, _time, NullLogger<TaskQueue>.Instance);
    }

    [Fact]
    public async Task Lease_HandsOutTasksInOrder()
    {
        _queue.Add(10);
        _queue.Add(20);

        var first = await Lease("a");
        var second = await Lease("b");

        Assert.Equal(new TaskLease(1, 10, 1, 2000), first);
        Assert.Equal(new TaskLease(2, 20, 1, 2000), second);
        Assert.Null(await Lease("a"));
    }

    [Fact]
    public async Task Lease_WaitsForANewTask()
    {
        var waiting = _queue.LeaseAsync("a", TimeSpan.FromSeconds(1), CancellationToken.None);
        Assert.False(waiting.IsCompleted);

        _queue.Add(7);

        Assert.Equal(7, (await waiting)?.Input);
    }

    [Fact]
    public async Task Lease_GivesUpAfterTheWait()
    {
        var waiting = _queue.LeaseAsync("a", TimeSpan.FromSeconds(1), CancellationToken.None);

        _time.Advance(TimeSpan.FromSeconds(1));

        Assert.Null(await waiting);
    }

    [Fact]
    public async Task Heartbeats_KeepTheLease()
    {
        _queue.Add(1);
        var lease = (await Lease("a"))!;

        _time.Advance(Ttl * 0.75);
        Assert.True(_queue.Heartbeat(lease.Id, new TaskAttempt("a", 1)));
        _time.Advance(Ttl * 0.75);

        Assert.True(_queue.Complete(lease.Id, new TaskAttempt("a", 1)));
        Assert.Equal(1, _queue.Stats().Done);
    }

    [Fact]
    public async Task ExpiredLease_GoesBackToTheFront_WithTheNextAttempt()
    {
        _queue.Add(1);
        _queue.Add(2);
        await Lease("a");

        _time.Advance(Ttl);
        var retried = await Lease("b");

        Assert.Equal(new TaskLease(1, 1, 2, 2000), retried);
        Assert.Equal(1, _queue.Stats().Reassignments);
    }

    [Fact]
    public async Task LostLease_CannotHeartbeatOrComplete()
    {
        _queue.Add(1);
        await Lease("a");
        _time.Advance(Ttl);
        await Lease("b");

        Assert.False(_queue.Heartbeat(1, new TaskAttempt("a", 1)));
        Assert.False(_queue.Complete(1, new TaskAttempt("a", 1)));
        Assert.True(_queue.Complete(1, new TaskAttempt("b", 2)));
        Assert.False(_queue.Complete(1, new TaskAttempt("a", 1)));

        var stats = _queue.Stats();
        Assert.Equal(2, stats.StaleCompletions);
        Assert.Equal(1, stats.Done);
    }

    [Fact]
    public async Task ExpireOverdue_RequeuesWithoutAnyoneAsking()
    {
        _queue.Add(1);
        await Lease("a");

        _time.Advance(Ttl);
        _queue.ExpireOverdue();

        var stats = _queue.Stats();
        Assert.Equal(1, stats.Queued);
        Assert.Empty(stats.Leases);
    }

    [Fact]
    public async Task RepeatedCompletion_ByTheSameAttempt_IsNotStale()
    {
        _queue.Add(1);
        await Lease("a");

        Assert.True(_queue.Complete(1, new TaskAttempt("a", 1)));
        Assert.True(_queue.Complete(1, new TaskAttempt("a", 1))); // a retry whose first reply was lost

        Assert.Equal(0, _queue.Stats().StaleCompletions);
    }

    [Fact]
    public void Add_RejectsInputsWhoseSquareWouldOverflow()
    {
        Assert.Throws<InvalidInputException>(() => _queue.Add(TaskQueue.MaxInput + 1));
    }

    [Fact]
    public void UnknownTasksAndBadWorkers_AreRejected()
    {
        Assert.Throws<NotFoundException>(() => _queue.Complete(99, new TaskAttempt("a", 1)));
        Assert.Throws<InvalidInputException>(() => _queue.Heartbeat(1, new TaskAttempt("Bad Worker", 1)));
    }

    private Task<TaskLease?> Lease(string worker) => _queue.LeaseAsync(worker, TimeSpan.Zero, CancellationToken.None);
}
