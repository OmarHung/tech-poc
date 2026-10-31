using Shared;

namespace Worker.Tests;

/// <summary>Both architectures end to end in one process. verify/independent.py covers the same with real processes.</summary>
public sealed class WorkTests
{
    [Fact]
    public async Task HotHot_BothWorkersProcessEveryEvent_AndLosingOneLosesNothing()
    {
        await using var work = await InProcessWork.StartAsync(WorkerMode.HotHot);
        var expected = new Dictionary<string, long>();
        Append(work, expected, 1, 30);
        await InProcessWork.WaitUntilAsync(() => work.Results.Snapshot().Summary.Received == 60, "both workers to deliver");

        await work.KillAsync("a");
        Append(work, expected, 31, 60);
        await InProcessWork.WaitUntilAsync(() => work.Results.Snapshot().Summary.Applied == 60, "b to deliver the rest");

        var summary = work.Results.Snapshot().Summary;
        Assert.Equal(0, summary.Conflicts);
        Assert.Equal(expected, work.Status("b").Ledger.ToFile().Balances);
    }

    [Fact]
    public async Task Dispatch_ATaskHeldByADeadWorker_IsDoneByTheOther()
    {
        await using var work = await InProcessWork.StartAsync(WorkerMode.Dispatch);
        work.Tasks.Add(7);
        await InProcessWork.WaitUntilAsync(() => work.Tasks.Stats().Leases.Count == 1, "a worker to take the task");
        var holder = work.Tasks.Stats().Leases[0].Worker;

        await work.KillAsync(holder);
        await InProcessWork.WaitUntilAsync(() => work.Tasks.Stats().Done == 1, "the task to be redone");

        var stats = work.Tasks.Stats();
        var result = work.Results.Snapshot().Results["task-1"];
        Assert.Equal(1, stats.Reassignments);
        Assert.NotEqual(holder, result.Node);
        Assert.Equal(("7^2=49", 2), (result.Value, result.Attempt));
    }

    [Fact]
    public async Task Dispatch_ATaskLongerThanTheLease_IsKeptAliveByHeartbeats()
    {
        await using var work = await InProcessWork.StartAsync(WorkerMode.Dispatch, workMs: 1_000); // lease is 400 ms
        work.Tasks.Add(3);

        await InProcessWork.WaitUntilAsync(() => work.Tasks.Stats().Done == 1, "the long task");

        Assert.Equal(0, work.Tasks.Stats().Reassignments);
        Assert.Equal(1, work.Results.Snapshot().Summary.Received);
    }

    [Fact]
    public async Task Dispatch_AWorkerThatCannotReachTheSink_LetsTheTaskGo()
    {
        await using var work = await InProcessWork.StartAsync(WorkerMode.Dispatch);
        work.Tasks.Add(5);
        await InProcessWork.WaitUntilAsync(() => work.Tasks.Stats().Leases.Count == 1, "a worker to take the task");
        var holder = work.Tasks.Stats().Leases[0].Worker;

        work.Cut(holder, "sink");
        await InProcessWork.WaitUntilAsync(() => work.Tasks.Stats().Done == 1, "the other worker to finish it");

        Assert.NotEqual(holder, work.Results.Snapshot().Results["task-1"].Node);
        Assert.True(work.Tasks.Stats().Reassignments >= 1);
    }

    [Fact]
    public async Task Dispatch_SpreadsTasksOverBothWorkers()
    {
        await using var work = await InProcessWork.StartAsync(WorkerMode.Dispatch);
        for (var i = 1; i <= 6; i++)
        {
            work.Tasks.Add(i);
        }

        await InProcessWork.WaitUntilAsync(() => work.Tasks.Stats().Done == 6, "all tasks");

        var nodes = work.Results.Snapshot().Results.Values.Select(r => r.Node).Distinct().Order();
        Assert.Equal(["a", "b"], nodes);
        Assert.Equal(6, work.Results.Snapshot().Summary.Received);
    }

    private static void Append(InProcessWork work, Dictionary<string, long> expected, int from, int to)
    {
        for (var i = from; i <= to; i++)
        {
            var account = $"acct-{i % 3}";
            work.Events.Append(new NewEvent(account, i));
            expected[account] = expected.GetValueOrDefault(account) + i;
        }
    }
}
