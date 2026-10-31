using Shared;

namespace Worker.Dispatch;

/// <summary>
/// Takes one task at a time from the upstream queue, keeps the lease alive with heartbeats while working, writes the
/// result to the sink, then reports the task done. Writing before reporting makes delivery at-least-once: a crash in
/// between means another worker does the task again, and the sink sees a second copy.
/// </summary>
public sealed class DispatchWorker(
    WorkClient client,
    WorkerStatus status,
    WorkerConfig config,
    TimeProvider time,
    ILogger<DispatchWorker> logger) : BackgroundService
{
    private readonly MonotonicClock _clock = new(time);
    private bool _sinkDown;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Dispatched worker {Worker} starting; lease check {LeaseCheck}", config.WorkerId, config.LeaseCheck);
        while (!stoppingToken.IsCancellationRequested)
        {
            if (_sinkDown && !await SinkBackAsync(stoppingToken))
            {
                continue;
            }
            var sentAt = _clock.Now;
            var (reached, lease) = await client.LeaseTaskAsync(stoppingToken);
            if (lease is not null)
            {
                await RunAsync(lease, new LeaseTracker(_clock, LeaseTracker.End(sentAt, lease.TtlMs)), stoppingToken);
            }
            else if (!reached)
            {
                await Task.Delay(config.RetryDelay, time, stoppingToken); // refused or reset: do not spin
            }
        }
    }

    private async Task RunAsync(TaskLease lease, LeaseTracker tracker, CancellationToken ct)
    {
        status.TaskStarted(lease.Id);
        using var heartbeats = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var heartbeating = HeartbeatAsync(lease, tracker, heartbeats.Token);
        try
        {
            await Task.Delay(config.WorkTime, time, ct); // the work itself
            if (!StillHolds(lease, tracker, "before writing the result"))
            {
                return;
            }
            status.TaskPhase(lease.Id, "delivering");
            var result = new ResultRecord($"task-{lease.Id}", config.WorkerId, lease.Attempt, Compute(lease.Input));
            if (!await DeliverAsync(lease, tracker, result, ct))
            {
                return;
            }
            status.TaskPhase(lease.Id, "completing");
            var completed = await CompleteAsync(lease, tracker, ct);
            status.TaskFinished(completed ? "completed" : "stale");
        }
        finally
        {
            await heartbeats.CancelAsync();
            try
            {
                await heartbeating;
            }
            catch (OperationCanceledException)
            {
                // Stopped with the task.
            }
        }
    }

    internal static string Compute(long input) => $"{input}^2={input * input}";

    private bool StillHolds(TaskLease lease, LeaseTracker tracker, string when) =>
        !config.LeaseCheck || tracker.IsValid || GiveUp(lease, when);

    /// <summary>Records the task as abandoned. Always false, so callers can return it.</summary>
    private bool GiveUp(TaskLease lease, string when)
    {
        logger.LogWarning("Gave up task {Id} attempt {Attempt} {When}: lease lost", lease.Id, lease.Attempt, when);
        status.TaskFinished("abandoned");
        return false;
    }

    /// <summary>
    /// Retries until the sink answers. With the lease check on, each try is cut off when the lease runs out, so a write
    /// cannot start late; only one already on the wire can still land after it. A worker that cannot reach the sink
    /// for a whole lease gives the task up (heartbeats stop with it), so the other worker can get it.
    /// </summary>
    private async Task<bool> DeliverAsync(TaskLease lease, LeaseTracker tracker, ResultRecord result, CancellationToken ct)
    {
        var giveUpAt = _clock.Now + TimeSpan.FromMilliseconds(lease.TtlMs);
        while (true)
        {
            var timeout = config.LeaseCheck ? Min(config.RpcTimeout, tracker.Remaining) : config.RpcTimeout;
            if (timeout <= TimeSpan.Zero)
            {
                // Out of lease and nothing written. Do not look again: a heartbeat landing right now could make the
                // lease valid, and returning true would report the task done without a result.
                return GiveUp(lease, "while writing the result");
            }
            var reply = await client.DeliverAsync(result, ct, timeout);
            if (reply is not null)
            {
                status.SinkReplied(reply);
                return true;
            }
            if (_clock.Now >= giveUpAt)
            {
                logger.LogWarning("Gave up task {Id} attempt {Attempt}: sink unreachable for a whole lease", lease.Id, lease.Attempt);
                status.TaskFinished("abandoned");
                _sinkDown = true; // take no more work until it is back, or the next task (maybe this one again) stalls too
                return false;
            }
            await Task.Delay(config.RetryDelay, time, ct);
            if (!StillHolds(lease, tracker, "while the sink was unreachable"))
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Circuit breaker after giving a task up for an unreachable sink: lease nothing until the sink answers again.
    /// Otherwise this worker keeps taking tasks it cannot finish, and may even get back the one it just let go.
    /// </summary>
    private async Task<bool> SinkBackAsync(CancellationToken ct)
    {
        if (await client.SinkReachableAsync(ct))
        {
            logger.LogInformation("Sink reachable again; taking tasks");
            _sinkDown = false;
            return true;
        }
        await Task.Delay(config.RetryDelay, time, ct);
        return false;
    }

    /// <summary>Keeps reporting until the upstream answers. False means another attempt owns the task (it will run again).</summary>
    private async Task<bool> CompleteAsync(TaskLease lease, LeaseTracker tracker, CancellationToken ct)
    {
        bool? completed;
        while ((completed = await client.CompleteAsync(lease, ct)) is null)
        {
            await Task.Delay(config.RetryDelay, time, ct);
        }
        if (completed == false)
        {
            logger.LogWarning("Task {Id} attempt {Attempt} was reassigned before it was reported done", lease.Id, lease.Attempt);
            tracker.Revoke();
        }
        return completed.Value;
    }

    /// <summary>
    /// Heartbeats on a fixed schedule, each bounded by the interval, so one hung call cannot push the next one past
    /// the lease. The lease is extended from each heartbeat's send time.
    /// </summary>
    private async Task HeartbeatAsync(TaskLease lease, LeaseTracker tracker, CancellationToken ct)
    {
        var interval = TimeSpan.FromMilliseconds(lease.TtlMs / 3.0);
        using var timer = new PeriodicTimer(interval, time);
        while (await timer.WaitForNextTickAsync(ct))
        {
            var sentAt = _clock.Now;
            switch (await client.HeartbeatAsync(lease, interval, ct))
            {
                case true:
                    tracker.ExtendTo(LeaseTracker.End(sentAt, lease.TtlMs));
                    break;
                case false:
                    tracker.Revoke();
                    return;
            }
        }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}
