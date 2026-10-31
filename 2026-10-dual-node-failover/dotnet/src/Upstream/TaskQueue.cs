using Shared;

namespace Upstream;

/// <summary>
/// The dispatcher for worker-style redundancy. Every task is leased to one worker at a time; a worker that stops
/// heartbeating loses the lease and the task goes back to the front of the queue with a higher attempt number.
/// Workers never talk to each other: the queue is the only thing that knows who does what.
/// </summary>
/// <remarks>Delivery is at-least-once: a worker that did the work but died before reporting it, or that was frozen past its lease, means the task runs twice.</remarks>
public sealed class TaskQueue
{
    /// <summary>Inputs are squared by the workers; this keeps the square inside a long.</summary>
    public const long MaxInput = 3_000_000_000;

    private readonly object _gate = new();
    private readonly MonotonicClock _clock;
    private readonly TimeSpan _ttl;
    private readonly ILogger<TaskQueue> _logger;
    private readonly Dictionary<long, Item> _tasks = [];
    private readonly LinkedList<long> _pending = new();
    private TaskCompletionSource _available = NewSignal();
    private long _reassignments;
    private long _staleCompletions;

    public TaskQueue(UpstreamConfig config, TimeProvider time, ILogger<TaskQueue> logger)
    {
        _clock = new MonotonicClock(time);
        _ttl = config.TaskLeaseTtl;
        _logger = logger;
    }

    public long Add(long input)
    {
        if (input is < -MaxInput or > MaxInput)
        {
            throw new InvalidInputException($"input must be between -{MaxInput} and {MaxInput}");
        }
        lock (_gate)
        {
            var id = _tasks.Count + 1L;
            _tasks[id] = new Item(id, input);
            _pending.AddLast(id);
            PulseLocked();
            return id;
        }
    }

    /// <summary>Hands the oldest waiting task to <paramref name="worker"/>, waiting up to <paramref name="wait"/> for one.</summary>
    public async Task<TaskLease?> LeaseAsync(string? worker, TimeSpan wait, CancellationToken ct)
    {
        RequireWorker(worker);
        var deadline = _clock.Now + wait;
        while (true)
        {
            Task available;
            lock (_gate)
            {
                ExpireLocked();
                if (_pending.First is { } first)
                {
                    _pending.RemoveFirst();
                    var item = _tasks[first.Value];
                    item.Attempt++;
                    item.Worker = worker;
                    item.LeaseUntil = _clock.Now + _ttl;
                    return new TaskLease(item.Id, item.Input, item.Attempt, (int)_ttl.TotalMilliseconds);
                }
                available = _available.Task;
            }
            var remaining = deadline - _clock.Now;
            if (remaining <= TimeSpan.Zero)
            {
                return null;
            }
            try
            {
                await available.WaitAsync(remaining, _clock.Time, ct);
            }
            catch (TimeoutException)
            {
                return null;
            }
        }
    }

    /// <summary>Extends the lease if <paramref name="request"/> still holds it.</summary>
    public bool Heartbeat(long id, TaskAttempt request)
    {
        RequireWorker(request.Worker);
        lock (_gate)
        {
            ExpireLocked();
            if (!Holds(id, request, out var item))
            {
                return false;
            }
            item.LeaseUntil = _clock.Now + _ttl;
            return true;
        }
    }

    /// <summary>Marks the task done if <paramref name="request"/> still holds it; anything else is a stale completion.</summary>
    public bool Complete(long id, TaskAttempt request)
    {
        RequireWorker(request.Worker);
        lock (_gate)
        {
            ExpireLocked();
            if (_tasks.TryGetValue(id, out var done) && done.Done && done.Worker == request.Worker && done.Attempt == request.Attempt)
            {
                return true; // the same report again, e.g. a retry after its reply was lost
            }
            if (!Holds(id, request, out var item))
            {
                _staleCompletions++;
                _logger.LogWarning(
                    "Stale completion of task {Id} attempt {Attempt} by {Worker}", id, request.Attempt, request.Worker);
                return false;
            }
            item.Done = true;
            item.LeaseUntil = TimeSpan.Zero;
            return true;
        }
    }

    /// <summary>Puts every task whose lease ran out back at the front of the queue. Called on every access and by a timer.</summary>
    public void ExpireOverdue()
    {
        lock (_gate)
        {
            ExpireLocked();
        }
    }

    public TaskStats Stats()
    {
        lock (_gate)
        {
            var leased = _tasks.Values.Where(t => t is { Done: false, Worker: not null } && t.LeaseUntil > TimeSpan.Zero).ToList();
            return new TaskStats(
                _tasks.Count,
                _pending.Count,
                leased.Count,
                _tasks.Values.Count(t => t.Done),
                _reassignments,
                _staleCompletions,
                leased.Select(t => new LeaseView(t.Id, t.Worker!, t.Attempt)).ToList());
        }
    }

    private bool Holds(long id, TaskAttempt request, out Item item)
    {
        if (!_tasks.TryGetValue(id, out item!))
        {
            throw new NotFoundException($"task {id} not found");
        }
        return !item.Done && item.Worker == request.Worker && item.Attempt == request.Attempt && item.LeaseUntil > TimeSpan.Zero;
    }

    private void ExpireLocked()
    {
        var now = _clock.Now;
        var expired = _tasks.Values
            .Where(t => !t.Done && t.LeaseUntil > TimeSpan.Zero && t.LeaseUntil <= now)
            .OrderByDescending(t => t.Id)
            .ToList();
        foreach (var item in expired)
        {
            _logger.LogWarning("Lease on task {Id} attempt {Attempt} by {Worker} expired; requeued", item.Id, item.Attempt, item.Worker);
            item.LeaseUntil = TimeSpan.Zero;
            item.Worker = null;
            _pending.AddFirst(item.Id); // retried work goes first: it has waited longest
            _reassignments++;
        }
        if (expired.Count > 0)
        {
            PulseLocked();
        }
    }

    private void PulseLocked()
    {
        var fired = _available;
        _available = NewSignal();
        fired.TrySetResult();
    }

    private static void RequireWorker(string? worker)
    {
        if (!NodeIds.IsValid(worker))
        {
            throw new InvalidInputException("worker must be 1-32 lowercase letters, digits or '-'");
        }
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Item(long id, long input)
    {
        public long Id { get; } = id;
        public long Input { get; } = input;
        public int Attempt { get; set; }
        public string? Worker { get; set; }

        /// <summary>Zero when nobody holds the task.</summary>
        public TimeSpan LeaseUntil { get; set; }

        public bool Done { get; set; }
    }
}

public sealed record LeaseView(long Id, string Worker, int Attempt);

public sealed record TaskStats(
    long Total,
    long Queued,
    long Leased,
    long Done,
    long Reassignments,
    long StaleCompletions,
    IReadOnlyList<LeaseView> Leases);
