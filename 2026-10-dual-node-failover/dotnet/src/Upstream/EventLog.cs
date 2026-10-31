using Shared;

namespace Upstream;

/// <summary>
/// The feed for hot-hot redundancy: an append-only, in-memory log of events. Readers keep their own position and can
/// re-read from any point, which is what lets a restarted worker catch up without asking the other worker anything.
/// </summary>
public sealed class EventLog
{
    public const int MaxPage = 500;
    public const long MaxAmount = 1_000_000_000;

    private readonly object _gate = new();
    private readonly List<LoggedEvent> _events = [];
    private TaskCompletionSource _appended = NewSignal();

    public long Last
    {
        get
        {
            lock (_gate)
            {
                return _events.Count;
            }
        }
    }

    public LoggedEvent Append(NewEvent request)
    {
        if (!WorkIds.IsValid(request.Account))
        {
            throw new InvalidInputException($"account must be 1-{WorkIds.MaxLength} ASCII letters, digits, '-', '_' or '.'");
        }
        if (request.Amount is < -MaxAmount or > MaxAmount)
        {
            throw new InvalidInputException($"amount must be between -{MaxAmount} and {MaxAmount}");
        }
        lock (_gate)
        {
            var logged = new LoggedEvent(_events.Count + 1, request.Account!, request.Amount);
            _events.Add(logged);
            var fired = _appended;
            _appended = NewSignal();
            fired.TrySetResult();
            return logged;
        }
    }

    /// <summary>Events after <paramref name="after"/>; waits up to <paramref name="wait"/> for one if there are none yet.</summary>
    public async Task<EventPage> ReadAsync(long after, int limit, TimeSpan wait, TimeProvider time, CancellationToken ct)
    {
        if (after < 0 || limit < 1)
        {
            throw new InvalidInputException("after must be >= 0 and limit >= 1");
        }
        Task appended;
        lock (_gate)
        {
            if (after < _events.Count || wait <= TimeSpan.Zero)
            {
                return PageLocked(after, limit);
            }
            appended = _appended.Task;
        }
        try
        {
            await appended.WaitAsync(wait, time, ct);
        }
        catch (TimeoutException)
        {
            // Nothing new; answer with an empty page.
        }
        lock (_gate)
        {
            return PageLocked(after, limit);
        }
    }

    private EventPage PageLocked(long after, int limit)
    {
        var start = (int)Math.Min(after, _events.Count);
        var count = Math.Min(Math.Min(limit, MaxPage), _events.Count - start);
        return new EventPage(_events.GetRange(start, count), _events.Count);
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
