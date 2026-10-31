using Shared;

namespace Worker.Dispatch;

/// <summary>
/// This worker's own view of its task lease. It starts at the send time of the request that won or renewed it and
/// ends a margin early, so the worker gives up before the upstream hands the task to someone else.
/// </summary>
public sealed class LeaseTracker(MonotonicClock clock, TimeSpan until)
{
    private readonly object _gate = new();
    private TimeSpan _until = until;
    private bool _revoked;

    public bool IsValid
    {
        get
        {
            lock (_gate)
            {
                return !_revoked && clock.Now < _until;
            }
        }
    }

    /// <summary>Time left on the lease; zero or less once it is gone.</summary>
    public TimeSpan Remaining
    {
        get
        {
            lock (_gate)
            {
                return _revoked ? TimeSpan.Zero : _until - clock.Now;
            }
        }
    }

    public void ExtendTo(TimeSpan until)
    {
        lock (_gate)
        {
            if (until > _until)
            {
                _until = until;
            }
        }
    }

    /// <summary>The upstream said another attempt owns the task now.</summary>
    public void Revoke()
    {
        lock (_gate)
        {
            _revoked = true;
        }
    }

    /// <summary>End of a lease that started at <paramref name="sentAt"/>, minus a tenth for clock drift.</summary>
    public static TimeSpan End(TimeSpan sentAt, int ttlMs) => sentAt + TimeSpan.FromMilliseconds(ttlMs * 0.9);
}
