namespace Shared;

/// <summary>
/// Time elapsed since this clock was created, read from <see cref="TimeProvider"/>'s monotonic timestamp.
/// Leases are measured with it so a wall-clock jump cannot stretch or shrink them.
/// </summary>
public sealed class MonotonicClock(TimeProvider time)
{
    private readonly long _origin = time.GetTimestamp();

    public TimeProvider Time => time;

    public TimeSpan Now => time.GetElapsedTime(_origin);
}
