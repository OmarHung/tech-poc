using Shared;

namespace Upstream;

public sealed record UpstreamConfig
{
    public int Port { get; init; } = 8120;

    /// <summary>How long a worker holds a task without a heartbeat before it is handed to someone else.</summary>
    public TimeSpan TaskLeaseTtl { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>How often expired task leases are looked for.</summary>
    public TimeSpan ReapInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    public static UpstreamConfig FromEnvironment() => FromLookup(Environment.GetEnvironmentVariable);

    public static UpstreamConfig FromLookup(Func<string, string?> lookup)
    {
        var env = new EnvReader(lookup);
        var d = new UpstreamConfig();
        return new UpstreamConfig
        {
            Port = env.Port("PORT", d.Port),
            TaskLeaseTtl = env.Millis("TASK_LEASE_TTL_MS", d.TaskLeaseTtl, 100, 60_000),
        };
    }
}
