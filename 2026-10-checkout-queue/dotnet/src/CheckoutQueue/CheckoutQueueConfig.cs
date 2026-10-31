using System.Globalization;
using System.Text.RegularExpressions;

namespace CheckoutQueue;

/// <summary>
/// Settings for one process. Every instance pointed at the same Redis and SALE_ID serves the
/// same sale, so scaling out is just starting more processes.
/// </summary>
public sealed partial record CheckoutQueueConfig
{
    private static readonly TimeSpan MaxDuration = TimeSpan.FromHours(24);

    public int Port { get; init; } = 8090;
    public string RedisUrl { get; init; } = "127.0.0.1:6379";
    public string InstanceName { get; init; } = "checkout-queue";
    public string SaleId { get; init; } = "demo";
    public int InitialStock { get; init; } = 100;
    public int MaxQuantityPerOrder { get; init; } = 2;

    /// <summary>How many admitted users may be in checkout at once. This is the knob that flattens the spike.</summary>
    public int MaxActiveSessions { get; init; } = 50;

    /// <summary>How long an admitted user has to place the order before the slot goes to the next in line.</summary>
    public TimeSpan AdmissionWindow { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>How long an order holds its stock waiting for payment.</summary>
    public TimeSpan PaymentWindow { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>A waiting user who has not polled for this long is skipped when their turn comes.</summary>
    public TimeSpan HeartbeatTimeout { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>How often clients are told to poll their ticket.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>How often each instance runs admission and releases unpaid orders.</summary>
    public TimeSpan TickInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    public static CheckoutQueueConfig FromEnvironment() => FromLookup(Environment.GetEnvironmentVariable);

    public static CheckoutQueueConfig FromLookup(Func<string, string?> env)
    {
        var d = new CheckoutQueueConfig();
        var config = new CheckoutQueueConfig
        {
            Port = Int(env, "PORT", d.Port, 1, 65535),
            RedisUrl = env("REDIS_URL") ?? d.RedisUrl,
            InstanceName = env("INSTANCE_NAME") ?? d.InstanceName,
            SaleId = env("SALE_ID") ?? d.SaleId,
            InitialStock = Int(env, "INITIAL_STOCK", d.InitialStock, 0, 10_000_000),
            MaxQuantityPerOrder = Int(env, "MAX_PER_ORDER", d.MaxQuantityPerOrder, 1, 100),
            MaxActiveSessions = Int(env, "MAX_ACTIVE", d.MaxActiveSessions, 1, 1_000_000),
            AdmissionWindow = Millis(env, "ADMISSION_WINDOW_MS", d.AdmissionWindow),
            PaymentWindow = Millis(env, "PAYMENT_WINDOW_MS", d.PaymentWindow),
            HeartbeatTimeout = Millis(env, "HEARTBEAT_TIMEOUT_MS", d.HeartbeatTimeout),
            PollInterval = Millis(env, "POLL_INTERVAL_MS", d.PollInterval),
            TickInterval = Millis(env, "TICK_MS", d.TickInterval),
        };
        config.Validate();
        return config;
    }

    /// <summary>Checks the rules that span several settings. Throws with the variable name to fix.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(RedisUrl))
        {
            throw new InvalidOperationException("REDIS_URL must not be empty");
        }
        if (!SaleIdPattern().IsMatch(SaleId))
        {
            throw new InvalidOperationException($"SALE_ID must be 1-64 letters, digits, '-' or '_', got '{SaleId}'");
        }
        // A client that misses a poll or two (GC pause, flaky mobile network) must not lose its place.
        if (HeartbeatTimeout < PollInterval * 3)
        {
            throw new InvalidOperationException(
                $"HEARTBEAT_TIMEOUT_MS ({HeartbeatTimeout.TotalMilliseconds}) must be at least 3 x POLL_INTERVAL_MS ({PollInterval.TotalMilliseconds})");
        }
    }

    private static int Int(Func<string, string?> env, string name, int fallback, int min, int max)
    {
        var raw = env(name);
        if (raw is null)
        {
            return fallback;
        }
        return int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= min && value <= max
            ? value
            : throw new InvalidOperationException($"{name} must be an integer between {min} and {max}, got '{raw}'");
    }

    private static TimeSpan Millis(Func<string, string?> env, string name, TimeSpan fallback) =>
        env(name) is null
            ? fallback
            : TimeSpan.FromMilliseconds(Int(env, name, 0, 1, (int)MaxDuration.TotalMilliseconds));

    [GeneratedRegex("^[A-Za-z0-9_-]{1,64}$")]
    private static partial Regex SaleIdPattern();
}
