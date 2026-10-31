namespace CheckoutQueue.Admin;

/// <summary>
/// The sale rules a client needs to draw the line and its countdowns. Deliberately leaves out
/// the Redis address, which may carry a password.
/// </summary>
public sealed record SaleSettings(
    string Instance,
    string SaleId,
    int MaxActive,
    int MaxPerOrder,
    long AdmissionWindowMs,
    long PaymentWindowMs,
    long HeartbeatTimeoutMs,
    long PollIntervalMs,
    long TickMs)
{
    public static SaleSettings From(CheckoutQueueConfig config) => new(
        config.InstanceName,
        config.SaleId,
        config.MaxActiveSessions,
        config.MaxQuantityPerOrder,
        (long)config.AdmissionWindow.TotalMilliseconds,
        (long)config.PaymentWindow.TotalMilliseconds,
        (long)config.HeartbeatTimeout.TotalMilliseconds,
        (long)config.PollInterval.TotalMilliseconds,
        (long)config.TickInterval.TotalMilliseconds);
}
