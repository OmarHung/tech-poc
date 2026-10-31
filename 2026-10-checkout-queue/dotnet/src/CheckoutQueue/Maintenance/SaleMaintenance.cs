using CheckoutQueue.Orders;
using CheckoutQueue.Queue;

namespace CheckoutQueue.Maintenance;

public sealed record MaintenanceResult(int ReleasedOrders, AdmitResult Admission);

/// <summary>
/// One maintenance pass. Every instance runs it on its own timer; each step is one atomic
/// script, so there is no leader to elect and no instance whose loss stops the line.
/// </summary>
public sealed class SaleMaintenance(QueueStore queue, OrderStore orders)
{
    public async Task<MaintenanceResult> RunOnceAsync(CancellationToken ct = default)
    {
        // Release first: stock coming back from unpaid orders can be admitted in the same pass.
        var released = await orders.ExpireOrdersAsync(ct);
        var admission = await queue.AdmitAsync(ct);
        return new MaintenanceResult(released, admission);
    }
}
