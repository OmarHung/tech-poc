using System.Net;
using Shared;

namespace Node.Cluster;

public sealed class WitnessClient(RpcClient rpc, NodeConfig config, ILogger<WitnessClient> logger) : IArbiter
{
    private readonly Uri _leaseUrl = new(config.WitnessUrl, "lease");
    private readonly Uri _syncUrl = new(config.WitnessUrl, "sync");

    /// <summary>Acquires or renews the lease. Null if the witness could not be reached.</summary>
    public async Task<LeaseReply?> AcquireAsync(CancellationToken ct)
    {
        using var response = await rpc.PostAsync(_leaseUrl, new LeaseRequest(config.NodeId), ct);
        if (response is not { IsSuccessStatusCode: true })
        {
            return null;
        }
        return await RpcClient.ReadAsync<LeaseReply>(response, ct);
    }

    public async Task<SyncReply?> SetStandbyInSyncAsync(long epoch, bool inSync, long expectedVersion, CancellationToken ct)
    {
        using var response = await rpc.PostAsync(_syncUrl, new SyncRequest(config.NodeId, epoch, inSync, expectedVersion), ct);
        if (response is null)
        {
            return null;
        }
        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            logger.LogWarning(
                "Witness refused standby in sync = {InSync} for epoch {Epoch}: {Reason}",
                inSync, epoch, (await RpcClient.ReadAsync<ErrorBody>(response, ct))?.Error);
            return null;
        }
        return response.IsSuccessStatusCode ? await RpcClient.ReadAsync<SyncReply>(response, ct) : null;
    }
}

/// <summary>No witness: nobody to ask, so the primary may always go on alone. This is what lets both sides keep writing.</summary>
public sealed class NoArbiter : IArbiter
{
    public Task<SyncReply?> SetStandbyInSyncAsync(long epoch, bool inSync, long expectedVersion, CancellationToken ct) =>
        Task.FromResult<SyncReply?>(new SyncReply(epoch, inSync, expectedVersion + 1));
}
