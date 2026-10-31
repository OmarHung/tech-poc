namespace Shared;

// The witness API. Nodes call it; nothing else should.

public sealed record LeaseRequest(string? Node);

/// <param name="Granted">Whether the caller now holds the lease for <paramref name="TtlMs"/> from when it sent the request.</param>
/// <param name="Epoch">The current epoch. It goes up by one on every handover and doubles as the fencing token.</param>
/// <param name="StandbyInSync">Whether the node that does not hold the lease may take over. Reset to false on every handover.</param>
/// <param name="SyncVersion">Goes up on every change of <paramref name="StandbyInSync"/> and every handover.</param>
/// <param name="Reason">Why the lease was refused.</param>
public sealed record LeaseReply(bool Granted, string? Holder, long Epoch, int TtlMs, bool StandbyInSync, long SyncVersion, string? Reason);

/// <summary>Sent by the lease holder to say whether the other node has every write it acknowledged.</summary>
/// <param name="ExpectedVersion">
/// For <c>InSync = true</c> only: the latest <see cref="SyncReply.SyncVersion"/> the holder has seen. If anything changed
/// since, the claim may be out of date (for example a request that arrived late) and is refused.
/// </param>
public sealed record SyncRequest(string? Node, long Epoch, bool InSync, long ExpectedVersion = 0);

public sealed record SyncReply(long Epoch, bool StandbyInSync, long SyncVersion);
