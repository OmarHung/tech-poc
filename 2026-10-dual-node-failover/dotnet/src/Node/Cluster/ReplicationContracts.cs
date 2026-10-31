using System.Text.Json.Serialization;
using Node.Storage;

namespace Node.Cluster;

/// <summary>Primary to standby: "append these after (prevIndex, prevEpoch)". An empty batch is the heartbeat.</summary>
public sealed record ReplicateRequest(
    string? Leader,
    long Epoch,
    long PrevIndex,
    long PrevEpoch,
    IReadOnlyList<LogEntry>? Entries,
    long LeaderLastIndex);

[JsonConverter(typeof(JsonStringEnumConverter<ReplicateStatus>))]
public enum ReplicateStatus
{
    /// <summary>Applied. The standby now matches the primary up to MatchIndex.</summary>
    [JsonStringEnumMemberName("ok")]
    Ok,

    /// <summary>The standby lacks or disagrees with the entry at PrevIndex; retry from MatchIndex + 1.</summary>
    [JsonStringEnumMemberName("mismatch")]
    Mismatch,

    /// <summary>Not accepted from this sender at all (older epoch, or the standby is asking the witness for the lease).</summary>
    [JsonStringEnumMemberName("rejected")]
    Rejected,
}

/// <param name="Epoch">The standby's epoch after handling the request. Higher than the sender's means the sender was deposed.</param>
/// <param name="PromiseMs">How long from receiving the request the standby will not seek the lease (0 when rejected).</param>
public sealed record ReplicateReply(ReplicateStatus Status, long Epoch, long MatchIndex, long PromiseMs, string? Reason);
