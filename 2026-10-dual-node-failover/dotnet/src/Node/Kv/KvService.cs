using Node.Cluster;
using Node.Storage;
using Shared;

namespace Node.Kv;

public sealed record KvItem(string Key, string Value, long Index, long Epoch)
{
    public static KvItem From(LogEntry entry) => new(entry.Key, entry.Value, entry.Index, entry.Epoch);
}

/// <summary>The client-facing store. Only the primary serves, and in sync mode only what is safe to acknowledge.</summary>
public sealed class KvService(NodeState state, NodeConfig config)
{
    public const int MaxKeyLength = 128;
    public const int MaxValueLength = 4096;

    public async Task<KvItem> PutAsync(string key, string value, CancellationToken ct)
    {
        var entry = state.AppendAsPrimary(key, value, out var term);
        await WaitDurableAsync(term, entry, ct);
        return KvItem.From(entry);
    }

    public async Task<KvItem> GetAsync(string key, CancellationToken ct)
    {
        var term = state.ServingTerm() ?? throw NodeState.NotPrimary();
        var entry = state.Log.Get(key) ?? throw new NotFoundException($"key '{key}' not found");
        // Never show a value that could still vanish in a failover.
        await WaitDurableAsync(term, entry, ct);
        return KvItem.From(entry);
    }

    public static void ValidateKey(string key)
    {
        if (key.Length is 0 or > MaxKeyLength || !key.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' or ':'))
        {
            throw new InvalidInputException($"key must be 1-{MaxKeyLength} ASCII letters, digits, '-', '_', '.' or ':'");
        }
    }

    public static void ValidateValue(string? value)
    {
        if (value is null || value.Length > MaxValueLength)
        {
            throw new InvalidInputException($"value must be a string of at most {MaxValueLength} characters");
        }
    }

    private Task WaitDurableAsync(PrimaryTerm term, LogEntry entry, CancellationToken ct) =>
        config.Replication == ReplicationMode.Async
            ? Task.CompletedTask
            : term.WaitDurableAsync(entry.Index, config.ReplicationTimeout, state.Clock, ct);
}
