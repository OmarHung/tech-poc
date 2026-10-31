using Shared;

namespace Sink;

public enum ResultStatus
{
    /// <summary>First result for this key: the side effect happens now.</summary>
    Applied,

    /// <summary>The same value again (the other hot-hot worker, or a retried task). Ignored.</summary>
    Duplicate,

    /// <summary>A different value for a key that already has one: the two copies disagree. The first one stays.</summary>
    Conflict,

    /// <summary>From an older attempt than one already seen: a worker that lost its task lease and kept going.</summary>
    Stale,
}

/// <summary>
/// The downstream end. Applying a result is the side effect (send the mail, book the payment), so it must happen once
/// per key no matter how many copies arrive: the first copy is applied, later ones are only counted.
/// </summary>
public sealed class ResultStore(TimeProvider time)
{
    public const int MaxValueLength = 1024;

    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private long _received;
    private long _duplicates;
    private long _conflicts;
    private long _stale;

    public ResultStatus Record(ResultRecord record)
    {
        if (!WorkIds.IsValid(record.Key) || !NodeIds.IsValid(record.Node) || record.Attempt < 0
            || record.Value is null || record.Value.Length > MaxValueLength)
        {
            throw new InvalidInputException(
                $"key must be a valid id, node a node id, attempt >= 0 and value a string of at most {MaxValueLength} characters");
        }
        lock (_gate)
        {
            _received++;
            if (!_entries.TryGetValue(record.Key!, out var entry))
            {
                _entries[record.Key!] = new Entry(record.Value, record.Node!, record.Attempt, time.GetUtcNow().ToUnixTimeMilliseconds());
                return ResultStatus.Applied;
            }
            entry.Copies++;
            if (record.Attempt < entry.MaxAttempt)
            {
                entry.Stale++;
                _stale++;
                return ResultStatus.Stale;
            }
            entry.MaxAttempt = record.Attempt;
            if (record.Value == entry.Value)
            {
                _duplicates++;
                return ResultStatus.Duplicate;
            }
            entry.Conflicts++;
            _conflicts++;
            return ResultStatus.Conflict;
        }
    }

    public SinkSnapshot Snapshot()
    {
        lock (_gate)
        {
            var results = _entries.ToDictionary(
                pair => pair.Key,
                pair => new ResultView(pair.Value.Value, pair.Value.Node, pair.Value.Attempt, pair.Value.FirstAtMs,
                    pair.Value.Copies, pair.Value.Conflicts, pair.Value.Stale),
                StringComparer.Ordinal);
            return new SinkSnapshot(new SinkSummary(_received, _entries.Count, _duplicates, _conflicts, _stale), results);
        }
    }

    private sealed class Entry(string value, string node, int attempt, long firstAtMs)
    {
        public string Value { get; } = value;
        public string Node { get; } = node;
        public int Attempt { get; } = attempt;
        public long FirstAtMs { get; } = firstAtMs;
        public int MaxAttempt { get; set; } = attempt;
        public int Copies { get; set; } = 1;
        public int Conflicts { get; set; }
        public int Stale { get; set; }
    }
}

/// <param name="Received">Every result that arrived: the number of side effects a sink without deduplication would cause.</param>
/// <param name="Applied">Distinct keys: the side effects that actually happened.</param>
public sealed record SinkSummary(long Received, long Applied, long Duplicates, long Conflicts, long Stale);

/// <param name="FirstAtMs">Wall-clock time (Unix ms) the first copy arrived.</param>
public sealed record ResultView(string Value, string Node, int Attempt, long FirstAtMs, int Copies, int Conflicts, int Stale);

public sealed record SinkSnapshot(SinkSummary Summary, IReadOnlyDictionary<string, ResultView> Results);
