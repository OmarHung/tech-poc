using Shared;

namespace Node.Storage;

public enum AppendStatus
{
    Ok,
    Mismatch,
}

/// <param name="MatchIndex">On Ok, the last index of the batch, now known to match the leader. On Mismatch, where the leader should continue from.</param>
public readonly record struct AppendOutcome(AppendStatus Status, long MatchIndex);

/// <summary>
/// The node's data: an ordered log of writes plus the key-value view built from it. Thread-safe.
/// Replication uses Raft's log-matching rule: an entry is identified by (index, epoch), a standby only accepts a
/// batch that continues an entry it already has, and drops whatever it has past that point that disagrees.
/// </summary>
public sealed class ReplicatedLog : IDisposable
{
    private readonly object _gate = new();
    private readonly LogFile _file;
    private readonly List<LogEntry> _entries;
    private readonly Dictionary<string, LogEntry> _latest = new(StringComparer.Ordinal);
    private long _truncated;

    private ReplicatedLog(LogFile file, List<LogEntry> entries)
    {
        _file = file;
        _entries = entries;
        RebuildLatest();
    }

    public static ReplicatedLog Open(string path)
    {
        var (file, entries) = LogFile.Open(path);
        return new ReplicatedLog(file, entries);
    }

    public long LastIndex
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    public long LastEpoch
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count == 0 ? 0 : _entries[^1].Epoch;
            }
        }
    }

    /// <summary>Entries this node dropped because a newer primary never had them (writes nobody was told succeeded).</summary>
    public long TruncatedEntries
    {
        get
        {
            lock (_gate)
            {
                return _truncated;
            }
        }
    }

    public LogEntry Append(long epoch, string key, string value)
    {
        lock (_gate)
        {
            var entry = new LogEntry(_entries.Count + 1, epoch, key, value);
            _file.Append([entry]);
            Add(entry);
            return entry;
        }
    }

    /// <summary>Applies a batch from the primary (the standby side of replication).</summary>
    /// <param name="leaderEpoch">The primary's epoch.</param>
    /// <param name="leaderLastIndex">The primary's last index when it built the batch.</param>
    public AppendOutcome AppendFrom(long leaderEpoch, long prevIndex, long prevEpoch, IReadOnlyList<LogEntry> entries, long leaderLastIndex)
    {
        ValidateBatch(leaderEpoch, prevIndex, entries, leaderLastIndex);
        lock (_gate)
        {
            if (EpochAtLocked(prevIndex) != prevEpoch)
            {
                return new AppendOutcome(AppendStatus.Mismatch, MismatchHintLocked(prevIndex));
            }
            var fresh = new List<LogEntry>();
            foreach (var entry in entries)
            {
                if (fresh.Count == 0 && entry.Index <= _entries.Count)
                {
                    if (_entries[(int)entry.Index - 1].Epoch == entry.Epoch)
                    {
                        continue; // already have it, e.g. a resend after a timeout
                    }
                    TruncateLocked(entry.Index);
                }
                fresh.Add(entry);
            }
            if (fresh.Count > 0)
            {
                _file.Append(fresh);
                fresh.ForEach(Add);
            }
            var matched = prevIndex + entries.Count;
            // The batch reached the primary's last entry when it was sent, so entries past it from an older epoch came from
            // an older primary. Entries from this epoch stay: a request that arrived late must not undo a newer one.
            if (matched == leaderLastIndex && _entries.Count > matched && _entries[(int)matched].Epoch < leaderEpoch)
            {
                TruncateLocked(matched + 1);
            }
            return new AppendOutcome(AppendStatus.Ok, matched);
        }
    }

    /// <summary>Up to <paramref name="max"/> entries starting at <paramref name="fromIndex"/>; null if that is past the end.</summary>
    public LogSlice? Read(long fromIndex, int max)
    {
        lock (_gate)
        {
            if (EpochAtLocked(fromIndex - 1) is not { } prevEpoch)
            {
                return null;
            }
            var start = (int)fromIndex - 1;
            var count = Math.Clamp(_entries.Count - start, 0, max);
            return new LogSlice(prevEpoch, _entries.GetRange(start, count), _entries.Count);
        }
    }

    public LogEntry? Get(string key)
    {
        lock (_gate)
        {
            return _latest.GetValueOrDefault(key);
        }
    }

    public Dictionary<string, string> Snapshot()
    {
        lock (_gate)
        {
            return _latest.ToDictionary(pair => pair.Key, pair => pair.Value.Value, StringComparer.Ordinal);
        }
    }

    public void Dispose() => _file.Dispose();

    private static void ValidateBatch(long leaderEpoch, long prevIndex, IReadOnlyList<LogEntry> entries, long leaderLastIndex)
    {
        if (prevIndex < 0 || leaderLastIndex < prevIndex + entries.Count)
        {
            throw new InvalidInputException("prevIndex and leaderLastIndex do not fit the batch");
        }
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry is not { Key: not null, Value: not null } || entry.Index != prevIndex + 1 + i || entry.Epoch < 1 || entry.Epoch > leaderEpoch)
            {
                throw new InvalidInputException($"entry {i} does not continue the batch at index {prevIndex + 1 + i}");
            }
        }
    }

    /// <summary>Epoch at <paramref name="index"/>; 0 for index 0 (the empty prefix), null past the end.</summary>
    private long? EpochAtLocked(long index) => index switch
    {
        0 => 0,
        _ when index < 0 || index > _entries.Count => null,
        _ => _entries[(int)index - 1].Epoch,
    };

    /// <summary>
    /// Where the leader should retry from: the end of this log if it is too short, otherwise just before the
    /// first entry of the disagreeing epoch, so a whole divergent stretch is skipped in one round trip (Raft's fast backup).
    /// </summary>
    private long MismatchHintLocked(long prevIndex)
    {
        if (prevIndex == 0)
        {
            return 0; // only a malformed request (index 0 with a nonzero epoch) gets here
        }
        if (prevIndex > _entries.Count)
        {
            return _entries.Count;
        }
        var conflictEpoch = _entries[(int)prevIndex - 1].Epoch;
        var index = prevIndex;
        while (index > 1 && _entries[(int)index - 2].Epoch == conflictEpoch)
        {
            index--;
        }
        return index - 1;
    }

    private void TruncateLocked(long fromIndex)
    {
        var start = (int)fromIndex - 1;
        var count = _entries.Count - start;
        // File first: if the rewrite fails, memory still matches what is on disk.
        _file.Rewrite(_entries.GetRange(0, start));
        _entries.RemoveRange(start, count);
        _truncated += count;
        RebuildLatest();
    }

    private void Add(LogEntry entry)
    {
        _entries.Add(entry);
        _latest[entry.Key] = entry;
    }

    private void RebuildLatest()
    {
        _latest.Clear();
        foreach (var entry in _entries)
        {
            _latest[entry.Key] = entry;
        }
    }
}
