namespace Node.Storage;

/// <summary>One write. <see cref="Index"/> orders writes; <see cref="Epoch"/> says which primary term created it.</summary>
public sealed record LogEntry(long Index, long Epoch, string Key, string Value);

/// <summary>A consistent read of the log: the entries asked for, plus what surrounds them at that moment.</summary>
/// <param name="PrevEpoch">Epoch of the entry just before <see cref="Entries"/> (0 when they start at index 1).</param>
public sealed record LogSlice(long PrevEpoch, IReadOnlyList<LogEntry> Entries, long LastIndex);
