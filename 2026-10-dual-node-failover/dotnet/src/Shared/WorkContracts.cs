namespace Shared;

// Wire format of the two architectures where the nodes never talk to each other: an upstream that feeds them
// (an event log for hot-hot redundancy, a task queue for dispatched workers) and a sink that receives their results.

/// <summary>An event appended to the upstream log. Both hot-hot workers read every event.</summary>
public sealed record NewEvent(string? Account, long Amount);

public sealed record LoggedEvent(long Seq, string Account, long Amount);

/// <param name="Last">The newest sequence number in the log, so a reader knows how far behind it is.</param>
public sealed record EventPage(IReadOnlyList<LoggedEvent> Events, long Last);

/// <summary>A task for the upstream queue. Exactly one worker holds it at a time.</summary>
public sealed record NewTask(long Input);

public sealed record NewTaskReply(long Id);

public sealed record TaskLeaseRequest(string? Worker, int WaitMs = 0);

/// <param name="Attempt">How many times this task has been handed out, this time included. Doubles as a fencing token.</param>
/// <param name="TtlMs">The lease runs out this long after the request was sent, unless a heartbeat extends it.</param>
public sealed record TaskLease(long Id, long Input, int Attempt, int TtlMs);

public sealed record TaskAttempt(string? Worker, int Attempt);

/// <summary>One result delivered to the sink. <see cref="Key"/> identifies the event or task it is for.</summary>
public sealed record ResultRecord(string? Key, string? Node, int Attempt, string? Value);

/// <param name="Status">"applied" (first copy), "duplicate" (same value again), "conflict" (a different value), "stale" (an older attempt than one already seen).</param>
public sealed record ResultReply(string Status);

/// <summary>Validation shared by the upstream and the sink.</summary>
public static class WorkIds
{
    public const int MaxLength = 64;

    /// <summary>Accounts and result keys: 1-64 ASCII letters, digits, '-', '_' or '.'.</summary>
    public static bool IsValid(string? value) =>
        value is { Length: > 0 and <= MaxLength } && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.');
}
