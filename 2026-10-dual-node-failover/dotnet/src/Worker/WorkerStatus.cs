using System.Collections.Immutable;
using Worker.HotHot;

namespace Worker;

/// <summary>Counters behind GET /health, updated by the worker loop.</summary>
public sealed class WorkerStatus
{
    private readonly object _gate = new();
    private readonly Dictionary<string, long> _sinkReplies = new(StringComparer.Ordinal);
    private long _completed;
    private long _abandoned;
    private long _staleCompletions;
    private long? _currentTask;
    private string? _phase;
    private Ledger _ledger = Ledger.Empty;

    public Ledger Ledger
    {
        get
        {
            lock (_gate)
            {
                return _ledger;
            }
        }
        set
        {
            lock (_gate)
            {
                _ledger = value;
            }
        }
    }

    public void SinkReplied(string status)
    {
        lock (_gate)
        {
            _sinkReplies[status] = _sinkReplies.GetValueOrDefault(status) + 1;
        }
    }

    public void TaskStarted(long id) => TaskPhase(id, "working");

    /// <param name="phase">"working", "delivering" (writing the result) or "completing" (reporting it done).</param>
    public void TaskPhase(long id, string phase)
    {
        lock (_gate)
        {
            _currentTask = id;
            _phase = phase;
        }
    }

    /// <param name="outcome">"completed", "abandoned" (lease lost before writing) or "stale" (another attempt owned it at completion).</param>
    public void TaskFinished(string outcome)
    {
        lock (_gate)
        {
            _currentTask = null;
            _phase = null;
            switch (outcome)
            {
                case "completed":
                    _completed++;
                    break;
                case "abandoned":
                    _abandoned++;
                    break;
                default:
                    _staleCompletions++;
                    break;
            }
        }
    }

    public object Health(WorkerConfig config)
    {
        lock (_gate)
        {
            return new
            {
                status = "ok",
                worker = config.WorkerId,
                mode = WorkerConfig.WireName(config.Mode),
                offset = _ledger.Offset,
                sinkReplies = _sinkReplies.ToImmutableSortedDictionary(),
                completed = _completed,
                abandoned = _abandoned,
                staleCompletions = _staleCompletions,
                currentTask = _currentTask,
                phase = _phase,
            };
        }
    }
}
