using Node.Storage;
using Shared;

namespace Node.Cluster;

public sealed record EpochRecord(long Epoch);

/// <summary>What <c>GET /health</c> returns. The gateway routes to the node that reports <c>role = primary</c>.</summary>
public sealed record NodeHealth(
    string Status,
    string Node,
    string Role,
    long Epoch,
    long LastIndex,
    long? MatchIndex,
    string? Peer,
    long? LeaseRemainingMs,
    long TruncatedEntries,
    string Arbiter,
    string Replication);

/// <summary>
/// Role, epoch and leases of this node, and the only way to write to its log. One lock covers all of them,
/// so a node can never append as primary after it has stepped down.
/// </summary>
/// <remarks>
/// A primary may serve while either lease is valid: the witness lease, or the "peer lease" the standby grants by
/// accepting replication (it promises not to seek the lease for one TTL after hearing from the primary). With two
/// of three votes either way, losing the witness alone or the standby alone does not stop the primary.
/// </remarks>
public sealed class NodeState
{
    private const string EpochFileName = "epoch.json";

    private readonly object _gate = new();
    private readonly NodeConfig _config;
    private readonly IArbiter _arbiter;
    private readonly ILogger<NodeState> _logger;
    private readonly string _epochPath;
    private long _epoch;
    private PrimaryTerm? _term;
    private TimeSpan _witnessLeaseUntil;
    private TimeSpan _peerLeaseUntil;
    private TimeSpan _lastHeardFromPrimary;
    private bool _candidate;
    private TaskCompletionSource<PrimaryTerm> _nextTerm = NewTermSignal();

    public NodeState(NodeConfig config, ReplicatedLog log, IArbiter arbiter, TimeProvider time, ILogger<NodeState> logger)
    {
        _config = config;
        _arbiter = arbiter;
        _logger = logger;
        Log = log;
        Clock = new MonotonicClock(time);
        _epochPath = Path.Combine(config.DataDir, EpochFileName);
        _epoch = Math.Max(JsonStateFile.Load<EpochRecord>(_epochPath)?.Epoch ?? 0, log.LastEpoch);
        // Count startup as having just heard from a primary: a node that restarts must not seek the lease while
        // the primary may still be relying on the promise this node made before it went down.
        _lastHeardFromPrimary = Clock.Now;
    }

    public ReplicatedLog Log { get; }

    public MonotonicClock Clock { get; }

    public long Epoch
    {
        get
        {
            lock (_gate)
            {
                return _epoch;
            }
        }
    }

    /// <summary>The current term while this node is primary, even if its lease just ran out.</summary>
    public PrimaryTerm? CurrentTerm
    {
        get
        {
            lock (_gate)
            {
                return _term;
            }
        }
    }

    /// <summary>The current term if this node may serve right now (primary with a valid lease).</summary>
    public PrimaryTerm? ServingTerm()
    {
        lock (_gate)
        {
            return ServingTermLocked();
        }
    }

    /// <summary>Completes with the current term, or the next one once this node is promoted.</summary>
    public Task<PrimaryTerm> WaitForTermAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            return _term is not null ? Task.FromResult(_term) : _nextTerm.Task.WaitAsync(ct);
        }
    }

    public LogEntry AppendAsPrimary(string key, string value, out PrimaryTerm term)
    {
        lock (_gate)
        {
            term = ServingTermLocked() ?? throw NotPrimary();
            var entry = Log.Append(term.Epoch, key, value);
            term.NotifyAppended();
            return entry;
        }
    }

    /// <summary>The standby side of replication.</summary>
    public ReplicateReply HandleReplicate(ReplicateRequest request)
    {
        lock (_gate)
        {
            if (request.Epoch < _epoch)
            {
                return Rejected($"epoch {request.Epoch} is older than {_epoch}");
            }
            if (_candidate)
            {
                // Accepting would promise the sender a lease while this node may be granted one by the witness.
                ObserveEpochLocked(request.Epoch, $"replication from {request.Leader}");
                return Rejected("asking the witness for the lease");
            }
            if (request.Epoch > _epoch)
            {
                ObserveEpochLocked(request.Epoch, $"replication from {request.Leader}");
            }
            else if (_term is not null)
            {
                return Rejected($"also primary in epoch {_epoch}"); // only possible without a witness
            }
            _lastHeardFromPrimary = Clock.Now;
            var outcome = Log.AppendFrom(request.Epoch, request.PrevIndex, request.PrevEpoch, request.Entries ?? [], request.LeaderLastIndex);
            // The promise: this node will not seek the lease for one TTL from now (see TryBeginCandidacy).
            var promiseMs = (long)_config.LeaseTtl.TotalMilliseconds;
            return outcome.Status == AppendStatus.Ok
                ? new ReplicateReply(ReplicateStatus.Ok, _epoch, outcome.MatchIndex, promiseMs, null)
                : new ReplicateReply(ReplicateStatus.Mismatch, _epoch, outcome.MatchIndex, promiseMs, "log does not contain prevIndex/prevEpoch");
        }
    }

    /// <summary>Starts a primary term. Returns false if a newer epoch is already known.</summary>
    /// <param name="standbyInSync">What the arbiter said about the other node when it granted the lease.</param>
    /// <param name="witnessVersion">The witness's sync version in that same reply (0 without a witness).</param>
    public bool TryPromote(long epoch, TimeSpan leaseUntil, bool standbyInSync, long witnessVersion = 0)
    {
        lock (_gate)
        {
            if (epoch < _epoch)
            {
                return false;
            }
            if (_term is not null)
            {
                StepDownLocked("starting a new term");
            }
            SaveEpochLocked(epoch);
            _term = new PrimaryTerm(epoch, standbyInSync, witnessVersion, Log, _arbiter, ReplicationWorker.MaxBatch);
            _witnessLeaseUntil = leaseUntil;
            _peerLeaseUntil = TimeSpan.Zero;
            var promoted = _nextTerm;
            _nextTerm = NewTermSignal();
            promoted.TrySetResult(_term);
            _logger.LogWarning(
                "Promoted to primary in epoch {Epoch} at index {LastIndex}; standby in sync: {InSync}", epoch, Log.LastIndex, standbyInSync);
            return true;
        }
    }

    public void ExtendWitnessLease(PrimaryTerm term, TimeSpan until)
    {
        lock (_gate)
        {
            if (_term == term && until > _witnessLeaseUntil)
            {
                _witnessLeaseUntil = until;
            }
        }
    }

    public void ExtendPeerLease(PrimaryTerm term, TimeSpan until)
    {
        lock (_gate)
        {
            if (_term == term && until > _peerLeaseUntil)
            {
                _peerLeaseUntil = until;
            }
        }
    }

    public void StepDown(PrimaryTerm term, string reason)
    {
        lock (_gate)
        {
            if (_term == term)
            {
                StepDownLocked(reason);
            }
        }
    }

    public void StepDownIfLeaseExpired()
    {
        lock (_gate)
        {
            if (_term is not null && ServingTermLocked() is null)
            {
                StepDownLocked("lease expired");
            }
        }
    }

    /// <summary>Adopts a newer epoch seen elsewhere; a primary that sees one has been deposed.</summary>
    public void ObserveEpoch(long epoch, string source)
    {
        lock (_gate)
        {
            ObserveEpochLocked(epoch, source);
        }
    }

    /// <summary>
    /// Marks this standby as a candidate if it has not heard from a primary for a full lease. While a candidate it
    /// refuses replication, so no primary can count on it while it asks for the lease.
    /// </summary>
    public bool TryBeginCandidacy()
    {
        lock (_gate)
        {
            if (_term is not null || _candidate || Clock.Now - _lastHeardFromPrimary < _config.LeaseTtl + _config.ElectionDelay)
            {
                return false;
            }
            _candidate = true;
            return true;
        }
    }

    public void EndCandidacy()
    {
        lock (_gate)
        {
            _candidate = false;
        }
    }

    public NodeHealth Health()
    {
        lock (_gate)
        {
            var serving = ServingTermLocked();
            long? leaseRemainingMs = serving is null || _config.Arbiter == ArbiterMode.None
                ? null
                : (long)(LeaseUntilLocked() - Clock.Now).TotalMilliseconds;
            return new NodeHealth(
                "ok",
                _config.NodeId,
                serving is null ? "standby" : "primary",
                _epoch,
                Log.LastIndex,
                serving?.MatchIndex,
                serving is null ? null : WireName(serving.PeerState),
                leaseRemainingMs,
                Log.TruncatedEntries,
                NodeConfig.WireName(_config.Arbiter),
                NodeConfig.WireName(_config.Replication));
        }
    }

    public static string WireName(PeerState state) => state switch
    {
        PeerState.InSync => "in-sync",
        PeerState.Solo => "solo",
        _ => "catching-up",
    };

    public static UnavailableException NotPrimary() => new("not primary");

    private PrimaryTerm? ServingTermLocked() =>
        _term is not null && Clock.Now < LeaseUntilLocked() ? _term : null;

    private TimeSpan LeaseUntilLocked() =>
        _witnessLeaseUntil > _peerLeaseUntil ? _witnessLeaseUntil : _peerLeaseUntil;

    private void ObserveEpochLocked(long epoch, string source)
    {
        if (epoch <= _epoch)
        {
            return;
        }
        SaveEpochLocked(epoch);
        if (_term is not null)
        {
            StepDownLocked($"epoch {epoch} seen in {source}");
            // Someone newer is primary; give it a full lease to reach this node before competing again.
            _lastHeardFromPrimary = Clock.Now;
        }
    }

    private void StepDownLocked(string reason)
    {
        var term = _term!;
        _term = null;
        _witnessLeaseUntil = TimeSpan.Zero;
        _peerLeaseUntil = TimeSpan.Zero;
        term.End();
        _logger.LogWarning("Stepped down from epoch {Epoch} at index {LastIndex}: {Reason}", term.Epoch, Log.LastIndex, reason);
    }

    private void SaveEpochLocked(long epoch)
    {
        if (epoch == _epoch)
        {
            return;
        }
        JsonStateFile.Save(_epochPath, new EpochRecord(epoch));
        _epoch = epoch;
    }

    private ReplicateReply Rejected(string reason) => new(ReplicateStatus.Rejected, _epoch, Log.LastIndex, 0, reason);

    private static TaskCompletionSource<PrimaryTerm> NewTermSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
