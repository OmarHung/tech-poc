using Shared;

namespace Witness;

/// <summary>What the witness keeps on disk: who holds the lease, in which epoch, and whether the other node may take over.</summary>
/// <param name="SyncVersion">Bumped on every accepted sync report and every handover, so a stale "in sync" can be told apart.</param>
public sealed record WitnessState(long Epoch, string? Holder, bool StandbyInSync, long SyncVersion = 0);

/// <summary>
/// The tie-breaker between the two nodes. It hands out one lease at a time and bumps the epoch on every
/// handover, so a deposed primary can be recognised by its old epoch (the fencing token).
/// It also remembers whether the standby has every acknowledged write, and refuses to promote it if not:
/// a stale standby taking over would silently drop writes the clients were told had succeeded.
/// </summary>
public sealed class LeaseTable
{
    private const string StateFileName = "witness.json";

    private readonly object _gate = new();
    private readonly MonotonicClock _clock;
    private readonly WitnessConfig _config;
    private readonly string _statePath;
    private readonly ILogger<LeaseTable> _logger;
    private WitnessState _state;
    private TimeSpan _expiresAt;

    public LeaseTable(WitnessConfig config, TimeProvider time, ILogger<LeaseTable> logger)
    {
        _config = config;
        _clock = new MonotonicClock(time);
        _logger = logger;
        _statePath = Path.Combine(config.DataDir, StateFileName);
        _state = JsonStateFile.Load<WitnessState>(_statePath) ?? new WitnessState(0, null, StandbyInSync: false);
        // After a restart the old holder may still be inside its lease, so nobody else gets it for one more TTL.
        _expiresAt = _state.Holder is null ? TimeSpan.Zero : _clock.Now + config.LeaseTtl;
    }

    public WitnessState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    /// <summary>Grants, renews or refuses the lease. Renewing and acquiring are the same call.</summary>
    public LeaseReply Acquire(string? node)
    {
        RequireKnown(node);
        lock (_gate)
        {
            var now = _clock.Now;
            if (_state.Holder == node)
            {
                // Also after expiry: nobody else took over in between, so the holder still has the newest data.
                _expiresAt = now + _config.LeaseTtl;
                return Reply(granted: true, reason: null);
            }
            if (_state.Holder is not null && now < _expiresAt)
            {
                return Reply(granted: false, $"lease held by {_state.Holder}");
            }
            if (_state.Holder is not null && !_state.StandbyInSync)
            {
                return Reply(granted: false, $"{node} may be missing acknowledged writes; waiting for {_state.Holder} to return");
            }
            var previous = _state.Holder;
            // The node that just lost the lease is behind until the new holder says otherwise.
            Commit(new WitnessState(_state.Epoch + 1, node, StandbyInSync: false, _state.SyncVersion + 1));
            _expiresAt = now + _config.LeaseTtl;
            _logger.LogWarning("Lease handed from {Previous} to {Holder}, epoch {Epoch}", previous ?? "nobody", node, _state.Epoch);
            return Reply(granted: true, reason: null);
        }
    }

    /// <summary>
    /// Records whether the standby may be promoted. Only the holder, in its own epoch, may say so. "Not in sync" is
    /// always accepted; "in sync" only if nothing changed since the holder last looked, because an HTTP request the
    /// holder gave up on can still arrive later and must not undo a newer "not in sync".
    /// </summary>
    public SyncReply SetStandbyInSync(SyncRequest request)
    {
        RequireKnown(request.Node);
        lock (_gate)
        {
            if (_state.Holder != request.Node || _state.Epoch != request.Epoch)
            {
                throw new ConflictException(
                    $"{request.Node} is not the lease holder in epoch {request.Epoch} (holder {_state.Holder ?? "none"}, epoch {_state.Epoch})");
            }
            if (request.InSync && request.ExpectedVersion != _state.SyncVersion)
            {
                throw new ConflictException(
                    $"standby state changed since version {request.ExpectedVersion} (now {_state.SyncVersion}); check again");
            }
            var changed = _state.StandbyInSync != request.InSync;
            Commit(_state with { StandbyInSync = request.InSync, SyncVersion = _state.SyncVersion + 1 });
            if (changed)
            {
                _logger.LogInformation("Epoch {Epoch}: standby in sync = {InSync}", _state.Epoch, request.InSync);
            }
            return new SyncReply(_state.Epoch, _state.StandbyInSync, _state.SyncVersion);
        }
    }

    public object Health()
    {
        lock (_gate)
        {
            var remaining = _state.Holder is null ? TimeSpan.Zero : _expiresAt - _clock.Now;
            return new
            {
                status = "ok",
                service = "witness",
                holder = _state.Holder,
                epoch = _state.Epoch,
                standbyInSync = _state.StandbyInSync,
                syncVersion = _state.SyncVersion,
                leaseRemainingMs = (long)Math.Max(0, remaining.TotalMilliseconds),
            };
        }
    }

    private LeaseReply Reply(bool granted, string? reason) =>
        new(granted, _state.Holder, _state.Epoch, (int)_config.LeaseTtl.TotalMilliseconds, _state.StandbyInSync, _state.SyncVersion, reason);

    /// <summary>
    /// Disk first, then memory. If the write fails the request fails and nothing changed: a lease granted only in memory
    /// would be forgotten by a restart and could then be granted to the other node as well.
    /// </summary>
    private void Commit(WitnessState next)
    {
        JsonStateFile.Save(_statePath, next);
        _state = next;
    }

    private void RequireKnown(string? node)
    {
        if (node is null || !_config.Nodes.Contains(node))
        {
            throw new InvalidInputException($"node must be one of {string.Join(", ", _config.Nodes)}, got '{node}'");
        }
    }
}
