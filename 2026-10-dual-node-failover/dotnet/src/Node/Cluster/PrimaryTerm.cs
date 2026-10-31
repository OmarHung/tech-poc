using Node.Storage;
using Shared;

namespace Node.Cluster;

/// <summary>Where the standby stands, as far as the primary knows.</summary>
public enum PeerState
{
    /// <summary>The witness may promote the standby, so every acknowledged write must already be on it.</summary>
    InSync,

    /// <summary>The witness agreed not to promote the standby; a write is acknowledged once it is on this node.</summary>
    Solo,

    /// <summary>Writes wait for the standby, and once it has everything the witness is told it may take over again.</summary>
    CatchingUp,
}

/// <summary>Reports to the arbiter whether the standby may take over. Without a witness every report is accepted.</summary>
public interface IArbiter
{
    /// <param name="expectedVersion">The latest witness version this node has seen; only checked for <paramref name="inSync"/> = true.</param>
    /// <returns>The witness's answer, or null if the report was refused or the witness could not be reached.</returns>
    Task<SyncReply?> SetStandbyInSyncAsync(long epoch, bool inSync, long expectedVersion, CancellationToken ct);
}

/// <summary>
/// One stint as primary: created on promotion, ended on step-down. Tracks how far the standby has
/// replicated and decides when a write is safe to acknowledge.
/// </summary>
public sealed class PrimaryTerm
{
    private readonly object _gate = new();
    private readonly CancellationTokenSource _ended = new();
    // One witness report at a time, so this node never has two in flight that could arrive in the wrong order.
    // Reports from an older process or a call this node gave up on are handled by the witness's version check.
    private readonly SemaphoreSlim _witnessCalls = new(1, 1);
    private readonly ReplicatedLog _log;
    private readonly IArbiter _arbiter;
    private readonly int _catchUpSlack;
    private PeerState _peerState;
    private long _matchIndex;
    private long _catchUpTarget;
    private long _witnessVersion;
    private TaskCompletionSource _progress = NewSignal();
    private TaskCompletionSource _appended = NewSignal();
    private Task<bool>? _soloRequest;

    /// <param name="standbyInSync">What the witness said when it granted the lease.</param>
    /// <param name="catchUpSlack">How far behind the standby may be when writes start waiting for it again.</param>
    public PrimaryTerm(long epoch, bool standbyInSync, long witnessVersion, ReplicatedLog log, IArbiter arbiter, int catchUpSlack)
    {
        Epoch = epoch;
        _log = log;
        _arbiter = arbiter;
        _catchUpSlack = catchUpSlack;
        _witnessVersion = witnessVersion;
        _peerState = standbyInSync ? PeerState.InSync : PeerState.CatchingUp;
        _catchUpTarget = log.LastIndex;
    }

    public long Epoch { get; }

    public CancellationToken Ended => _ended.Token;

    public PeerState PeerState
    {
        get
        {
            lock (_gate)
            {
                return _peerState;
            }
        }
    }

    public long MatchIndex
    {
        get
        {
            lock (_gate)
            {
                return _matchIndex;
            }
        }
    }

    /// <summary>Completes on the next append. Take it before reading the log so no append slips between.</summary>
    public Task AppendSignal
    {
        get
        {
            lock (_gate)
            {
                return _appended.Task;
            }
        }
    }

    public void NotifyAppended()
    {
        lock (_gate)
        {
            Pulse(ref _appended);
        }
    }

    internal void End()
    {
        lock (_gate)
        {
            // Callers hold NodeState's lock; CancelAsync runs token callbacks elsewhere instead of inside it.
            _ = _ended.CancelAsync();
            Pulse(ref _progress);
            Pulse(ref _appended);
        }
    }

    /// <summary>
    /// Returns once <paramref name="index"/> may be acknowledged: it is on the standby, or the witness has agreed the
    /// standby is out. If the standby has not confirmed within <paramref name="timeout"/>, asks the witness to go on alone.
    /// </summary>
    public async Task WaitDurableAsync(long index, TimeSpan timeout, MonotonicClock clock, CancellationToken ct)
    {
        var deadline = clock.Now + timeout;
        while (true)
        {
            Task progress;
            lock (_gate)
            {
                if (index <= _matchIndex || _peerState == PeerState.Solo)
                {
                    return;
                }
                if (_ended.IsCancellationRequested)
                {
                    throw new UnavailableException("lost the primary role before the write reached the standby");
                }
                progress = _progress.Task;
            }
            var remaining = deadline - clock.Now;
            if (remaining <= TimeSpan.Zero)
            {
                if (!await TryGoSoloAsync())
                {
                    throw new UnavailableException("standby unreachable and the witness did not allow going on alone");
                }
                continue;
            }
            try
            {
                await progress.WaitAsync(remaining, clock.Time, ct);
            }
            catch (TimeoutException)
            {
                // Deadline reached; the loop asks the witness.
            }
        }
    }

    /// <summary>
    /// Asks the witness to let this node acknowledge writes on its own. Writes waiting for the standby share one request.
    /// The replicator also calls it when a term starts with the witness saying the standby is behind: that both avoids
    /// stalling the first writes after a failover and outdates any late "in sync" from an older process.
    /// </summary>
    public async Task<bool> TryGoSoloAsync()
    {
        Task<bool> request;
        lock (_gate)
        {
            if (_peerState == PeerState.Solo)
            {
                return true;
            }
            // Task.Run keeps the request's completion (which takes this lock) from running inside it.
            request = _soloRequest ??= Task.Run(RequestSoloAsync);
        }
        return await request;
    }

    /// <summary>Records that the standby holds everything up to <paramref name="matchIndex"/>.</summary>
    /// <returns>True when the standby has caught up and the witness should be told it may be promoted again.</returns>
    public bool RecordReplicated(long matchIndex)
    {
        lock (_gate)
        {
            _matchIndex = Math.Max(_matchIndex, matchIndex);
            if (_peerState == PeerState.Solo)
            {
                // Read under this lock: a write acknowledged while Solo was appended before it saw Solo, so it is
                // at or below this index, and every write from here on waits for the standby.
                var lastIndex = _log.LastIndex;
                if (lastIndex - _matchIndex <= _catchUpSlack)
                {
                    StartCatchingUpLocked(lastIndex);
                }
            }
            Pulse(ref _progress);
            return _peerState == PeerState.CatchingUp && _matchIndex >= _catchUpTarget;
        }
    }

    /// <summary>
    /// Takes in what the witness reported on lease renewal. If it says the standby is behind while this node thinks
    /// it is in sync (a "go on alone" whose reply was lost), the standby goes through catching up and is reported again.
    /// </summary>
    public void ObserveWitness(long version, bool standbyInSync)
    {
        lock (_gate)
        {
            if (version < _witnessVersion)
            {
                return; // an older reply overtaken by one already seen
            }
            _witnessVersion = version;
            if (!standbyInSync && _peerState == PeerState.InSync)
            {
                StartCatchingUpLocked(_log.LastIndex);
            }
        }
    }

    /// <summary>Tells the witness the standby may be promoted again, if it still has every acknowledged write.</summary>
    public async Task TryMarkInSyncAsync(CancellationToken ct)
    {
        try
        {
            await _witnessCalls.WaitAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
        try
        {
            long expectedVersion;
            lock (_gate)
            {
                if (_peerState != PeerState.CatchingUp || _matchIndex < _catchUpTarget)
                {
                    return;
                }
                expectedVersion = _witnessVersion;
            }
            var reply = await _arbiter.SetStandbyInSyncAsync(Epoch, inSync: true, expectedVersion, ct);
            if (reply is null)
            {
                return;
            }
            lock (_gate)
            {
                _witnessVersion = Math.Max(_witnessVersion, reply.SyncVersion);
                if (_peerState == PeerState.CatchingUp)
                {
                    _peerState = PeerState.InSync;
                }
            }
        }
        finally
        {
            _witnessCalls.Release();
        }
    }

    private async Task<bool> RequestSoloAsync()
    {
        try
        {
            await _witnessCalls.WaitAsync(Ended);
            try
            {
                long version;
                lock (_gate)
                {
                    version = _witnessVersion;
                }
                var reply = await _arbiter.SetStandbyInSyncAsync(Epoch, inSync: false, version, Ended);
                if (reply is null)
                {
                    return false;
                }
                lock (_gate)
                {
                    _witnessVersion = Math.Max(_witnessVersion, reply.SyncVersion);
                    _peerState = PeerState.Solo;
                    Pulse(ref _progress);
                }
                return true;
            }
            finally
            {
                _witnessCalls.Release();
            }
        }
        catch (OperationCanceledException) when (Ended.IsCancellationRequested)
        {
            return false;
        }
        finally
        {
            lock (_gate)
            {
                _soloRequest = null;
            }
        }
    }

    private void StartCatchingUpLocked(long target)
    {
        _peerState = PeerState.CatchingUp;
        _catchUpTarget = target;
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static void Pulse(ref TaskCompletionSource signal)
    {
        var fired = signal;
        signal = NewSignal();
        fired.TrySetResult();
    }
}
