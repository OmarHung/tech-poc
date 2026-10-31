using System.Threading.Channels;

namespace CheckoutQueue.Queue;

/// <summary>
/// The server-sent event streams open on this instance, by ticket. A stream only ever needs the
/// latest view, so an unsent older one is simply replaced.
/// </summary>
public sealed class TicketStreams
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, List<Channel<TicketView>>> _open = [];

    public TicketStream Open(string ticketId)
    {
        var channel = Channel.CreateBounded<TicketView>(
            new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
        lock (_gate)
        {
            if (!_open.TryGetValue(ticketId, out var channels))
            {
                channels = [];
                _open[ticketId] = channels;
            }
            channels.Add(channel);
        }
        return new TicketStream(channel.Reader, () => Close(ticketId, channel));
    }

    public IReadOnlyCollection<string> TicketIds
    {
        get
        {
            lock (_gate)
            {
                return [.. _open.Keys];
            }
        }
    }

    public void Publish(TicketView view)
    {
        Channel<TicketView>[] targets;
        lock (_gate)
        {
            targets = _open.TryGetValue(view.TicketId, out var channels) ? [.. channels] : [];
        }
        foreach (var channel in targets)
        {
            channel.Writer.TryWrite(view);
        }
    }

    private void Close(string ticketId, Channel<TicketView> channel)
    {
        lock (_gate)
        {
            if (_open.TryGetValue(ticketId, out var channels) && channels.Remove(channel) && channels.Count == 0)
            {
                _open.Remove(ticketId);
            }
        }
    }
}

public sealed class TicketStream(ChannelReader<TicketView> updates, Action close) : IDisposable
{
    public ChannelReader<TicketView> Updates { get; } = updates;

    public void Dispose() => close();
}
