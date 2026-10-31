using System.Text.Json;
using System.Threading.Channels;
using CheckoutQueue.Http;
using Microsoft.Extensions.Options;
using HttpJsonOptions = Microsoft.AspNetCore.Http.Json.JsonOptions;

namespace CheckoutQueue.Queue;

/// <summary>
/// <c>GET /queue/tickets/{ticketId}/events</c>: the ticket as server-sent events, the push
/// alternative to polling. Each event's data is the same JSON as <c>GET /queue/tickets/{ticketId}</c>.
/// The stream ends once the ticket can no longer change.
/// </summary>
public static class TicketEvents
{
    /// <summary>Proxies and load balancers drop connections that stay silent for too long.</summary>
    private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(15);

    private static readonly HashSet<TicketStatus> Final =
        [TicketStatus.Ordered, TicketStatus.Expired, TicketStatus.Abandoned, TicketStatus.SoldOut];

    public static async Task StreamAsync(
        string ticketId,
        HttpContext http,
        QueueStore queue,
        TicketStreams streams,
        IOptions<HttpJsonOptions> json,
        CancellationToken ct)
    {
        var id = HttpExtensions.ValidId(ticketId, "ticket id");
        // Open the stream before reading the ticket, so no change can slip in between the two.
        using var stream = streams.Open(id);
        var view = await queue.GetTicketAsync(id, ct) ?? throw ApiException.NotFound("ticket_not_found", "ticket not found");

        var response = http.Response;
        response.ContentType = "text/event-stream";
        response.Headers.CacheControl = "no-cache";
        response.Headers["X-Accel-Buffering"] = "no";
        var options = json.Value.SerializerOptions;
        try
        {
            await WriteAsync(response, $"data: {JsonSerializer.Serialize(view, options)}\n\n", ct);
            while (!Final.Contains(view.Status))
            {
                var next = await NextAsync(stream.Updates, ct);
                if (next is null)
                {
                    await WriteAsync(response, ": keep-alive\n\n", ct);
                }
                else if (next != view)
                {
                    view = next;
                    await WriteAsync(response, $"data: {JsonSerializer.Serialize(view, options)}\n\n", ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The client went away; closing the stream is all there is to do.
        }
    }

    /// <summary>The next pushed view, or null when it is time for a keep-alive.</summary>
    private static async Task<TicketView?> NextAsync(ChannelReader<TicketView> updates, CancellationToken ct)
    {
        using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct);
        wait.CancelAfter(KeepAliveInterval);
        try
        {
            return await updates.WaitToReadAsync(wait.Token) && updates.TryRead(out var view) ? view : null;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return null;
        }
    }

    private static async Task WriteAsync(HttpResponse response, string text, CancellationToken ct)
    {
        await response.WriteAsync(text, ct);
        await response.Body.FlushAsync(ct);
    }
}
