using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using CheckoutQueue.Queue;

namespace CheckoutQueue.Tests;

/// <summary>Server-sent events: the push alternative to polling a ticket.</summary>
[Collection(RedisCollection.Name)]
public sealed class TicketEventsTests(RedisFixture redis)
{
    private static readonly TimeSpan EventTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Watch_ReturnsTheViews_AndCountsAsPresence()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 2 });
        var watched = (await sale.Queue.JoinAsync("streaming")).Ticket!;
        var silent = (await sale.Queue.JoinAsync("silent")).Ticket!;

        sale.Time.Advance(TimeSpan.FromSeconds(16));
        var views = await sale.Queue.WatchAsync([watched.TicketId, Ids.New()]);
        await sale.Queue.AdmitAsync();

        Assert.Equal(watched.TicketId, Assert.Single(views).TicketId);
        Assert.Equal(TicketStatus.Admitted, (await sale.Queue.GetTicketAsync(watched.TicketId))!.Status);
        Assert.Equal(TicketStatus.Abandoned, (await sale.Queue.GetTicketAsync(silent.TicketId))!.Status);
    }

    [Fact]
    public async Task Events_StartWithTheTicket_AndPushTheAdmissionRightAway()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        var ticketId = await JoinAsync(app, "listener");
        using var response = await app.Client.GetAsync($"/queue/tickets/{ticketId}/events", HttpCompletionOption.ResponseHeadersRead);
        using var events = new StreamReader(await response.Content.ReadAsStreamAsync());

        var first = await NextEventAsync(events);
        await app.Maintenance.RunOnceAsync();
        var second = await NextEventAsync(events);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType!.MediaType);
        Assert.Equal(("waiting", 1), (first.GetProperty("status").GetString(), first.GetProperty("position").GetInt32()));
        Assert.Equal("admitted", second.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Events_EndOnceTheTicketCanNoLongerChange()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        var ticketId = await JoinAsync(app, "buyer");
        await app.Maintenance.RunOnceAsync();
        using var response = await app.Client.GetAsync($"/queue/tickets/{ticketId}/events", HttpCompletionOption.ResponseHeadersRead);
        using var events = new StreamReader(await response.Content.ReadAsStreamAsync());
        Assert.Equal("admitted", (await NextEventAsync(events)).GetProperty("status").GetString());

        await app.Client.PostAsJsonAsync("/checkout", new { ticketId, quantity = 1 });
        app.Time.Advance(TimeSpan.FromSeconds(1));
        var last = await NextEventAsync(events);

        Assert.Equal("ordered", last.GetProperty("status").GetString());
        Assert.Null(await events.ReadLineAsync().WaitAsync(EventTimeout));
    }

    private static async Task<string> JoinAsync(TestApp app, string userId)
    {
        var response = await app.Client.PostAsJsonAsync("/queue/join", new { userId });
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("ticketId").GetString()!;
    }

    /// <summary>Reads lines up to the blank line that ends an event and returns its data as JSON. Skips keep-alive comments.</summary>
    private static async Task<JsonElement> NextEventAsync(StreamReader events)
    {
        string? data = null;
        while (true)
        {
            var line = await events.ReadLineAsync().WaitAsync(EventTimeout) ?? throw new EndOfStreamException("stream ended");
            if (line.StartsWith("data: ", StringComparison.Ordinal))
            {
                data = line["data: ".Length..];
            }
            else if (line.Length == 0 && data is not null)
            {
                return JsonDocument.Parse(data).RootElement;
            }
        }
    }
}
