using System.Net;
using System.Text.Json;
using CheckoutQueue.Orders;
using CheckoutQueue.Queue;

namespace CheckoutQueue.Tests;

/// <summary>The shared view every demo tab draws: all users, read without touching their heartbeat.</summary>
[Collection(RedisCollection.Name)]
public sealed class TicketBoardTests(RedisFixture redis)
{
    [Fact]
    public async Task List_ShowsEachUsersCurrentTicket_WithItsOrder()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var order = await sale.PlaceOrderAsync("buyer", quantity: 2);
        var waiting = (await sale.Queue.JoinAsync("waiter")).Ticket!;

        var board = await sale.Board.ListAsync();

        Assert.Equal(["waiter", "buyer"], board.Select(e => e.Ticket.UserId));
        Assert.Equal((waiting.TicketId, TicketStatus.Waiting, 1L), (board[0].Ticket.TicketId, board[0].Ticket.Status, board[0].Ticket.Position!.Value));
        Assert.Null(board[0].Order);
        Assert.Equal((order.OrderId, OrderStatus.PendingPayment, 2), (board[1].Order!.OrderId, board[1].Order!.Status, board[1].Order!.Quantity));
    }

    [Fact]
    public async Task List_ShowsOnlyTheNewTicket_AfterAUserLinesUpAgain()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var first = (await sale.JoinAndAdmitAsync("slow"))[0];
        sale.Time.Advance(TimeSpan.FromSeconds(60));
        var second = (await sale.Queue.JoinAsync("slow")).Ticket!;

        var board = await sale.Board.ListAsync();

        var entry = Assert.Single(board);
        Assert.NotEqual(first.TicketId, entry.Ticket.TicketId);
        Assert.Equal(second.TicketId, entry.Ticket.TicketId);
    }

    [Fact]
    public async Task List_IsNotAHeartbeat_SoWatchersCannotKeepAGoneUserInLine()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 1 });
        await sale.Queue.JoinAsync("gone");

        sale.Time.Advance(TimeSpan.FromSeconds(16));
        await sale.Board.ListAsync();
        await sale.Queue.AdmitAsync();

        Assert.Equal(TicketStatus.Abandoned, (await sale.Board.ListAsync())[0].Ticket.Status);
    }

    [Fact]
    public async Task AdminTickets_ReturnsTicketAndOrderObjects()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        await app.Client.PostAsync("/queue/join", new StringContent("""{"userId":"alice"}""", System.Text.Encoding.UTF8, "application/json"));

        var response = await app.Client.GetAsync("/admin/tickets");
        var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var entry = Assert.Single(body.EnumerateArray());
        Assert.Equal("alice", entry.GetProperty("ticket").GetProperty("userId").GetString());
        Assert.Equal("waiting", entry.GetProperty("ticket").GetProperty("status").GetString());
        Assert.False(entry.TryGetProperty("order", out _), "no order yet, so the field is left out");
    }
}
