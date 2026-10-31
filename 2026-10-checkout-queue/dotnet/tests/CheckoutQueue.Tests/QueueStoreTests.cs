using CheckoutQueue.Queue;

namespace CheckoutQueue.Tests;

[Collection(RedisCollection.Name)]
public sealed class QueueStoreTests(RedisFixture redis)
{
    [Fact]
    public async Task Join_PutsUsersInLine_InArrivalOrder()
    {
        await using var sale = await TestSale.StartAsync(redis);

        var first = await sale.Queue.JoinAsync("alice");
        var second = await sale.Queue.JoinAsync("bob");

        Assert.Equal(JoinOutcome.Created, first.Outcome);
        Assert.Equal(TicketStatus.Waiting, second.Ticket!.Status);
        Assert.Equal(1, first.Ticket!.Position);
        Assert.Equal(2, second.Ticket.Position);
        Assert.True(second.Ticket.Seq > first.Ticket.Seq);
        Assert.Equal(1000, second.Ticket.PollAfterMs);
    }

    [Fact]
    public async Task Join_Again_ReturnsTheSameTicket_SoNobodyHoldsTwoPlaces()
    {
        await using var sale = await TestSale.StartAsync(redis);

        var first = await sale.Queue.JoinAsync("alice");
        var again = await sale.Queue.JoinAsync("alice");

        Assert.Equal(JoinOutcome.Existing, again.Outcome);
        Assert.Equal(first.Ticket!.TicketId, again.Ticket!.TicketId);
    }

    [Fact]
    public async Task Admit_LetsInAtMostMaxActive_InArrivalOrder()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 2 });

        var tickets = await sale.JoinAndAdmitAsync("a", "b", "c");

        Assert.Equal(TicketStatus.Admitted, tickets[0].Status);
        Assert.Equal(TicketStatus.Admitted, tickets[1].Status);
        Assert.Equal(TicketStatus.Waiting, tickets[2].Status);
        Assert.Equal(1, tickets[2].Position);
        Assert.Equal(sale.NowMs, tickets[0].AdmittedAtMs);
        Assert.Equal(sale.NowMs + 60_000, tickets[0].AdmissionExpiresAtMs);
        Assert.Null(tickets[0].Position);
    }

    [Fact]
    public async Task Admit_NeverLetsInMoreUsersThanStockLeft()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 1, MaxActiveSessions = 5 });

        var tickets = await sale.JoinAndAdmitAsync("a", "b");

        Assert.Equal(TicketStatus.Admitted, tickets[0].Status);
        Assert.Equal(TicketStatus.Waiting, tickets[1].Status);
    }

    [Fact]
    public async Task Admit_SkipsUsersWhoStoppedPolling()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 1 });
        var gone = (await sale.Queue.JoinAsync("gone")).Ticket!;
        var stays = (await sale.Queue.JoinAsync("stays")).Ticket!;

        sale.Time.Advance(TimeSpan.FromSeconds(16));
        await sale.Queue.GetTicketAsync(stays.TicketId);
        var result = await sale.Queue.AdmitAsync();

        Assert.Equal(new AdmitResult(Admitted: 1, Abandoned: 1, Expired: 0), result);
        Assert.Equal(TicketStatus.Abandoned, (await sale.Queue.GetTicketAsync(gone.TicketId))!.Status);
        Assert.Equal(TicketStatus.Admitted, (await sale.Queue.GetTicketAsync(stays.TicketId))!.Status);
    }

    [Fact]
    public async Task AdmissionWindow_Expires_AndFreesTheSlotForTheNextUser()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 1 });
        var tickets = await sale.JoinAndAdmitAsync("slow", "next");

        sale.Time.Advance(TimeSpan.FromSeconds(60));
        await sale.Queue.GetTicketAsync(tickets[1].TicketId);
        var expiredBeforeTick = await sale.Queue.GetTicketAsync(tickets[0].TicketId);
        var result = await sale.Queue.AdmitAsync();

        Assert.Equal(TicketStatus.Expired, expiredBeforeTick!.Status);
        Assert.Equal(new AdmitResult(Admitted: 1, Abandoned: 0, Expired: 1), result);
        Assert.Equal(TicketStatus.Admitted, (await sale.Queue.GetTicketAsync(tickets[1].TicketId))!.Status);
    }

    [Fact]
    public async Task Rejoin_AfterAdmissionExpired_GoesToTheBackOfTheLine()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 1 });
        var tickets = await sale.JoinAndAdmitAsync("slow", "next", "last");
        sale.Time.Advance(TimeSpan.FromSeconds(60));

        var rejoined = await sale.Queue.JoinAsync("slow");

        Assert.Equal(JoinOutcome.Created, rejoined.Outcome);
        Assert.NotEqual(tickets[0].TicketId, rejoined.Ticket!.TicketId);
        Assert.Equal(3, rejoined.Ticket.Position);
    }

    [Fact]
    public async Task Rejoin_AfterBeingSkippedForNotPolling_GetsANewTicket()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var first = (await sale.Queue.JoinAsync("napper")).Ticket!;
        sale.Time.Advance(TimeSpan.FromSeconds(16));
        await sale.Queue.AdmitAsync();

        var rejoined = await sale.Queue.JoinAsync("napper");

        Assert.Equal(JoinOutcome.Created, rejoined.Outcome);
        Assert.NotEqual(first.TicketId, rejoined.Ticket!.TicketId);
    }

    [Fact]
    public async Task WaitingTicket_EstimatesTheWait_OnceAdmissionsAreFlowing()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 1 });
        foreach (var user in new[] { "u0", "u1", "u2" })
        {
            await sale.Queue.JoinAsync(user);
        }
        var beforeAnyAdmission = (await sale.Queue.JoinAsync("u2")).Ticket!;

        sale.Time.Advance(TimeSpan.FromSeconds(1));
        await sale.Queue.AdmitAsync();
        var afterAdmission = (await sale.Queue.JoinAsync("u2")).Ticket!;

        Assert.Null(beforeAnyAdmission.EstimatedWaitSeconds);
        Assert.Equal(2, afterAdmission.Position);
        Assert.True(afterAdmission.EstimatedWaitSeconds > 0, $"estimate was {afterAdmission.EstimatedWaitSeconds}");
    }

    [Fact]
    public async Task WaitEstimate_MatchesASteadyAdmissionRate()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 100, MaxActiveSessions = 2 });
        var users = Enumerable.Range(0, 30).Select(i => $"u{i}").ToArray();
        foreach (var user in users)
        {
            await sale.Queue.JoinAsync(user);
        }

        // Two people check out every second, so two are let in every second.
        for (var second = 0; second < 5; second++)
        {
            await sale.Queue.AdmitAsync();
            foreach (var user in users)
            {
                var ticket = (await sale.Queue.JoinAsync(user)).Ticket!;
                if (ticket.Status == TicketStatus.Admitted)
                {
                    await sale.Orders.CheckoutAsync(ticket.TicketId, 1);
                }
            }
            sale.Time.Advance(TimeSpan.FromSeconds(1));
        }
        await sale.Queue.AdmitAsync();
        var tenthInLine = (await sale.Queue.JoinAsync(users[21])).Ticket!;

        Assert.Equal(10, tenthInLine.Position);
        Assert.Equal(5, tenthInLine.EstimatedWaitSeconds);
    }

    [Fact]
    public async Task GetTicket_ReturnsNull_ForUnknownTicket()
    {
        await using var sale = await TestSale.StartAsync(redis);

        Assert.Null(await sale.Queue.GetTicketAsync(Ids.New()));
    }
}
