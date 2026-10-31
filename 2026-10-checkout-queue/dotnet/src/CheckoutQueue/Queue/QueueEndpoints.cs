using System.Text.RegularExpressions;
using CheckoutQueue.Http;

namespace CheckoutQueue.Queue;

public sealed record JoinRequest(string? UserId);

public static partial class QueueEndpoints
{
    public static IEndpointRouteBuilder MapQueueEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/queue/join", async (HttpRequest request, QueueStore queue, CancellationToken ct) =>
        {
            var body = await request.ReadJsonAsync<JoinRequest>(ct);
            var result = await queue.JoinAsync(ValidUserId(body.UserId), ct);
            return result.Outcome switch
            {
                JoinOutcome.Created => Results.Created($"/queue/tickets/{result.Ticket!.TicketId}", result.Ticket),
                JoinOutcome.Existing => Results.Ok(result.Ticket),
                _ => throw ApiException.Conflict("sold_out", "sold out"),
            };
        });

        app.MapGet("/queue/tickets/{ticketId}", async (string ticketId, QueueStore queue, CancellationToken ct) =>
        {
            var ticket = await queue.GetTicketAsync(HttpExtensions.ValidId(ticketId, "ticket id"), ct);
            return ticket is null ? throw ApiException.NotFound("ticket_not_found", "ticket not found") : Results.Ok(ticket);
        });

        return app;
    }

    /// <summary>
    /// Stands in for the authenticated user. Restricted to characters that cannot reshape a Redis key.
    /// </summary>
    private static string ValidUserId(string? userId) =>
        userId is not null && UserIdPattern().IsMatch(userId)
            ? userId
            : throw ApiException.BadRequest("userId must be 1-64 letters, digits, '.', '_', '-' or '@'");

    [GeneratedRegex("^[A-Za-z0-9._@-]{1,64}$")]
    private static partial Regex UserIdPattern();
}
