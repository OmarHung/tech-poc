using System.Text.Json;
using StackExchange.Redis;

namespace CheckoutQueue.Http;

/// <summary>HTTP plumbing: error mapping, served-by header, strict request parsing.</summary>
public static class HttpExtensions
{
    /// <summary>Tags every response with X-Served-By so callers can tell which instance answered.</summary>
    public static IApplicationBuilder UseServedBy(this IApplicationBuilder app, string name) =>
        app.Use((context, next) =>
        {
            context.Response.Headers["X-Served-By"] = name;
            return next(context);
        });

    /// <summary>
    /// Turns <see cref="ApiException"/> into <c>{"error": "...", "code": "..."}</c>, and a Redis
    /// outage into a 503 instead of a 500, so clients know to retry rather than give up.
    /// </summary>
    public static IApplicationBuilder UseApiErrors(this IApplicationBuilder app) =>
        app.Use(async (context, next) =>
        {
            try
            {
                await next(context);
            }
            catch (ApiException ex) when (!context.Response.HasStarted)
            {
                await WriteError(context, ex.StatusCode, new ErrorBody(ex.Message, ex.Code));
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException && !context.Response.HasStarted)
            {
                context.RequestServices.GetRequiredService<ILogger<ApiException>>()
                    .LogWarning(ex, "Redis unavailable while serving {Method} {Path}", context.Request.Method, context.Request.Path);
                await WriteError(context, StatusCodes.Status503ServiceUnavailable, new ErrorBody("storage unavailable, retry shortly", "unavailable"));
            }
        });

    /// <summary>Reads a JSON body, turning every way it can be wrong into a 400.</summary>
    public static async Task<T> ReadJsonAsync<T>(this HttpRequest request, CancellationToken ct)
        where T : class
    {
        if (!request.HasJsonContentType())
        {
            throw ApiException.BadRequest("request body must be JSON");
        }
        try
        {
            return await request.ReadFromJsonAsync<T>(ct) ?? throw ApiException.BadRequest("request body is required");
        }
        catch (JsonException ex)
        {
            // ex.Message names .NET types; the JSON path is enough for the caller.
            throw ApiException.BadRequest($"invalid JSON body at {ex.Path ?? "$"}");
        }
    }

    /// <summary>Ticket and order ids are 32 lowercase hex characters; anything else is a 400, not a lookup.</summary>
    public static string ValidId(string? value, string name) =>
        Ids.IsValid(value) ? value! : throw ApiException.BadRequest($"{name} must be 32 lowercase hex characters");

    private static Task WriteError(HttpContext context, int status, ErrorBody body)
    {
        context.Response.StatusCode = status;
        return context.Response.WriteAsJsonAsync(body);
    }
}
