using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace CatalogService.Catalog;

/// <summary>HTTP plumbing shared by every host of the catalog module.</summary>
public static class HostingExtensions
{
    /// <summary>
    /// ASP.NET's web defaults accept numbers sent as strings ("2" becomes 2).
    /// The contract says that is a 400, the same as in the Rust and Python versions.
    /// </summary>
    public static IServiceCollection AddStrictJson(this IServiceCollection services) =>
        services.ConfigureHttpJsonOptions(options => options.SerializerOptions.NumberHandling = JsonNumberHandling.Strict);

    /// <summary>Tags every response with X-Served-By so callers can tell who answered.</summary>
    public static IApplicationBuilder UseServedBy(this IApplicationBuilder app, string name) =>
        app.Use((context, next) =>
        {
            context.Response.Headers["X-Served-By"] = name;
            return next(context);
        });

    /// <summary>Turns <see cref="ApiException"/> into <c>{"error": "..."}</c> with its status code.</summary>
    public static IApplicationBuilder UseApiErrors(this IApplicationBuilder app) =>
        app.Use(async (context, next) =>
        {
            try
            {
                await next(context);
            }
            catch (ApiException ex) when (!context.Response.HasStarted)
            {
                context.Response.StatusCode = ex.StatusCode;
                await context.Response.WriteAsJsonAsync(new ErrorBody(ex.Message));
            }
        });

    /// <summary>Reads a JSON body, turning every way it can be wrong into a 400.</summary>
    public static async Task<T> ReadJsonAsync<T>(this HttpRequest request, CancellationToken ct)
        where T : class
    {
        if (!request.HasJsonContentType())
        {
            throw new InvalidInputException("request body must be JSON");
        }
        try
        {
            return await request.ReadFromJsonAsync<T>(ct) ?? throw new InvalidInputException("request body is required");
        }
        catch (JsonException ex)
        {
            // ex.Message names .NET types; the JSON path is enough for the caller.
            throw new InvalidInputException($"invalid JSON body at {ex.Path ?? "$"}");
        }
    }
}
