using System.Net;
using Shared;
using Yarp.ReverseProxy.Forwarder;

namespace Gateway;

public static class GatewayHosting
{
    /// <param name="upstream">How to reach the nodes. Tests pass an in-process handler.</param>
    public static IServiceCollection AddGateway(this IServiceCollection services, GatewayConfig config, HttpMessageHandler? upstream = null)
    {
        services.AddHttpForwarder();
        services.AddSingleton(config);
        services.AddSingleton(new HttpMessageInvoker(upstream ?? new SocketsHttpHandler
        {
            UseProxy = false,
            AllowAutoRedirect = false,
            AutomaticDecompression = DecompressionMethods.None,
            UseCookies = false,
        }));
        services.AddSingleton<PrimaryTracker>();
        services.AddHostedService(sp => sp.GetRequiredService<PrimaryTracker>());
        return services;
    }

    public static WebApplication MapGateway(this WebApplication app)
    {
        var upstream = app.Services.GetRequiredService<HttpMessageInvoker>();
        var forwarding = new ForwarderRequestConfig { ActivityTimeout = GatewayConfig.UpstreamTimeout };

        // Gateway-owned responses are tagged so callers can tell them from forwarded ones.
        var own = app.MapGroup("").AddEndpointFilter(async (context, next) =>
        {
            context.HttpContext.Response.Headers[HostingExtensions.ServedByHeader] = "gateway";
            return await next(context);
        });
        own.MapGet("/health", (PrimaryTracker tracker) =>
            new { status = "ok", service = "gateway", primary = tracker.Current?.Target.Id, epoch = tracker.Current?.Epoch });

        app.Map("/{**path}", async (HttpContext context, IHttpForwarder forwarder, PrimaryTracker tracker, ILogger<PrimaryTracker> logger) =>
        {
            var primary = tracker.Current;
            if (primary is null)
            {
                await RespondAsync(context, StatusCodes.Status503ServiceUnavailable, "no primary available");
                return;
            }
            var error = await forwarder.SendAsync(context, primary.Target.Url.ToString(), upstream, forwarding, HttpTransformer.Default);
            if (error == ForwarderError.None)
            {
                if (context.Response.StatusCode == StatusCodes.Status503ServiceUnavailable)
                {
                    tracker.Nudge(); // the node says it is not primary (any more)
                }
                return;
            }
            tracker.Nudge();
            logger.LogDebug(context.GetForwarderErrorFeature()?.Exception, "Forwarding to {Node} failed: {Error}", primary.Target.Id, error);
            if (!context.Response.HasStarted)
            {
                await RespondAsync(context, StatusCodes.Status502BadGateway, "primary unavailable");
            }
        });
        return app;
    }

    private static Task RespondAsync(HttpContext context, int status, string error)
    {
        context.Response.StatusCode = status;
        context.Response.Headers[HostingExtensions.ServedByHeader] = "gateway";
        return context.Response.WriteAsJsonAsync(new ErrorBody(error));
    }
}
