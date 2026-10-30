// Strangler Fig facade: one entry point whose catalog route can be switched at runtime.
using System.Net;
using System.Text.Json;
using Gateway;
using Yarp.ReverseProxy.Forwarder;

const string ServedByHeader = "X-Served-By";

var config = GatewayConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning).AddFilter("Yarp", LogLevel.Warning);
builder.Services.AddHttpForwarder();
builder.Services.AddSingleton(new RoutingTable(config.Upstreams, config.InitialRoute));

var app = builder.Build();

var upstreamClient = new HttpMessageInvoker(new SocketsHttpHandler
{
    UseProxy = false,
    AllowAutoRedirect = false,
    AutomaticDecompression = DecompressionMethods.None,
    UseCookies = false,
});
var forwarding = new ForwarderRequestConfig { ActivityTimeout = GatewayConfig.UpstreamTimeout };

// Gateway-owned responses are tagged so callers can tell them from proxied ones.
var own = app.MapGroup("").AddEndpointFilter(async (context, next) =>
{
    context.HttpContext.Response.Headers[ServedByHeader] = "gateway";
    return await next(context);
});

own.MapGet("/health", (RoutingTable routing) =>
    new { status = "ok", service = "gateway", routes = new RoutesBody(routing.CatalogRoute.ToWireName()) });

own.MapGet("/admin/routes", (RoutingTable routing) => new RoutesBody(routing.CatalogRoute.ToWireName()));

// No auth: the PoC binds to 127.0.0.1 only. A real gateway must protect this.
own.MapPut("/admin/routes", async (HttpRequest request, RoutingTable routing) =>
{
    RouteUpdate? update;
    try
    {
        update = await request.ReadFromJsonAsync<RouteUpdate>();
    }
    catch (Exception ex) when (ex is JsonException or InvalidOperationException)
    {
        return Results.BadRequest(new GatewayError("request body must be valid JSON"));
    }
    if (!CatalogRoutes.TryParse(update?.Catalog, out var route))
    {
        return Results.BadRequest(new GatewayError($"catalog must be 'monolith' or 'service', got '{update?.Catalog}'"));
    }
    routing.Switch(route);
    return Results.Ok(new RoutesBody(route.ToWireName()));
});

app.Map("/{**path}", async (HttpContext context, IHttpForwarder forwarder, RoutingTable routing, ILogger<RoutingTable> logger) =>
{
    var upstream = routing.Resolve(context.Request.Path);
    var error = await forwarder.SendAsync(context, upstream, upstreamClient, forwarding, HttpTransformer.Default);
    if (error == ForwarderError.None)
    {
        return;
    }
    logger.LogWarning(context.GetForwarderErrorFeature()?.Exception, "upstream {Upstream} failed: {Error}", upstream, error);
    if (!context.Response.HasStarted)
    {
        context.Response.StatusCode = StatusCodes.Status502BadGateway;
        context.Response.Headers[ServedByHeader] = "gateway";
        await context.Response.WriteAsJsonAsync(new GatewayError("upstream unavailable"));
    }
});

app.Run();
