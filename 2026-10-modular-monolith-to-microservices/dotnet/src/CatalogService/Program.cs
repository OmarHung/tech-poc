// The catalog module extracted into its own process. Same routes, same data model.
using CatalogService.Catalog;

var rawPort = Environment.GetEnvironmentVariable("PORT") ?? "8082";
if (!int.TryParse(rawPort, out var port) || port is < 1 or > 65535)
{
    throw new InvalidOperationException($"PORT must be between 1 and 65535, got '{rawPort}'");
}

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Services.AddStrictJson();
builder.Services.AddSingleton<ICatalog>(InMemoryCatalog.Seeded());

var app = builder.Build();
app.UseServedBy("catalog-service");
app.UseApiErrors();

app.MapGet("/health", () => new { status = "ok", service = "catalog-service" });
app.MapCatalogEndpoints();

app.Run();
