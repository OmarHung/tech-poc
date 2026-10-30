using Monolith;
using Monolith.Modules.Catalog;
using Monolith.Modules.Orders;

var config = MonolithConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Services.AddStrictJson();

// Catalog routes and orders both resolve the same ICatalog, so in remote mode the
// monolith delegates everything catalog-related to CatalogService and never holds
// a second copy of the data.
builder.Services.AddSingleton<ICatalog>(services => config.CatalogMode == CatalogMode.Remote
    ? HttpCatalogClient.Create(config.CatalogUrl, services.GetRequiredService<ILogger<HttpCatalogClient>>())
    : InMemoryCatalog.Seeded());
builder.Services.AddSingleton<OrderService>();

var app = builder.Build();
app.UseServedBy("monolith");
app.UseApiErrors();

app.MapGet("/health", () => new { status = "ok", service = "monolith", catalogMode = config.CatalogMode.ToWireName() });
app.MapCatalogEndpoints();
app.MapOrderEndpoints();

app.Run();
