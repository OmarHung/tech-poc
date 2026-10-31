using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using StackExchange.Redis;

namespace CheckoutQueue.Tests;

/// <summary>Goes through the real middleware: strict JSON, error mapping, served-by header, wire names.</summary>
[Collection(RedisCollection.Name)]
public sealed class ApiTests(RedisFixture redis)
{
    private static readonly string UnknownId = new('a', 32);

    [Fact]
    public async Task Health_NamesTheInstance()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);

        var response = await app.Client.GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("test-instance", response.Headers.GetValues("X-Served-By").Single());
        Assert.Equal("ok", (await ReadJson(response)).GetProperty("status").GetString());
    }

    [Fact]
    public async Task FullFlow_JoinWaitCheckoutPay()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);

        var join = await app.Client.PostAsJsonAsync("/queue/join", new { userId = "alice" });
        var ticket = await ReadJson(join);
        var ticketId = ticket.GetProperty("ticketId").GetString()!;
        Assert.Equal(HttpStatusCode.Created, join.StatusCode);
        Assert.Equal($"/queue/tickets/{ticketId}", join.Headers.Location!.ToString());
        Assert.Equal("waiting", ticket.GetProperty("status").GetString());
        Assert.Equal(1, ticket.GetProperty("position").GetInt32());
        Assert.False(ticket.TryGetProperty("orderId", out _), "null fields are left out");

        await app.Maintenance.RunOnceAsync();
        var admitted = await ReadJson(await app.Client.GetAsync($"/queue/tickets/{ticketId}"));
        Assert.Equal("admitted", admitted.GetProperty("status").GetString());

        var checkout = await app.Client.PostAsJsonAsync("/checkout", new { ticketId, quantity = 2 });
        var order = await ReadJson(checkout);
        var orderId = order.GetProperty("orderId").GetString()!;
        Assert.Equal(HttpStatusCode.Created, checkout.StatusCode);
        Assert.Equal("pending_payment", order.GetProperty("status").GetString());

        var repeat = await app.Client.PostAsJsonAsync("/checkout", new { ticketId, quantity = 2 });
        Assert.Equal(HttpStatusCode.OK, repeat.StatusCode);
        Assert.Equal(orderId, (await ReadJson(repeat)).GetProperty("orderId").GetString());

        var pay = await app.Client.PostAsync($"/orders/{orderId}/pay", null);
        var payAgain = await app.Client.PostAsync($"/orders/{orderId}/pay", null);
        Assert.Equal(HttpStatusCode.Created, pay.StatusCode);
        Assert.Equal(HttpStatusCode.OK, payAgain.StatusCode);
        Assert.Equal("paid", (await ReadJson(await app.Client.GetAsync($"/orders/{orderId}"))).GetProperty("status").GetString());

        var audit = await ReadJson(await app.Client.GetAsync("/admin/audit"));
        Assert.Equal(8, audit.GetProperty("stock").GetInt32());
        Assert.Equal(0, audit.GetProperty("oversold").GetInt32());
        Assert.Equal(1, audit.GetProperty("charges").GetInt32());
        Assert.True(audit.GetProperty("stockBalanced").GetBoolean());
    }

    [Theory]
    [InlineData("POST", "/queue/join", """{"userId":""}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", """{"userId":"has space"}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", """{"userId":"alice\n"}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", """{"userId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", """{"userId":42}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", "{}", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", "{not json", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/queue/join", null, HttpStatusCode.BadRequest)]
    [InlineData("GET", "/queue/tickets/not-an-id", null, HttpStatusCode.BadRequest)]
    [InlineData("GET", "/queue/tickets/{unknown}", null, HttpStatusCode.NotFound)]
    [InlineData("GET", "/queue/tickets/{unknown}/events", null, HttpStatusCode.NotFound)]
    [InlineData("GET", "/queue/tickets/not-an-id/events", null, HttpStatusCode.BadRequest)]
    [InlineData("POST", "/checkout", """{"ticketId":"{unknown}","quantity":"1"}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/checkout", """{"ticketId":"{unknown}","quantity":0}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/checkout", """{"ticketId":"{unknown}","quantity":3}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/checkout", """{"ticketId":"{unknown}"}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/checkout", """{"ticketId":"ABC","quantity":1}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/checkout", """{"ticketId":"{unknown}","quantity":1}""", HttpStatusCode.NotFound)]
    [InlineData("GET", "/orders/not-an-id", null, HttpStatusCode.BadRequest)]
    [InlineData("GET", "/orders/{unknown}", null, HttpStatusCode.NotFound)]
    [InlineData("POST", "/orders/{unknown}/pay", null, HttpStatusCode.NotFound)]
    public async Task Errors_UseStatusCode_AndErrorBody(string method, string path, string? body, HttpStatusCode expected)
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        using var request = new HttpRequestMessage(new HttpMethod(method), path.Replace("{unknown}", UnknownId));
        if (body is not null)
        {
            request.Content = new StringContent(body.Replace("{unknown}", UnknownId), Encoding.UTF8, "application/json");
        }

        var response = await app.Client.SendAsync(request);

        await AssertError(response, expected);
    }

    [Fact]
    public async Task Checkout_BeforeAdmission_Is403()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        var ticketId = await JoinAsync(app, "eager");

        var response = await app.Client.PostAsJsonAsync("/checkout", new { ticketId, quantity = 1 });

        Assert.Equal("not_admitted", await AssertError(response, HttpStatusCode.Forbidden));
    }

    [Fact]
    public async Task Checkout_AfterTheAdmissionWindow_Is410()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        var ticketId = await JoinAsync(app, "slow");
        await app.Maintenance.RunOnceAsync();
        app.Time.Advance(TimeSpan.FromSeconds(60));

        var response = await app.Client.PostAsJsonAsync("/checkout", new { ticketId, quantity = 1 });

        Assert.Equal("admission_expired", await AssertError(response, HttpStatusCode.Gone));
    }

    [Fact]
    public async Task Checkout_Conflicts_AreReportedWithTheirCode()
    {
        await using var app = await TestApp.StartAsync(redis.Connection, c => c with { InitialStock = 3 });
        var first = await JoinAsync(app, "first");
        var second = await JoinAsync(app, "second");
        await app.Maintenance.RunOnceAsync();
        await app.Client.PostAsJsonAsync("/checkout", new { ticketId = first, quantity = 2 });

        var changedMind = await app.Client.PostAsJsonAsync("/checkout", new { ticketId = first, quantity = 1 });
        var tooMany = await app.Client.PostAsJsonAsync("/checkout", new { ticketId = second, quantity = 2 });

        Assert.Equal("ticket_used", await AssertError(changedMind, HttpStatusCode.Conflict));
        Assert.Equal("insufficient_stock", await AssertError(tooMany, HttpStatusCode.Conflict));
    }

    [Fact]
    public async Task Checkout_WhenSoldOut_Is409()
    {
        await using var app = await TestApp.StartAsync(redis.Connection, c => c with { InitialStock = 2 });
        var first = await JoinAsync(app, "first");
        var second = await JoinAsync(app, "second");
        await app.Maintenance.RunOnceAsync();
        var order = await ReadJson(await app.Client.PostAsJsonAsync("/checkout", new { ticketId = first, quantity = 2 }));
        await app.Client.PostAsync($"/orders/{order.GetProperty("orderId").GetString()}/pay", null);

        var response = await app.Client.PostAsJsonAsync("/checkout", new { ticketId = second, quantity = 1 });

        Assert.Equal("sold_out", await AssertError(response, HttpStatusCode.Conflict));
    }

    [Fact]
    public async Task Join_WhenSoldOut_Is409()
    {
        await using var app = await TestApp.StartAsync(redis.Connection, c => c with { InitialStock = 0 });

        var response = await app.Client.PostAsJsonAsync("/queue/join", new { userId = "late" });

        Assert.Equal("sold_out", await AssertError(response, HttpStatusCode.Conflict));
    }

    [Fact]
    public async Task Pay_AfterTheDeadline_Is410()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);
        var ticketId = await JoinAsync(app, "late-payer");
        await app.Maintenance.RunOnceAsync();
        var order = await ReadJson(await app.Client.PostAsJsonAsync("/checkout", new { ticketId, quantity = 1 }));
        app.Time.Advance(TimeSpan.FromSeconds(120));

        var response = await app.Client.PostAsync($"/orders/{order.GetProperty("orderId").GetString()}/pay", null);

        Assert.Equal("order_expired", await AssertError(response, HttpStatusCode.Gone));
    }

    [Fact]
    public async Task RedisDown_Is503_NotACrash()
    {
        var options = ConfigurationOptions.Parse("127.0.0.1:1");
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 200;
        await using var deadRedis = await ConnectionMultiplexer.ConnectAsync(options);
        await using var app = await TestApp.StartAsync(deadRedis, initializeSale: false);

        var health = await app.Client.GetAsync("/health");
        var join = await app.Client.PostAsJsonAsync("/queue/join", new { userId = "alice" });

        Assert.Equal(HttpStatusCode.ServiceUnavailable, health.StatusCode);
        Assert.Equal("unavailable", await AssertError(join, HttpStatusCode.ServiceUnavailable));
    }

    private static async Task<string> JoinAsync(TestApp app, string userId)
    {
        var response = await app.Client.PostAsJsonAsync("/queue/join", new { userId });
        response.EnsureSuccessStatusCode();
        return (await ReadJson(response)).GetProperty("ticketId").GetString()!;
    }

    private static async Task<JsonElement> ReadJson(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

    /// <summary>Asserts the status and the <c>{"error": ..., "code": ...}</c> shape; returns the code.</summary>
    private static async Task<string> AssertError(HttpResponseMessage response, HttpStatusCode expected)
    {
        var raw = await response.Content.ReadAsStringAsync();
        Assert.True(expected == response.StatusCode, $"expected {expected}, got {response.StatusCode}: {raw}");
        var body = JsonDocument.Parse(raw).RootElement;
        Assert.False(string.IsNullOrWhiteSpace(body.GetProperty("error").GetString()));
        return body.GetProperty("code").GetString()!;
    }
}
