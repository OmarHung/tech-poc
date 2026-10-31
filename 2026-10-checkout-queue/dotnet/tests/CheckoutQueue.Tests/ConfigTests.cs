namespace CheckoutQueue.Tests;

public sealed class ConfigTests
{
    [Fact]
    public void FromLookup_UsesDefaults_WhenNothingIsSet()
    {
        var config = CheckoutQueueConfig.FromLookup(_ => null);

        Assert.Equal(new CheckoutQueueConfig(), config);
    }

    [Fact]
    public void FromLookup_ReadsEveryVariable()
    {
        var env = new Dictionary<string, string>
        {
            ["PORT"] = "9001",
            ["REDIS_URL"] = "redis.local:6380",
            ["INSTANCE_NAME"] = "node-a",
            ["SALE_ID"] = "black-friday",
            ["INITIAL_STOCK"] = "500",
            ["MAX_PER_ORDER"] = "3",
            ["MAX_ACTIVE"] = "80",
            ["ADMISSION_WINDOW_MS"] = "30000",
            ["PAYMENT_WINDOW_MS"] = "60000",
            ["HEARTBEAT_TIMEOUT_MS"] = "9000",
            ["POLL_INTERVAL_MS"] = "2000",
            ["TICK_MS"] = "50",
        };

        var config = CheckoutQueueConfig.FromLookup(name => env.GetValueOrDefault(name));

        Assert.Equal(new CheckoutQueueConfig
        {
            Port = 9001,
            RedisUrl = "redis.local:6380",
            InstanceName = "node-a",
            SaleId = "black-friday",
            InitialStock = 500,
            MaxQuantityPerOrder = 3,
            MaxActiveSessions = 80,
            AdmissionWindow = TimeSpan.FromSeconds(30),
            PaymentWindow = TimeSpan.FromSeconds(60),
            HeartbeatTimeout = TimeSpan.FromSeconds(9),
            PollInterval = TimeSpan.FromSeconds(2),
            TickInterval = TimeSpan.FromMilliseconds(50),
        }, config);
    }

    [Theory]
    [InlineData("PORT", "0")]
    [InlineData("PORT", "http")]
    [InlineData("INITIAL_STOCK", "-1")]
    [InlineData("MAX_PER_ORDER", "0")]
    [InlineData("MAX_ACTIVE", "0")]
    [InlineData("ADMISSION_WINDOW_MS", "0")]
    [InlineData("PAYMENT_WINDOW_MS", "1.5")]
    [InlineData("TICK_MS", "0")]
    [InlineData("SALE_ID", "has space")]
    [InlineData("SALE_ID", "brace}breaks{the-hash-tag")]
    [InlineData("REDIS_URL", " ")]
    [InlineData("HEARTBEAT_TIMEOUT_MS", "2999")]
    public void FromLookup_RejectsInvalidValues(string name, string value)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            CheckoutQueueConfig.FromLookup(n => n == name ? value : null));

        Assert.Contains(name, ex.Message);
    }

    [Fact]
    public void Ids_AreRandom_AndValidateTheirOwnFormat()
    {
        var ids = Enumerable.Range(0, 100).Select(_ => Ids.New()).ToList();

        Assert.Equal(100, ids.Distinct().Count());
        Assert.All(ids, id => Assert.True(Ids.IsValid(id), id));
        Assert.False(Ids.IsValid(ids[0].ToUpperInvariant()));
        Assert.False(Ids.IsValid(ids[0][..31]));
        Assert.False(Ids.IsValid(null));
    }
}
