namespace Worker.Tests;

public sealed class WorkerConfigTests
{
    [Fact]
    public void Defaults_ApplyWhenNothingIsSet()
    {
        var config = WorkerConfig.FromLookup(_ => null);

        Assert.Equal(WorkerMode.HotHot, config.Mode);
        Assert.Equal("data/worker-a", config.DataDir);
        Assert.Equal(StartFrom.Checkpoint, config.StartFrom);
        Assert.True(config.Deterministic);
        Assert.True(config.LeaseCheck);
    }

    [Fact]
    public void Settings_AreReadFromTheEnvironment()
    {
        var env = new Dictionary<string, string>
        {
            ["WORKER_ID"] = "b",
            ["MODE"] = "dispatch",
            ["START_FROM"] = "latest",
            ["DETERMINISTIC"] = "false",
            ["LEASE_CHECK"] = "off",
            ["WORK_MS"] = "50",
            ["UPSTREAM_URL"] = "http://127.0.0.1:9120",
        };

        var config = WorkerConfig.FromLookup(env.GetValueOrDefault);

        Assert.Equal(("b", "data/worker-b"), (config.WorkerId, config.DataDir));
        Assert.Equal(WorkerMode.Dispatch, config.Mode);
        Assert.Equal(StartFrom.Latest, config.StartFrom);
        Assert.False(config.Deterministic);
        Assert.False(config.LeaseCheck);
        Assert.Equal(TimeSpan.FromMilliseconds(50), config.WorkTime);
        Assert.Equal(new Uri("http://127.0.0.1:9120"), config.UpstreamUrl);
    }

    [Theory]
    [InlineData("WORKER_ID", "Worker A")]
    [InlineData("MODE", "both")]
    [InlineData("START_FROM", "beginning")]
    [InlineData("LEASE_CHECK", "yes")]
    public void BadSettings_NameTheVariable(string name, string value)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => WorkerConfig.FromLookup(n => n == name ? value : null));

        Assert.Contains(name, ex.Message);
    }
}
