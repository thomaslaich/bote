using Bote.Kafka;
using Confluent.Kafka;

namespace Bote.Tests;

public sealed class KafkaConsumerOptionsTests
{
    [Fact]
    public void OverridesTheSharedConfigurationWithoutChangingIt()
    {
        var shared = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "shared",
            AutoOffsetReset = AutoOffsetReset.Latest,
        };

        var config = new KafkaConsumerOptions
        {
            GroupId = "projections",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }.Apply(shared);

        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("projections", config.GroupId);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal("shared", shared.GroupId);
        Assert.Equal(AutoOffsetReset.Latest, shared.AutoOffsetReset);
    }

    [Fact]
    public void KeepsTheSharedConfigurationByDefault()
    {
        var shared = new ConsumerConfig { GroupId = "shared" };

        var config = new KafkaConsumerOptions().Apply(shared);

        Assert.Equal("shared", config.GroupId);
        Assert.Null(config.AutoOffsetReset);
    }

    [Fact]
    public void RejectsABlankGroupId() =>
        Assert.Throws<ArgumentException>(() =>
            new KafkaConsumerOptions { GroupId = " " }.Apply(new ConsumerConfig())
        );
}
