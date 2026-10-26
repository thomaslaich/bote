using Confluent.Kafka;

namespace Bote.Kafka;

/// <summary>Broker configuration at the composition root. Configure before registration.</summary>
public sealed class KafkaMessagingOptions
{
    public ProducerConfig Producer { get; set; } = new();
    public ConsumerConfig Consumer { get; set; } = new();
}

/// <summary>
/// Settings of one consumer that override the shared <see cref="KafkaMessagingOptions.Consumer"/>
/// configuration, so consumers of different addresses can use their own groups.
/// </summary>
public sealed record KafkaConsumerOptions
{
    public string? GroupId { get; init; }

    /// <summary>Where the group starts when it has no committed offset.</summary>
    public AutoOffsetReset? AutoOffsetReset { get; init; }

    internal ConsumerConfig Apply(ConsumerConfig shared)
    {
        var config = KafkaConfig.Copy(shared);
        if (GroupId is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(GroupId);
            config.GroupId = GroupId;
        }
        if (AutoOffsetReset is { } reset)
            config.AutoOffsetReset = reset;
        return config;
    }
}
