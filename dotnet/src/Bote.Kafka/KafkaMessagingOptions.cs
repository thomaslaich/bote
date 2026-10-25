using Confluent.Kafka;

namespace Bote.Kafka;

/// <summary>Broker configuration at the composition root. Configure before registration.</summary>
public sealed class KafkaMessagingOptions
{
    public ProducerConfig Producer { get; set; } = new();
    public ConsumerConfig Consumer { get; set; } = new();
}
