using Confluent.Kafka;

namespace Bote.Kafka;

// Confluent's copy constructors share the original's properties, so changes would leak into it.
internal static class KafkaConfig
{
    public static ConsumerConfig Copy(ConsumerConfig config) =>
        new(config.ToDictionary(property => property.Key, property => property.Value));

    public static ProducerConfig Copy(ProducerConfig config) =>
        new(config.ToDictionary(property => property.Key, property => property.Value));
}
