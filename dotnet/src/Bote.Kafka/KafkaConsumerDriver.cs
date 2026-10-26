using Confluent.Kafka;

namespace Bote.Kafka;

// This seam retains native Kafka positions. It is intentionally not a universal broker interface.
internal interface IKafkaConsumerDriver : IDisposable
{
    void Subscribe(IEnumerable<string> topics);
    ConsumeResult<string?, byte[]> Consume(CancellationToken cancellationToken);
    void StoreOffset(ConsumeResult<string?, byte[]> delivery);
    void Close();
}

internal sealed class KafkaConsumerDriver : IKafkaConsumerDriver
{
    private readonly IConsumer<string?, byte[]> consumer;

    public KafkaConsumerDriver(ConsumerConfig config)
    {
        var copy = KafkaConfig.Copy(config);
        copy.EnableAutoCommit = true;
        copy.EnableAutoOffsetStore = false;
        consumer = new ConsumerBuilder<string?, byte[]>(copy).Build();
    }

    public void Subscribe(IEnumerable<string> topics) => consumer.Subscribe(topics);

    public ConsumeResult<string?, byte[]> Consume(CancellationToken cancellationToken) =>
        consumer.Consume(cancellationToken);

    public void StoreOffset(ConsumeResult<string?, byte[]> delivery) =>
        consumer.StoreOffset(delivery);

    public void Close() => consumer.Close();

    public void Dispose() => consumer.Dispose();
}
