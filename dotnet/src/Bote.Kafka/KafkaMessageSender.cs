using Confluent.Kafka;

namespace Bote.Kafka;

/// <summary>Owns the producer; individual sends await Kafka's configured delivery confirmation.</summary>
public sealed class KafkaMessageSender : IMessageSender, IDisposable
{
    private readonly IProducer<string?, byte[]> producer;

    public KafkaMessageSender(ProducerConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        producer = new ProducerBuilder<string?, byte[]>(KafkaConfig.Copy(config)).Build();
    }

    public async Task SendAsync<T>(
        OutboundBinding<T> outbound,
        T message,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(outbound);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();
        var payload = outbound.Encode(message);
        var headers = new Headers();
        if (payload.Headers is { } modeledHeaders)
            foreach (var header in modeledHeaders)
                headers.Add(header.Key, header.Value);
        await producer
            .ProduceAsync(
                outbound.Address,
                new Message<string?, byte[]>
                {
                    Key = payload.Key,
                    Value = payload.Value,
                    Headers = headers,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public void Dispose() => producer.Dispose();
}
