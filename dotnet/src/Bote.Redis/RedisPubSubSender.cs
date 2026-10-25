using StackExchange.Redis;

namespace Bote.Redis;

public sealed class RedisPubSubSender : IMessageSender
{
    private readonly IRedisDriver driver;

    public RedisPubSubSender(IConnectionMultiplexer connection)
        : this(new RedisDriver(connection, -1)) { }

    internal RedisPubSubSender(IRedisDriver driver) => this.driver = driver;

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
        await driver
            .PublishAsync(outbound.Address, payload.Value)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }
}
