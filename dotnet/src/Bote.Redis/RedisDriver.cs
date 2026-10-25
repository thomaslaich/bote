using StackExchange.Redis;

namespace Bote.Redis;

internal sealed record RedisDelivery(
    string Id,
    byte[] Payload,
    string? ReplyTo = null,
    string? CorrelationId = null,
    string? ContentType = null,
    IReadOnlyDictionary<string, byte[]>? Headers = null
)
{
    public MessagePayload ToPayload() => new(Payload, Headers: Headers, ContentType: ContentType);
}

internal sealed record RedisClaimPage(string NextPosition, IReadOnlyList<RedisDelivery> Deliveries);

// Redis-specific state remains behind this seam; it is not a general broker API.
internal interface IRedisDriver
{
    Task CreateGroupAsync(string address, string group, string position);
    Task<IReadOnlyList<RedisDelivery>> ReadGroupAsync(
        string address,
        string group,
        string consumer,
        int count
    );
    Task<RedisClaimPage> ClaimAsync(
        string address,
        string group,
        string consumer,
        long idleMilliseconds,
        string position,
        int count
    );
    Task<IReadOnlyList<RedisDelivery>> ReadAsync(string address, string position, int count);
    Task<string> LatestPositionAsync(string address);
    Task AcknowledgeAsync(string address, string group, string id);
    Task AppendAsync(
        string address,
        byte[] payload,
        long? maxLength,
        string? contentType = null,
        string? replyTo = null,
        string? correlationId = null,
        IReadOnlyDictionary<string, byte[]>? headers = null
    );
    Task PublishAsync(string address, byte[] payload);
    Task<IAsyncDisposable> SubscribeAsync(string address, Action<byte[]> receive);
}

internal sealed class RedisDriver(IConnectionMultiplexer connection, int database) : IRedisDriver
{
    private readonly IDatabase database = connection.GetDatabase(database);
    private readonly ISubscriber subscriber = connection.GetSubscriber();

    public async Task CreateGroupAsync(string address, string group, string position)
    {
        try
        {
            await database
                .StreamCreateConsumerGroupAsync(address, group, position, createStream: true)
                .ConfigureAwait(false);
        }
        catch (RedisServerException exception)
            when (exception.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal)) { }
    }

    public async Task<IReadOnlyList<RedisDelivery>> ReadGroupAsync(
        string address,
        string group,
        string consumer,
        int count
    ) =>
        Convert(
            await database
                .StreamReadGroupAsync(address, group, consumer, ">", count: count)
                .ConfigureAwait(false)
        );

    public async Task<RedisClaimPage> ClaimAsync(
        string address,
        string group,
        string consumer,
        long idleMilliseconds,
        string position,
        int count
    )
    {
        var result = await database
            .StreamAutoClaimAsync(address, group, consumer, idleMilliseconds, position, count)
            .ConfigureAwait(false);
        return new RedisClaimPage(result.NextStartId.ToString(), Convert(result.ClaimedEntries));
    }

    public async Task<IReadOnlyList<RedisDelivery>> ReadAsync(
        string address,
        string position,
        int count
    ) => Convert(await database.StreamReadAsync(address, position, count).ConfigureAwait(false));

    public async Task<string> LatestPositionAsync(string address)
    {
        var entries = await database
            .StreamRangeAsync(address, "-", "+", 1, Order.Descending)
            .ConfigureAwait(false);
        return entries.Length == 0 ? "0-0" : entries[0].Id.ToString();
    }

    public async Task AcknowledgeAsync(string address, string group, string id) =>
        _ = await database.StreamAcknowledgeAsync(address, group, id).ConfigureAwait(false);

    public async Task AppendAsync(
        string address,
        byte[] payload,
        long? maxLength,
        string? contentType = null,
        string? replyTo = null,
        string? correlationId = null,
        IReadOnlyDictionary<string, byte[]>? headers = null
    )
    {
        var entries = new List<NameValueEntry> { new("data", payload) };
        if (headers is not null)
            foreach (var header in headers)
                if (!ReservedFields.Contains(header.Key))
                    entries.Add(new(header.Key, header.Value));
        if (contentType is not null)
            entries.Add(new("content-type", contentType));
        if (replyTo is not null)
        {
            entries.Add(new("reply_to", replyTo));
            entries.Add(new("correlation_id", correlationId));
        }
        _ = await database
            .StreamAddAsync(
                address,
                [.. entries],
                maxLength: maxLength is null ? null : checked((int)maxLength.Value),
                useApproximateMaxLength: true
            )
            .ConfigureAwait(false);
    }

    public async Task PublishAsync(string address, byte[] payload) =>
        _ = await subscriber
            .PublishAsync(RedisChannel.Literal(address), payload)
            .ConfigureAwait(false);

    public async Task<IAsyncDisposable> SubscribeAsync(string address, Action<byte[]> receive)
    {
        var queue = await subscriber
            .SubscribeAsync(RedisChannel.Literal(address))
            .ConfigureAwait(false);
        queue.OnMessage(message => receive((byte[]?)message.Message ?? []));
        return new Subscription(queue);
    }

    private sealed class Subscription(ChannelMessageQueue queue) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() =>
            await queue.UnsubscribeAsync().ConfigureAwait(false);
    }

    private static readonly HashSet<string> ReservedFields =
    [
        "data",
        "content-type",
        "reply_to",
        "correlation_id",
    ];

    private static RedisDelivery[] Convert(StreamEntry[] entries) =>
        entries
            .Select(entry =>
            {
                byte[]? payload = null;
                string? replyTo = null,
                    correlationId = null,
                    contentType = null;
                Dictionary<string, byte[]>? headers = null;
                foreach (var field in entry.Values)
                {
                    var name = field.Name.ToString();
                    switch (name)
                    {
                        case "data":
                            payload = (byte[]?)field.Value;
                            break;
                        case "reply_to":
                            replyTo = (string?)field.Value;
                            break;
                        case "correlation_id":
                            correlationId = (string?)field.Value;
                            break;
                        case "content-type":
                            contentType = (string?)field.Value;
                            break;
                        default:
                            (headers ??= new(StringComparer.Ordinal))[name] =
                                (byte[]?)field.Value ?? [];
                            break;
                    }
                }
                return new RedisDelivery(
                    entry.Id.ToString(),
                    payload
                        ?? throw new InvalidOperationException(
                            "Redis stream entry has no data field."
                        ),
                    replyTo,
                    correlationId,
                    contentType,
                    headers
                );
            })
            .ToArray();
}
