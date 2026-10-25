using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace Bote.Redis;

public static class RedisMessagingExtensions
{
    /// <summary>
    /// Registers the Redis Streams and Pub/Sub senders on one connection, each by its own type so
    /// a service can also use another broker's sender. The caller owns the connection and disposes
    /// it after the host stops.
    /// </summary>
    public static IServiceCollection AddRedisMessaging(
        this IServiceCollection services,
        IConnectionMultiplexer connection,
        RedisMessagingOptions? options = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(connection);
        options ??= new();
        services.TryAddSingleton<IRedisDriver>(new RedisDriver(connection, options.Database));
        services.TryAddSingleton<MessageProcessor>();
        services.TryAddSingleton(provider => new RedisStreamsSender(
            provider.GetRequiredService<IRedisDriver>(),
            options
        ));
        services.TryAddSingleton(provider => new RedisPubSubSender(
            provider.GetRequiredService<IRedisDriver>()
        ));
        return services;
    }

    public static IServiceCollection AddRedisStreamConsumer(
        this IServiceCollection services,
        RedisStreamConsumerOptions? options,
        params MessageReceiveBinding[] bindings
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(bindings);
        var definitions = bindings.ToArray();
        options ??= new();
        services.AddSingleton<IHostedService>(provider => new RedisStreamsConsumer(
            provider.GetRequiredService<IRedisDriver>(),
            definitions,
            provider.GetRequiredService<MessageProcessor>(),
            provider,
            options
        ));
        return services;
    }

    public static IServiceCollection AddRedisPubSubConsumer(
        this IServiceCollection services,
        RedisPubSubConsumerOptions? options,
        params MessageReceiveBinding[] bindings
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(bindings);
        var definitions = bindings.ToArray();
        options ??= new();
        services.AddSingleton<IHostedService>(provider => new RedisPubSubConsumer(
            provider.GetRequiredService<IRedisDriver>(),
            definitions,
            provider.GetRequiredService<MessageProcessor>(),
            provider,
            options
        ));
        return services;
    }
}
