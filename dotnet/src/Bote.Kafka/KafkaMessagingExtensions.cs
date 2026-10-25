using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bote.Kafka;

public static class KafkaMessagingExtensions
{
    public static IServiceCollection AddKafkaMessaging(
        this IServiceCollection services,
        KafkaMessagingOptions options
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        var producer = new ProducerConfig(options.Producer);
        var consumer = new ConsumerConfig(options.Consumer);
        services.AddLogging();
        services.AddSingleton(consumer);
        services.TryAddSingleton<MessageProcessor>();
        // Registered by its own type, so a service can also use another broker's sender.
        services.TryAddSingleton(_ => new KafkaMessageSender(producer));
        return services;
    }

    /// <summary>Connects generated operation definitions to a runtime-owned hosted consumer.</summary>
    public static IServiceCollection AddKafkaMessageConsumer(
        this IServiceCollection services,
        params InboundBinding[] bindings
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(bindings);
        var definitions = bindings.ToArray();
        services.AddSingleton<IHostedService>(provider => new KafkaMessageConsumer(
            provider.GetRequiredService<ConsumerConfig>(),
            definitions,
            provider.GetRequiredService<MessageProcessor>(),
            provider,
            provider.GetRequiredService<ILogger<KafkaMessageConsumer>>()
        ));
        return services;
    }
}
