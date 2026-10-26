using System.Formats.Cbor;
using Bote.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using P = Tests.Redispubsubcbor;
using R = Tests.Redisreplycbor;
using S = Tests.Redisstreamscbor;

namespace Bote.Generated.Tests;

public sealed class RedisCborTests
{
    [Fact]
    public async Task EventIsACborEnvelopeThatReachesTheChannelHandler()
    {
        var sender = new CapturingSender();
        await new S.ChatEventsPublisher(sender).PublishMessagePostedAsync(
            new S.MessagePosted(Body: "hello", RoomId: "r-1")
        );

        var payload = Assert.IsType<MessagePayload>(sender.Payload);
        Assert.Equal("application/cbor", payload.ContentType);
        var cbor = new CborReader(payload.Value);
        Assert.Equal(1, cbor.ReadStartMap());
        Assert.Equal("messagePosted", cbor.ReadTextString());

        var handler = new ChatHandler();
        await using var services = new ServiceCollection()
            .AddSingleton<S.IChatEventsHandler>(handler)
            .BuildServiceProvider();
        await S.ChatRoomMessaging.ChatEventsInbound.DispatchAsync(
            payload,
            services,
            CancellationToken.None
        );
        Assert.Equal("hello", handler.Body);
    }

    [RedisFact]
    public async Task CborRequestReplyRoundTrips()
    {
        await using var connection = await RedisIntegrationTests.ConnectAsync();
        var group = Guid.NewGuid().ToString("N");
        var collection = new ServiceCollection();
        collection.AddRedisMessaging(
            connection,
            new() { RequestTimeout = TimeSpan.FromSeconds(5) }
        );
        R.InventoryMessagingExtensions.AddInventoryClient(collection);
        R.InventoryMessagingExtensions.AddInventoryCommandConsumer(
            collection,
            new() { ConsumerGroup = group, StartPosition = "$" }
        );
        collection.AddScoped<R.IGetStockHandler, StockHandler>();
        await using var services = collection.BuildServiceProvider();
        var consumer = Assert.Single(services.GetServices<IHostedService>());
        await consumer.StartAsync(CancellationToken.None);
        try
        {
            var reply = await services
                .GetRequiredService<R.IInventoryClient>()
                .GetStockAsync(new R.GetStockInput(ProductId: "coffee"));
            Assert.Equal("coffee", reply.ProductId);
            Assert.Equal(42, reply.Available);

            var address = R.InventoryMessaging.GetStockOutbound.Address;
            var entry = Assert.Single(
                await connection
                    .GetDatabase()
                    .StreamRangeAsync(address, "-", "+", 1, StackExchange.Redis.Order.Descending)
            );
            Assert.Equal(
                "application/cbor",
                (string?)entry.Values.Single(field => field.Name == "content-type").Value
            );
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await connection
                .GetDatabase()
                .StreamDeleteConsumerGroupAsync(
                    R.InventoryMessaging.GetStockOutbound.Address,
                    group
                );
        }
    }

    [RedisFact]
    public async Task CborPubSubEventReachesTheSubscriber()
    {
        await using var connection = await RedisIntegrationTests.ConnectAsync();
        var completion = new TaskCompletionSource<string?>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var collection = new ServiceCollection();
        collection.AddRedisMessaging(connection);
        P.ChatRoomMessagingExtensions.AddChatEventsPublisher(collection);
        P.ChatRoomMessagingExtensions.AddChatEventsConsumer(collection);
        collection.AddSingleton<P.IChatEventsHandler>(new PubSubHandler(completion));
        await using var services = collection.BuildServiceProvider();
        var consumer = Assert.Single(services.GetServices<IHostedService>());
        await consumer.StartAsync(CancellationToken.None);
        try
        {
            await services
                .GetRequiredService<P.IChatEventsPublisher>()
                .PublishMessagePostedAsync(new P.MessagePosted(Body: "hello"));
            Assert.Equal("hello", await completion.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }
    }

    private sealed class CapturingSender : IMessageSender
    {
        public MessagePayload? Payload { get; private set; }

        public Task SendAsync<T>(
            OutboundBinding<T> outbound,
            T message,
            CancellationToken cancellationToken = default
        )
        {
            Payload = outbound.Encode(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ChatHandler : S.IChatEventsHandler
    {
        public string? Body { get; private set; }

        public Task HandleMessagePostedAsync(
            S.MessagePosted @event,
            CancellationToken cancellationToken = default
        )
        {
            Body = @event.Body;
            return Task.CompletedTask;
        }
    }

    private sealed class StockHandler : R.IGetStockHandler
    {
        public Task<R.GetStockOutput> HandleAsync(
            R.GetStockInput command,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(new R.GetStockOutput(Available: 42, ProductId: command.ProductId));
    }

    private sealed class PubSubHandler(TaskCompletionSource<string?> completion)
        : P.IChatEventsHandler
    {
        public Task HandleMessagePostedAsync(
            P.MessagePosted @event,
            CancellationToken cancellationToken = default
        )
        {
            completion.TrySetResult(@event.Body);
            return Task.CompletedTask;
        }
    }
}
