using Bote.Messaging;
using Bote.Messaging.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using P = Tests.Redispubsub;
using R = Tests.Redisreply;
using S = Tests.Redisstreams;

namespace Bote.Messaging.Generated.Tests;

public sealed class RedisIntegrationTests
{
    [RedisFact]
    public async Task GeneratedRequestReplyPublishesBeforeAcknowledgment()
    {
        await using var connection = await ConnectAsync();
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
            await UntilAsync(async () =>
                (
                    await connection
                        .GetDatabase()
                        .StreamPendingAsync(R.InventoryMessaging.GetStockReceive.Address, group)
                ).PendingMessageCount == 0
            );
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await connection
                .GetDatabase()
                .StreamDeleteConsumerGroupAsync(
                    R.InventoryMessaging.GetStockReceive.Address,
                    group
                );
        }
    }

    [RedisFact]
    public async Task FailedDeliveryIsRecoveredByAReplacementConsumer()
    {
        await using var connection = await ConnectAsync();
        var driver = new RedisDriver(connection, -1);
        var group = Guid.NewGuid().ToString("N");
        var binding = S.ChatRoomMessaging.PostMessageReceive;
        var calls = new List<string>();
        var handler = new PostHandler(calls) { Fail = true };
        await using var services = new ServiceCollection()
            .AddSingleton<S.IPostMessageHandler>(handler)
            .BuildServiceProvider();
        var processor = new MessageProcessor(services.GetRequiredService<IServiceScopeFactory>());
        var options = new RedisStreamConsumerOptions
        {
            ConsumerGroup = group,
            ConsumerName = "first",
            StartPosition = "$",
            MinIdleTime = TimeSpan.Zero,
        };
        using var first = new RedisStreamsConsumer(driver, [binding], processor, services, options);
        await first.StartAsync(CancellationToken.None);
        var payload = S.ChatRoomMessaging.PostMessageSend.Encode(
            new S.PostMessageInput(Body: "recover me", RoomId: "test", UserId: "alice")
        );
        await driver.AppendAsync(binding.Address, payload.Value, null);
        await Assert.ThrowsAsync<IOException>(() =>
            first.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5))
        );
        Assert.Equal(
            1,
            (
                await connection.GetDatabase().StreamPendingAsync(binding.Address, group)
            ).PendingMessageCount
        );

        handler.Fail = false;
        using var replacement = new RedisStreamsConsumer(
            driver,
            [binding],
            processor,
            services,
            options with
            {
                ConsumerName = "replacement",
            }
        );
        await replacement.StartAsync(CancellationToken.None);
        try
        {
            await UntilAsync(async () =>
                (
                    await connection.GetDatabase().StreamPendingAsync(binding.Address, group)
                ).PendingMessageCount == 0
            );
        }
        finally
        {
            await replacement.StopAsync(CancellationToken.None);
            await connection.GetDatabase().StreamDeleteConsumerGroupAsync(binding.Address, group);
        }
        Assert.Equal(["recover me", "recover me"], calls);
    }

    [RedisFact]
    public async Task IndependentReaderResumesFromPersistedCheckpoint()
    {
        await using var connection = await ConnectAsync();
        var address = "bote:test:" + Guid.NewGuid().ToString("N");
        var driver = new RedisDriver(connection, -1);
        var store = new RedisStreamCheckpointStore(connection, keyPrefix: address + ":checkpoint:");
        var received = new List<int>();
        var binding = new MessageReceiveBinding<int, NumberHandler>(
            "test#Service",
            "test#Read",
            address,
            payload => payload.Value[0],
            (handler, value, ct) => handler.HandleAsync(value, ct)
        );
        await using var services = new ServiceCollection()
            .AddSingleton(new NumberHandler(received))
            .BuildServiceProvider();
        var processor = new MessageProcessor(services.GetRequiredService<IServiceScopeFactory>());
        var options = new RedisStreamConsumerOptions
        {
            ReadMode = RedisStreamReadMode.Independent,
            StartPosition = "$",
            CheckpointStore = store,
        };
        using (
            var first = new RedisStreamsConsumer(driver, [binding], processor, services, options)
        )
        {
            await first.StartAsync(CancellationToken.None);
            await driver.AppendAsync(address, [1], null);
            await UntilAsync(async () =>
                await store.LoadAsync("bote", address, CancellationToken.None) is not null
            );
            await first.StopAsync(CancellationToken.None);
        }
        await driver.AppendAsync(address, [2], null);
        var latest = await driver.LatestPositionAsync(address);
        using (
            var second = new RedisStreamsConsumer(driver, [binding], processor, services, options)
        )
        {
            await second.StartAsync(CancellationToken.None);
            await UntilAsync(async () =>
                await store.LoadAsync("bote", address, CancellationToken.None) == latest
            );
            await second.StopAsync(CancellationToken.None);
        }
        Assert.Equal([1, 2], received);
        await connection.GetDatabase().KeyDeleteAsync(address);
        await connection
            .GetDatabase()
            .KeyDeleteAsync(address + ":checkpoint:bote:" + Uri.EscapeDataString(address));
    }

    [RedisFact]
    public async Task PubSubUsesGeneratedUnionHandlerAndUnsubscribesOnStop()
    {
        await using var connection = await ConnectAsync();
        var completion = new TaskCompletionSource<string?>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var collection = new ServiceCollection();
        collection.AddRedisMessaging(connection);
        P.ChatRoomMessagingExtensions.AddChatEventsPublisher(collection);
        P.ChatRoomMessagingExtensions.AddChatEventsConsumer(collection);
        collection.AddSingleton<P.IChatEventsHandler>(new EventHandler(completion));
        await using var services = collection.BuildServiceProvider();
        var consumer = Assert.Single(services.GetServices<IHostedService>());
        await consumer.StartAsync(CancellationToken.None);
        var publisher = services.GetRequiredService<P.IChatEventsPublisher>();
        await publisher.PublishMessagePostedAsync(new P.MessagePosted(Body: "hello"));
        Assert.Equal("hello", await completion.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        await consumer.StopAsync(CancellationToken.None);
        var subscribers = await connection
            .GetSubscriber()
            .PublishAsync(
                RedisChannel.Literal(P.ChatRoomMessaging.ChatEventsReceive.Address),
                "{}"
            );
        Assert.Equal(0, subscribers);
    }

    [RedisFact]
    public async Task DisposingOneSubscriptionPreservesAnotherOnTheSameConnection()
    {
        await using var connection = await ConnectAsync();
        var driver = new RedisDriver(connection, -1);
        var address = "bote:test:" + Guid.NewGuid().ToString("N");
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = await driver.SubscribeAsync(address, _ => { });
        await using var second = await driver.SubscribeAsync(address, _ => received.TrySetResult());
        await first.DisposeAsync();
        await driver.PublishAsync(address, [1]);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static Task<ConnectionMultiplexer> ConnectAsync() =>
        ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("BOTE_TEST_REDIS")!);

    private static async Task UntilAsync(Func<Task<bool>> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!await condition())
            await Task.Delay(10, timeout.Token);
    }

    private sealed class StockHandler : R.IGetStockHandler
    {
        public Task<R.GetStockOutput> HandleAsync(
            R.GetStockInput command,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(new R.GetStockOutput(Available: 42, ProductId: command.ProductId));
    }

    private sealed class PostHandler(List<string> calls) : S.IPostMessageHandler
    {
        public bool Fail { get; set; }

        public Task HandleAsync(
            S.PostMessageInput command,
            CancellationToken cancellationToken = default
        )
        {
            calls.Add(command.Body!);
            if (Fail)
                throw new IOException("failed processing");
            return Task.CompletedTask;
        }
    }

    private sealed class NumberHandler(List<int> received)
    {
        public Task HandleAsync(int value, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            received.Add(value);
            return Task.CompletedTask;
        }
    }

    private sealed class EventHandler(TaskCompletionSource<string?> completion)
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

public sealed class RedisFactAttribute : FactAttribute
{
    public RedisFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("BOTE_TEST_REDIS")))
            Skip =
                "Set BOTE_TEST_REDIS to a disposable Redis 6.2+ instance to run broker integration tests.";
    }
}
