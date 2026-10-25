using System.Text;
using System.Text.Json;
using Bote.Messaging;
using Microsoft.Extensions.DependencyInjection;
using NSmithy.Core.Serde;
using E = Tests.Envelope;
using H = Tests.Header;
using N = Tests.None;

namespace Bote.Messaging.Generated.Tests;

public sealed class GeneratedKafkaTests
{
    [Fact]
    public async Task CommandClientWritesKeyAndHeaderAndOwnerDecodesThem()
    {
        var sender = new CapturingSender();
        var client = new E.DeviceClient(sender);
        using var cancellation = new CancellationTokenSource();
        await client.DimAsync(
            new E.DimInput(DeviceId: "light-1", TraceId: "trace-1"),
            cancellation.Token
        );
        Assert.Equal("tests.envelope#Dim", sender.OperationId);
        Assert.Equal("envelope.commands", sender.Address);
        Assert.Equal(cancellation.Token, sender.CancellationToken);
        var payload = Assert.IsType<MessagePayload>(sender.Payload);
        Assert.Equal("light-1", payload.Key);
        Assert.Equal("trace-1", Encoding.UTF8.GetString(payload.Headers!["trace-id"]));
        using var json = JsonDocument.Parse(payload.Value);
        Assert.False(json.RootElement.TryGetProperty("traceId", out _));
        Assert.Equal("light-1", json.RootElement.GetProperty("deviceId").GetString());

        var handler = new CommandHandler();
        await using var services = new ServiceCollection()
            .AddSingleton<E.IDimHandler>(handler)
            .BuildServiceProvider();
        await new MessageProcessor(
            services.GetRequiredService<IServiceScopeFactory>()
        ).ProcessAsync(E.DeviceMessaging.DimReceive, payload);
        Assert.Equal(new E.DimInput(DeviceId: "light-1", TraceId: "trace-1"), handler.Command);
    }

    [Theory]
    [InlineData("ENVELOPE")]
    [InlineData("HEADER")]
    [InlineData("NONE")]
    public async Task PublishedEventReachesTheChannelHandler(string mode)
    {
        var sender = new CapturingSender();
        var handler = new EventHandler();
        var collection = new ServiceCollection();
        MessageReceiveBinding receive;
        switch (mode)
        {
            case "ENVELOPE":
                await new E.DeviceEventsPublisher(sender).PublishMeasuredAsync(
                    new E.Measured(DeviceId: "light-1", Lumens: 42, Source: "sensor")
                );
                receive = E.DeviceMessaging.DeviceEventsReceive;
                collection.AddSingleton<E.IDeviceEventsHandler>(handler);
                break;
            case "HEADER":
                await new H.DeviceEventsPublisher(sender).PublishMeasuredAsync(
                    new H.Measured(DeviceId: "light-1", Lumens: 42, Source: "sensor")
                );
                receive = H.DeviceMessaging.DeviceEventsReceive;
                collection.AddSingleton<H.IDeviceEventsHandler>(handler);
                break;
            default:
                await new N.DeviceEventsPublisher(sender).PublishMeasuredAsync(
                    new N.Measured(DeviceId: "light-1", Lumens: 42, Source: "sensor")
                );
                receive = N.DeviceMessaging.DeviceEventsReceive;
                collection.AddSingleton<N.IDeviceEventsHandler>(handler);
                break;
        }
        var payload = Assert.IsType<MessagePayload>(sender.Payload);
        Assert.Equal($"{mode.ToLowerInvariant()}.events", sender.Address);
        Assert.Equal("light-1", payload.Key);
        using var json = JsonDocument.Parse(payload.Value);
        var body =
            mode == "ENVELOPE" ? json.RootElement.GetProperty("measured-event") : json.RootElement;
        Assert.Equal(42, body.GetProperty("lumens").GetInt32());
        Assert.False(body.TryGetProperty("source", out _));
        Assert.Equal(mode == "HEADER", payload.Headers!.ContainsKey("bote-type"));
        if (mode == "HEADER")
            Assert.Equal("measured", Encoding.UTF8.GetString(payload.Headers["bote-type"]));
        await using var services = collection.BuildServiceProvider();
        await new MessageProcessor(
            services.GetRequiredService<IServiceScopeFactory>()
        ).ProcessAsync(receive, payload);
        Assert.Equal("sensor", handler.Source);
        Assert.Equal(42, handler.Lumens);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("{\"measured-event\":{},\"other\":{}}")]
    public async Task InvalidEnvelopeFailsBeforeTheHandler(string json)
    {
        var handler = new EventHandler();
        await using var services = new ServiceCollection()
            .AddSingleton<E.IDeviceEventsHandler>(handler)
            .BuildServiceProvider();
        await Assert.ThrowsAsync<JsonException>(() =>
            E.DeviceMessaging.DeviceEventsReceive.DispatchAsync(
                new MessagePayload(Encoding.UTF8.GetBytes(json)),
                services,
                CancellationToken.None
            )
        );
        Assert.Null(handler.Source);
    }

    [Fact]
    public async Task UnknownEventGoesToHandleUnknown()
    {
        var handler = new EventHandler();
        await using var services = new ServiceCollection()
            .AddSingleton<E.IDeviceEventsHandler>(handler)
            .BuildServiceProvider();
        await E.DeviceMessaging.DeviceEventsReceive.DispatchAsync(
            new MessagePayload("{\"future-event\":{}}"u8.ToArray()),
            services,
            CancellationToken.None
        );
        Assert.Equal("future-event", handler.Unknown);
        Assert.Null(handler.Source);
    }

    [Fact]
    public async Task MissingDiscriminatorOrRequiredHeaderFailsBeforeTheHandler()
    {
        var handler = new EventHandler();
        await using var services = new ServiceCollection()
            .AddSingleton<H.IDeviceEventsHandler>(handler)
            .BuildServiceProvider();
        await Assert.ThrowsAsync<JsonException>(() =>
            H.DeviceMessaging.DeviceEventsReceive.DispatchAsync(
                new MessagePayload("{\"deviceId\":\"light-1\",\"lumens\":42}"u8.ToArray()),
                services,
                CancellationToken.None
            )
        );
        await Assert.ThrowsAsync<MissingRequiredMemberException>(() =>
            H.DeviceMessaging.DeviceEventsReceive.DispatchAsync(
                new MessagePayload(
                    "{\"deviceId\":\"light-1\",\"lumens\":42}"u8.ToArray(),
                    Headers: new Dictionary<string, byte[]>
                    {
                        ["bote-type"] = "measured"u8.ToArray(),
                    }
                ),
                services,
                CancellationToken.None
            )
        );
        Assert.Null(handler.Source);
    }

    private sealed class CapturingSender : IMessageSender
    {
        public MessagePayload? Payload { get; private set; }
        public string? OperationId { get; private set; }
        public string? Address { get; private set; }
        public CancellationToken CancellationToken { get; private set; }

        public Task SendAsync<T>(
            MessageSendBinding<T> binding,
            T message,
            CancellationToken cancellationToken = default
        )
        {
            Payload = binding.Encode(message);
            OperationId = binding.OperationId;
            Address = binding.Address;
            CancellationToken = cancellationToken;
            return Task.CompletedTask;
        }
    }

    private sealed class CommandHandler : E.IDimHandler
    {
        public E.DimInput? Command { get; private set; }

        public Task HandleAsync(E.DimInput command, CancellationToken cancellationToken = default)
        {
            Command = command;
            return Task.CompletedTask;
        }
    }

    private sealed class EventHandler
        : E.IDeviceEventsHandler,
            H.IDeviceEventsHandler,
            N.IDeviceEventsHandler
    {
        public string? Source { get; private set; }
        public int Lumens { get; private set; }
        public string? Unknown { get; private set; }

        public Task HandleMeasuredAsync(
            E.Measured @event,
            CancellationToken cancellationToken = default
        ) => Record(@event.Source, @event.Lumens);

        public Task HandleMeasuredAsync(
            H.Measured @event,
            CancellationToken cancellationToken = default
        ) => Record(@event.Source, @event.Lumens);

        public Task HandleMeasuredAsync(
            N.Measured @event,
            CancellationToken cancellationToken = default
        ) => Record(@event.Source, @event.Lumens);

        public Task HandleUnknownAsync(string type, CancellationToken cancellationToken = default)
        {
            Unknown = type;
            return Task.CompletedTask;
        }

        private Task Record(string source, int lumens)
        {
            Source = source;
            Lumens = lumens;
            return Task.CompletedTask;
        }
    }
}
