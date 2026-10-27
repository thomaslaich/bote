# bote C# generator

The `smithy-csharp-codegen` artifact is an integration for
[NSmithy](https://github.com/thomaslaich/smithy-dotnet) 0.14.0. In an NSmithy
project, add it to the project's `smithy-build.json`:

```json
{
    "version": "1.0",
    "sources": ["model"],
    "maven": {
        "dependencies": ["io.github.thomaslaich.bote:smithy-csharp-codegen:<version>"]
    }
}
```

For a bote service, NSmithy then generates the channels' event types and this
messaging surface in place of an HTTP client and server:

| Side | Generated | Registration |
| --- | --- | --- |
| Owner (`generateServer`) | `I<Operation>Handler` per command, `<Channel>Publisher` per channel | `Add<Service>CommandConsumer`, `Add<Channel>Publisher` |
| Client (`generateClient`) | `<Service>Client`, `I<Channel>Handler` per channel | `Add<Service>Client`, `Add<Channel>Consumer` |

A channel handler has one method per event, plus `HandleUnknownAsync` for
events the contract does not know yet, which skips them by default.

The generated code runs on the `Bote.Kafka` or `Bote.Redis`
NuGet package:

```csharp
builder.Services
    .AddKafkaMessaging(new KafkaMessagingOptions { Producer = producer, Consumer = consumer })
    .AddOrderServiceCommandConsumer()
    .AddOrderEventsPublisher()
    .AddScoped<ISubmitOrderHandler, SubmitOrderHandler>();
```

Each consumer registration takes options of its own, such as the Kafka group
ID or the Redis Streams consumer group.

C# generation supports `@kafkaJson` and all four Redis protocols, JSON and
CBOR.
