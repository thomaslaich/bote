# bote

> **Preview:** bote is exploratory and has not had a release. Expect breaking
> changes before 1.0.

**[Examples](examples/)** · **[Smithy](https://smithy.io)** ·
**[AsyncAPI](https://www.asyncapi.com/)**

bote models Kafka and Redis messaging contracts in Smithy. It provides
protocol and binding traits, build-time validators, and a reference AsyncAPI
3.1 generator.

## Protocols

| Service trait | Address trait | Encoding |
| --- | --- | --- |
| `@kafkaJson` | `@kafkaTopic` | JSON |
| `@kafkaAvro` | `@kafkaTopic` | Avro |
| `@kafkaProtobuf` | `@kafkaTopic` | Protobuf |
| `@redisStreamsJson` | `@redisStream` | JSON |
| `@redisStreamsCbor` | `@redisStream` | CBOR |
| `@redisPubSubJson` | `@redisChannel` | JSON |
| `@redisPubSubCbor` | `@redisChannel` | CBOR |

A bote service owns its contract. It receives **commands**, which are its
operations, and publishes **events** to **channels**, which are unions it lists
in `@channels`. The address trait binds both to a broker address. Where it sits
sets the direction: the owner receives commands at an operation's address and
publishes events to a channel's address. Traits name addresses, never actions,
so one contract reads the same from either side.

| Role | Marker | Position |
| --- | --- | --- |
| Command | `@input` | Input of an operation |
| Reply | `@output` | Output of a Redis Streams operation |
| Event | `@event` | Member of a channel union |

## Quick start

This service accepts a one-way command and a request/reply command, and
publishes presence changes:

```smithy
$version: "2"

namespace example.presence

use bote#channels
use bote#event
use bote#redisStream
use bote#redisStreamsJson

@redisStreamsJson
@channels([PresenceEvents])
service PresenceService {
    operations: [SetPresence, GetPresence]
}

@redisStream(key: "presence.commands")
operation SetPresence {
    input := {
        userId: String
        status: String
    }
}

@redisStream(key: "presence.requests")
operation GetPresence {
    input := {
        userId: String
    }
    output := {
        userId: String
        status: String
    }
}

@redisStream(key: "presence.events")
union PresenceEvents {
    changed: PresenceChanged
}

@event
structure PresenceChanged {
    userId: String
    status: String
}
```

With no output, a Redis Streams operation is fire-and-forget. An output makes
the same binding request/reply. Kafka operations never have outputs, and Redis
Pub/Sub services have no operations: Pub/Sub keeps no messages and delivers
each one to every subscriber, so commands use Redis Streams.

For request/reply, the requester generates `reply_to` and `correlation_id`
transport metadata, subscribes to the temporary Pub/Sub channel before sending
the request to its Redis Stream, accepts one reply with the same correlation
ID, and cleans up the subscription. Owner replicas consume the request stream
through one shared consumer group, so each delivery goes to one replica;
requests can be redelivered after failures. bote defines that interoperable
behavior and validates the static input/output contract. Channel allocation,
consumer naming, subscription lifecycle, and timeouts belong to generated
clients and runtimes. Replies are at-most-once, even though the request itself
is durable.

## Contract rules

- Every operation of a bote service carries the protocol's address trait and
  takes an `@input` command. Only Redis Streams operations may return an
  `@output` reply.
- Every channel carries the protocol's address trait and is listed by exactly
  one service. Its members target `@event` structures, which are never
  operation inputs or outputs.
- An address has one owning service and one use: one channel, or the address
  of command operations. Operations sharing an address agree on address-level
  values such as Kafka compaction and Redis `maxLen`.
- Every command carries its operation name as `bote-type`, a Kafka header or a
  Redis Streams entry field, so several commands can share an address.
- Channel union member names are the event discriminators. Kafka JSON events
  use an envelope by default, optionally a `bote-type` header or no
  discriminator for a single-event channel.
- `@kafkaHeader` members are excluded from the value; `@kafkaKey` remains in
  the value and is also used as the record key.
- Kafka Protobuf payload members require explicit `alloy.proto#protoIndex`
  values so reordering cannot silently change the wire format.

`@bote.infra#kafkaTopicConfig` declares partitions, replication, and retention.
A platform team can attach it with Smithy's `apply` without editing the
contract owner's model.

## AsyncAPI

Add the `smithy-asyncapi` artifact to the Smithy build classpath and enable the
plugin in `smithy-build.json`:

```json
{
    "version": "1.0",
    "sources": ["model"],
    "plugins": {
        "asyncapi": {}
    }
}
```

The plugin writes one `<ServiceName>.asyncapi.json` file per bote service.
`"perspective": "owner"` is the default: commands are `receive` operations and
channels are `send` operations. `"client"` reverses both. Redis Streams request/reply operations produce an AsyncAPI reply with
a dynamic `$message.header#/reply_to` address and correlation metadata.

## C#

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

C# generation supports `@kafkaJson` and all four Redis protocols, JSON and
CBOR.

## Development

The recommended environment uses [Nix](https://nixos.org/) and
[devenv](https://devenv.sh/). Install both, optionally enable
[direnv](https://direnv.net/), then use the `just` recipes:

```shell
just                 # list recipes
just build           # validate models, generate examples, and run tests
just test            # run tests
just fmt             # format Java, Nix, Markdown, and Smithy
just verify-golden   # compare generated AsyncAPI with checked-in examples
just dotnet-test     # build and test the C# runtime and generated code
just ci              # run the complete local CI pipeline
```

The build and published artifacts require Java 21 and .NET 10. The root Gradle
project builds the `bote` trait and validator JAR; `codegen/smithy-asyncapi`
and `codegen/smithy-csharp` build the generators, and `dotnet/` holds the C#
runtime and the tests for the code the C# generator emits.

Maven Central coordinates:

```text
io.github.thomaslaich.bote:bote:<version>
io.github.thomaslaich.bote:smithy-asyncapi:<version>
io.github.thomaslaich.bote:smithy-csharp-codegen:<version>
```

## Related projects

- **[Smithy](https://smithy.io):** the model and protocol framework bote
  extends.
- **[Alloy](https://github.com/disneystreaming/alloy):** Smithy traits used by
  the Protobuf protocol and an inspiration for bote's trait-library design.
- **[FastStream](https://faststream.ag2.ai/):** an implementation reference for
  Redis Streams request/reply transport metadata.
- **[NSmithy](https://github.com/thomaslaich/smithy-dotnet):** the C# generator
  bote's C# integration extends.
