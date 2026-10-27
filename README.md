# bote

> **Preview:** bote is exploratory and has not had a release. Expect breaking
> changes before 1.0.

**[Examples](examples/)** · **[Smithy](https://smithy.io)** ·
**[AsyncAPI](https://www.asyncapi.com/)**

bote is an opinionated, contract-first messaging framework for Kafka and
Redis. Commands and events are modeled in [Smithy](https://smithy.io); bote
validates the contract and generates AsyncAPI and typed code.

- **A [FastStream](https://faststream.ag2.ai/) counterpart.** FastStream
  derives the contract from Python code; bote derives code from the
  contract, for any language with a generator. Wire compatibility with
  FastStream is planned.
- **A test of Smithy's extensibility.** bote uses only Smithy's extension
  points and [NSmithy](https://github.com/thomaslaich/smithy-dotnet)'s
  integration hooks, without forking either.

Its opinions: a service owns the commands it receives and the events it
publishes, every address has one owner, and traits name addresses, never
actions, so one contract serves both sides.

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

A bote service receives **commands**, which are its operations, and publishes
**events** to **channels**, which are unions it lists in `@channels`. The
address trait binds both to a broker address. Where it sits sets the
direction: the owner receives commands at an operation's address and publishes
events to a channel's address. A channel is broker-neutral: its
address trait makes it a Kafka topic, a Redis stream, or a Redis Pub/Sub
channel.

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

On Redis, this contract uses three streams: `presence.commands`,
`presence.requests`, and `presence.events`. `PresenceEvents` is a channel
stored in a stream; `key` is the Redis key of the stream. Replies to
`GetPresence` travel on temporary Pub/Sub channels the requester names.

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
- **[FastStream](https://faststream.ag2.ai/):** the code-first Python
  counterpart, and the implementation reference for Redis Streams
  request/reply transport metadata.
- **[NSmithy](https://github.com/thomaslaich/smithy-dotnet):** the C# generator
  bote's C# integration extends.
