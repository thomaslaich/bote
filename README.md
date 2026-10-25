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
the same binding request/reply. Kafka and Redis Pub/Sub operations never have
outputs.

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
just ci              # run the complete local CI pipeline
```

The build and published artifacts require Java 21. The root Gradle project
builds the `bote` trait and validator JAR; the
`codegen/smithy-asyncapi` subproject builds the generator.

Planned Maven Central coordinates:

```text
io.github.thomaslaich.bote:bote:<version>
io.github.thomaslaich.bote:smithy-asyncapi:<version>
```

## Related projects

- **[Smithy](https://smithy.io):** the model and protocol framework bote
  extends.
- **[Alloy](https://github.com/disneystreaming/alloy):** Smithy traits used by
  the Protobuf protocol and an inspiration for bote's trait-library design.
- **[FastStream](https://faststream.ag2.ai/):** an implementation reference for
  Redis Streams request/reply transport metadata.
- **[NSmithy](https://github.com/thomaslaich/smithy-dotnet):** the planned first
  consumer of bote protocols.
