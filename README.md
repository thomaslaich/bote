# bote

> **Preview:** bote is exploratory and has not had a release. Expect breaking
> changes before 1.0.

**[Examples](examples/)** · **[Smithy](https://smithy.io)** ·
**[AsyncAPI](https://www.asyncapi.com/)**

bote models Kafka and Redis messaging contracts in Smithy. It provides
protocol and binding traits, build-time validators, and a reference AsyncAPI
3.1 generator.

## Protocols

| Service trait | Binding traits | Encoding |
| --- | --- | --- |
| `@kafkaJson` | `@kafkaProduce`, `@kafkaConsume` | JSON |
| `@kafkaAvro` | `@kafkaProduce`, `@kafkaConsume` | Avro |
| `@kafkaProtobuf` | `@kafkaProduce`, `@kafkaConsume` | Protobuf |
| `@redisStreamsJson` | `@redisStreamAdd`, `@redisStreamRead` | JSON |
| `@redisPubSubJson` | `@redisPublish`, `@redisSubscribe` | JSON |

Payload roles are broker-independent: `@command` is accepted by the contract
owner, `@event` is emitted by it, and `@reply` is returned by a Redis Streams
command.

## Quick start

This service accepts both a one-way command and a request/reply operation:

```smithy
$version: "2"

namespace example.presence

use bote#command
use bote#redisStreamAdd
use bote#redisStreamsJson
use bote#reply

@redisStreamsJson
service PresenceService {
    operations: [SetPresence, GetPresence]
}

@redisStreamAdd(stream: "presence.commands")
operation SetPresence {
    input: SetPresenceCommand
}

@redisStreamAdd(stream: "presence.requests")
operation GetPresence {
    input: GetPresenceRequest
    output: GetPresenceReply
}

@command
structure SetPresenceCommand {
    userId: String
    status: String
}

@command
structure GetPresenceRequest {
    userId: String
}

@reply
structure GetPresenceReply {
    userId: String
    status: String
}
```

With no output, a Redis Streams add operation is fire-and-forget. A `@reply`
output makes the same binding request/reply; no separate request trait is
needed. Redis `@redisPublish` operations never have outputs because every
active subscriber receives the command.

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

- Produce-side inputs are `@command` structures. Consume-side outputs contain
  a `@streaming` union of `@event` structures.
- Kafka and Redis Pub/Sub produce operations have no output. Redis Streams add
  operations may omit output or return one `@reply` structure.
- A broker address has one owning service. Operations sharing it must agree on
  channel-level values such as Kafka compaction and Redis `maxLen`.
- Kafka JSON events use an envelope discriminator by default, optionally a
  `bote-type` header or no discriminator for a single-event channel.
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
`"perspective": "owner"` is the default; `"client"` reverses the operation
actions. Redis Streams request/reply operations produce an AsyncAPI reply with
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
