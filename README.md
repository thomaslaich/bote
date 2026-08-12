# bote

> **Proof of concept.** This repo explores how to model messaging contracts
> with [Smithy](https://smithy.io). Kafka is the first target; the same
> trait-driven approach should extend to other brokers (AMQP, Pulsar, NATS).
> Nothing here is stable.

A Smithy trait library for messaging, inspired by
[Disney's Alloy](https://github.com/disneystreaming/alloy). This repo owns the
contract: trait definitions, protocol specs, and validators. Code generators
depend on the `bote` JAR to implement the protocols.

A reference AsyncAPI generator ships as a separate module
(`io.bote:smithy-asyncapi`) so the trait library itself has no codegen
dependencies.

## Traits

**Protocols**, one per service, picking broker and wire encoding:

| Trait               | Broker / mode           | Encoding | Status  |
| ------------------- | ----------------------- | -------- | ------- |
| `@kafkaJson`        | Kafka                   | JSON     | Defined |
| `@kafkaAvro`        | Kafka + Schema Registry | Avro     | Defined |
| `@kafkaProtobuf`    | Kafka                   | Protobuf | Defined |
| `@redisStreamsJson` | Redis Streams           | JSON     | Defined |
| `@redisPubSubJson`  | Redis Pub/Sub           | JSON     | Defined |

**Operation traits**, one per broker capability, in the broker's own
vocabulary (the same principle as Smithy's HTTP and MQTT bindings). Each
carries the channel address:

| Produce side               | Consume side                | Broker                                     |
| -------------------------- | --------------------------- | ------------------------------------------ |
| `@kafkaProduce(topic:)`    | `@kafkaConsume(topic:)`     | Kafka; `compacted` declares log compaction |
| `@redisStreamAdd(stream:)` | `@redisStreamRead(stream:)` | Redis Streams; `maxLen` caps the stream    |
| `@redisPublish(channel:)`  | `@redisSubscribe(channel:)` | Redis Pub/Sub                              |

A produce operation takes a `@command` input and no output. A consume
operation streams `@event` payloads through a `@streaming` union in its
output.

**Message traits**, broker-agnostic:

- `@command` marks an instruction the contract owner accepts.
- `@event` marks a fact the contract owner emits.
- `@reply` is reserved. No current protocol supports replies; request-reply
  needs broker-native plumbing such as AMQP's `reply_to`/`correlation_id`.

**Member traits**: `@kafkaKey` marks the Kafka message key, `@kafkaHeader`
maps a member to a Kafka header. `@avroCompatibility` declares the Avro
compatibility mode.

**Infrastructure** (namespace `bote.infra`): `@kafkaTopicConfig` declares
partitions, replication, and retention.

## Contract ownership

bote models the API a contract owner offers to other applications, not both
sides of a conversation. The owner defines the commands it accepts, the
events it emits, and the channel names. The channel address is part of the
API surface, like a URI in a REST contract.

Provisioning is separate. `@kafkaTopicConfig` lives in `bote.infra` so a
platform team can attach it from its own model file with `apply`, without
touching the contract (see `examples/kafka/model/infra.smithy`). The owner
may also declare it inline; the namespace split only makes separate ownership
possible.

```smithy
$version: "2"

namespace example.orders.infra

use bote.infra#kafkaTopicConfig

apply example.orders#ConsumeOrderEvents @kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
)
```

Validators enforce the model:

- A channel address belongs to exactly one service.
- Operations sharing an address must declare identical channel values
  (`compacted`, `maxLen`).
- `@kafkaTopicConfig` may appear on at most one operation per topic.
- An operation carries exactly one broker operation trait, and it must match
  the service's protocol.
- Produce inputs must be `@command`; consume outputs must stream `@event`
  shapes; produce operations must not declare an output.

## Wire rules

The protocol specs pin down serialization so independent code generators
interoperate:

- `@kafkaHeader` members travel only as Kafka headers and are never
  serialized into the JSON value (like `@httpHeader` and the HTTP body).
- The `@kafkaKey` member is serialized both as the Kafka message key and as a
  field of the value.
- `@command` values are the bare JSON serialization of their structure.
- `@event` values carry a discriminator so consumers of a multi-event channel
  can tell event types apart. `@kafkaJson` takes an `eventDiscrimination`
  setting:
  - `ENVELOPE` (default): the value is wrapped in a single-key object keyed
    by the `@streaming` union member name, the same idiom `restJson1` uses
    for tagged unions: `{"placed": {"orderId": "42", ...}}`
  - `HEADER`: the value is bare; a `bote-type` Kafka header carries the
    member name
  - `NONE`: no discriminator; at most one event type per channel
    (validator-enforced)

  The Redis JSON protocols always use the envelope. `@kafkaAvro` needs no
  discriminator because the schema ID in the Confluent wire format identifies
  the event type.

For `@kafkaProtobuf`, the `@streaming` union maps to a proto message with a
oneof, which is the discriminator. Every payload member must carry an
explicit `alloy.proto#protoIndex` (validator-enforced): implicit numbering
breaks the wire format when members are reordered or removed. `@kafkaHeader`
members still travel only as headers; their index is reserved, not
serialized.

## Example

One command and one event subscription; the topics are carried by the
operation traits.

```smithy
$version: "2"

namespace example.orders

use bote#command
use bote#event
use bote#kafkaConsume
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaProduce

@kafkaJson
service OrderService {
    operations: [SubmitOrder, ConsumeOrderEvents]
}

@kafkaProduce(topic: "orders.commands")
operation SubmitOrder {
    input: SubmitOrderCommand
}

@kafkaConsume(topic: "orders.events")
operation ConsumeOrderEvents {
    output := { events: OrderEvents }
}

@command
structure SubmitOrderCommand {
    @kafkaKey
    orderId: String
    customerId: String
}

@event
structure OrderPlaced {
    @kafkaKey
    orderId: String
    customerId: String
}

@streaming
union OrderEvents {
    placed: OrderPlaced
}
```

## Generating AsyncAPI

The `smithy-asyncapi` module is a Smithy build plugin that emits an
[AsyncAPI 3.1](https://www.asyncapi.com/) document per bote protocol service.

AsyncAPI 3 actions describe the application the document is about, so the
mapping depends on the `perspective` setting. The default `"owner"` describes
the contract owner: produce operations become `action: receive` and consume
operations become `action: send`. Setting `"perspective": "client"` flips
both. The document records its side in `info.x-bote-perspective` so readers
and tooling need not guess.

| bote                                               | AsyncAPI 3.1                                         |
| -------------------------------------------------- | ---------------------------------------------------- |
| protocol service                                   | the document (`info`, `defaultContentType`)          |
| operation trait address (topic / stream / channel) | a `channel`                                          |
| produce `input` / consume output                   | the channel's and operation's `messages`             |
| broker operation trait                             | an `operation`; the `action` follows the perspective |
| payload structure                                  | a component `message` plus JSON Schema `payload`     |
| `@kafkaKey`                                        | the Kafka message binding `key`                      |
| `@kafkaHeader`                                     | the message `headers` schema                         |
| HEADER event discrimination                        | a constant `bote-type` property in `headers`         |
| `@kafkaTopicConfig`                                | Kafka channel binding partitions/replicas/config     |
| `compacted: true`                                  | `cleanup.policy: [compact]`                          |

Enable the plugin in a Smithy build:

```json
{
  "version": "1.0",
  "sources": ["model"],
  "plugins": {
    "asyncapi": {}
  }
}
```

By default one file is written per protocol service, named
`<ServiceName>.asyncapi.json`. Optional settings: `service` targets one
service, `perspective` picks the viewpoint.

```json
"plugins": { "asyncapi": { "service": "examples.kafka.orders#OrderService", "perspective": "owner" } }
```

Two example modules exercise the generator end to end: `examples/kafka`
(order service and streetlight device over JSON, telemetry over Protobuf)
and `examples/redis` (Streams chat, Pub/Sub presence). Each generates the owner document in the `source`
projection and the client document in a `client` projection. Run
`gradle build` and inspect
`build/smithyprojections/<module>/<projection>/asyncapi/`.

To view a generated document in
[AsyncAPI Studio](https://studio.asyncapi.com):

```shell
just studio                          # kafka / StreetlightDevice, owner view
just studio kafka OrderService
just studio redis ChatRoom client    # client view
```

Requires Node; the recipe runs `npx @asyncapi/cli start studio <file>` and
live-reloads on rebuild.

## Modules

| Module                    | Artifact          | Contents                                      |
| ------------------------- | ----------------- | --------------------------------------------- |
| (root)                    | `bote`            | trait definitions, protocol specs, validators |
| `codegen/smithy-asyncapi` | `smithy-asyncapi` | the AsyncAPI Smithy build plugin              |
| `examples/kafka`          |                   | Kafka example contracts                       |
| `examples/redis`          |                   | Redis Streams and Pub/Sub examples            |

## Build

Requires Java 21. A [devenv](https://devenv.sh) environment provides the
toolchain, [`just`](https://github.com/casey/just), and the formatters.
`just` lists the recipes:

| Recipe                                         | What it does                                                                    |
| ---------------------------------------------- | ------------------------------------------------------------------------------- |
| `just build`                                   | build and validate the models, run the generator and tests                      |
| `just studio [module] [service] [perspective]` | build, then open a generated doc in AsyncAPI Studio                             |
| `just fmt`                                     | format everything (`treefmt`, `gradle smithyFormat`)                            |
| `just check-format`                            | check formatting without rewriting anything                                     |
| `just golden`                                  | regenerate the golden AsyncAPI docs CI diffs against                            |
| `just verify-golden`                           | check generated docs against the golden files                                   |
| `just ci`                                      | what CI and the release workflow run (`check-format`, `build`, `verify-golden`) |
| `just publish-local`                           | publish the JARs to the local Maven repo                                        |
| `just clean`                                   | clean build outputs                                                             |
| `just rebuild`                                 | `clean` then `build`                                                            |

Gradle works directly as well: `gradle build`, `gradle publishToMavenLocal`.

## Coordinates

Published to Maven Central under the `io.github.thomaslaich.bote` group:

```
io.github.thomaslaich.bote:bote:<version>
io.github.thomaslaich.bote:smithy-asyncapi:<version>
```

## Releasing

Releases are tag-driven. Local builds carry the `0.1.0-SNAPSHOT` placeholder from
`gradle.properties`; the published version comes from the GitHub release tag, with
the leading `v` stripped and passed to Gradle as `-Pversion` (tag `v0.2.0` publishes
`0.2.0`).

1. Land a release commit on `main` that moves the `[Unreleased]` section of
   [CHANGELOG.md](CHANGELOG.md) under the new version and opens a fresh empty
   `[Unreleased]` above it.
2. Create a GitHub release with a new `v<version>` tag on that commit, using the
   changelog section as the release notes.
3. Publish the release. That triggers
   [`.github/workflows/release.yml`](.github/workflows/release.yml), which runs
   `just ci` and then `just publish <version>` — building, signing, and uploading
   both JARs to Maven Central through the Sonatype Central Portal.

Publishing needs four repository secrets: `MAVEN_CENTRAL_USERNAME` /
`MAVEN_CENTRAL_PASSWORD` (a Sonatype Central Portal user token) and
`MAVEN_GPG_PRIVATE_KEY` / `MAVEN_GPG_PASSPHRASE` (the armored signing key).
