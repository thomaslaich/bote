# bote

> **Proof of concept.** This repo is an exploration — we are experimenting with how to model messaging technologies using [Smithy](https://smithy.io). Kafka is the first target, but the intent is to understand what it looks like to bring the same trait-driven, language-agnostic approach to other messaging systems (AMQP, Pulsar, NATS, etc.). Nothing here should be considered stable.

A [Smithy](https://smithy.io) trait library for Kafka, inspired by [Disney's Alloy](https://github.com/disneystreaming/alloy). The core of this repo is the *contract* — trait definitions, protocol specs, and validators. Codegen consumers (NSmithy, etc.) depend on the `bote` JAR to implement the protocol.

A reference **AsyncAPI generator** ships alongside the contract in a separate codegen module (`io.bote:smithy-asyncapi`), demonstrating one way to consume the traits. It stays out of the contract JAR so the trait library has no codegen dependencies.

## What's here

A broker-agnostic core plus service-level protocol traits and per-broker channel traits.

**Protocols** — one trait per service, picking the broker/mode and wire encoding:

| Trait            | Broker / mode           | Encoding | Status  |
|------------------|-------------------------|----------|---------|
| `@kafkaJson`     | Kafka                   | JSON     | Defined |
| `@kafkaAvro`     | Kafka + Schema Registry | Avro     | Defined |
| `@kafkaProtobuf` | Kafka                   | Protobuf | Stub    |
| `@redisStreamsJson` | Redis Streams        | JSON     | Defined |
| `@redisPubSubJson`  | Redis Pub/Sub        | JSON     | Defined |

**Broker-agnostic core** — these carry no broker-specific meaning:

- `@command` / `@event` — classifies payload structures by their role in the
  contract

**Per-broker operation traits** — each broker speaks its own language (the
same principle as Smithy's HTTP and MQTT bindings), and the trait carries the
channel address:

- `@kafkaProduce(topic:)` / `@kafkaConsume(topic:)` — Kafka produce/consume
  capabilities; `compacted` declares log compaction (a contract-level promise)
- `@redisStreamAdd(stream:)` / `@redisStreamRead(stream:)` — Redis Streams
  XADD/XREAD capabilities; `maxLen` caps the stream
- `@redisPublish(channel:)` / `@redisSubscribe(channel:)` — Redis Pub/Sub
  PUBLISH/SUBSCRIBE capabilities

**Infrastructure** (namespace `bote.infra`, deliberately outside the message
contract):

- `@kafkaTopicConfig` — partitions, replication, retention, …

**Message decoration & evolution**:

- `@kafkaKey` — marks the Kafka message key
- `@kafkaHeader` — maps a member to a Kafka header
- `@avroCompatibility` — Avro schema compatibility mode
- `@reply` — reserved vocabulary: no current protocol supports replies

A produce-side operation (`@kafkaProduce`, `@redisStreamAdd`, `@redisPublish`)
takes a `@command` payload and no output; a consume-side operation
(`@kafkaConsume`, `@redisStreamRead`, `@redisSubscribe`) streams `@event`
payloads through a `@streaming` union.

## Contract ownership

`bote` models the API a contract owner offers to other applications, not a
broker's internal view and not both sides of a conversation at once. The owner is
the party responsible for the domain semantics of the messages:

- A service owns the commands it accepts.
- A service owns the events it emits.
- A broker or platform may own topic provisioning, retention, ACLs and delivery
  mechanics, but that is separate from owning the message contract.

That is why operation traits describe the client-facing API surface, in each
broker's own vocabulary:

- A produce-side trait (`@kafkaProduce`, `@redisStreamAdd`, `@redisPublish`)
  means a client can write a command message to the channel carried by the
  trait. The input must be a `@command` structure. Produce operations declare
  no output — no current protocol supports reply semantics (request-reply
  needs broker-native reply plumbing, as in AMQP's
  `reply_to`/`correlation_id`; on Kafka and Redis it is only a convention).
- A consume-side trait (`@kafkaConsume`, `@redisStreamRead`,
  `@redisSubscribe`) means a client can receive events from the contract
  owner on that channel. The output must contain a `@streaming` union whose
  members target `@event` structures.

The owner names the channel and defines what flows over it, because the
address is part of the API surface — the analogue of a URI in a REST
contract. How the channel is *provisioned* is owned separately:

Message-kind traits describe payload semantics, not transport direction:

- `@command` is an instruction the contract owner accepts.
- `@event` is a fact the contract owner emits.
- `@reply` is reserved for a future protocol with first-class reply support.

Ownership is validator-enforced, not just prose: a channel address belongs to
exactly one service, all operations sharing an address must declare identical
channel values (compaction, `maxLen`, …), and `@kafkaTopicConfig` may appear
on at most one operation per topic. The owner can still provision — declaring
`@kafkaTopicConfig` inline on their own operation is perfectly valid — the
namespaces just make it possible for a different team to own that layer.

### Infrastructure is owned separately

Topic provisioning is not part of the message contract, so
`@kafkaTopicConfig` lives in its own namespace, `bote.infra`. A platform team
can attach it from a separate model file with `apply`, without touching the
contract (see `examples/kafka/model/infra.smithy`):

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

### Wire rules

The protocol specs pin down how messages are serialized, so independent
codegen consumers interoperate:

- Members annotated `@kafkaHeader` travel **only** as Kafka headers — they are
  never serialized into the JSON value (mirroring how `@httpHeader` members
  leave the HTTP body).
- The `@kafkaKey` member is serialized both as the Kafka message key and as a
  field of the value.
- `@command` values are the bare JSON serialization of their structure.
- `@event` values carry a discriminator so consumers of a multi-event channel
  can tell event types apart. `@kafkaJson` takes an `eventDiscrimination`
  setting:
  - `ENVELOPE` (default) — the value is wrapped in a single-key object keyed
    by the `@streaming` union member name, exactly how `restJson1` serializes
    tagged unions: `{"placed": {"orderId": "42", ...}}`
  - `HEADER` — the value is bare; a `bote-type` Kafka header carries the
    member name
  - `NONE` — no discriminator; at most one event type per channel
    (validator-enforced)

  The Redis JSON protocols always use the envelope. `@kafkaAvro` needs no
  discriminator: the schema ID in the Confluent wire format identifies the
  event type.

## Example

This order-service contract offers one command and one event subscription. The
command and event topics are carried by the broker operation traits.

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

The `smithy-asyncapi` module provides a Smithy build plugin that emits an
[AsyncAPI 3.1](https://www.asyncapi.com/) document from one or more bote protocol
services.

AsyncAPI 3 actions describe the application the document is about, so the
mapping depends on the `perspective` setting. The default, `"owner"`,
describes the contract owner: produce-side operations (the owner accepts
commands) become `action: receive`, and consume-side operations (the owner
emits events) become `action: send`. Set `"perspective": "client"` to
generate the client's view instead, flipping both.

| bote                          | AsyncAPI 3.1                                    |
|-------------------------------|-------------------------------------------------|
| protocol service              | the document (`info`, `defaultContentType`)     |
| the broker operation trait's address (topic / stream / channel) | a `channel` |
| produce `input` / consume output | the channel's `messages` and operation's `messages` |
| a broker operation trait      | an `operation`; the `action` follows the perspective |
| payload structure             | a component `message` + JSON Schema `payload` (envelope-wrapped for ENVELOPE-discriminated events; `@kafkaHeader` members stripped) |
| `@kafkaKey`                   | the Kafka message binding `key`                 |
| `@kafkaHeader`                | the message `headers` schema                    |
| HEADER event discrimination   | a constant `bote-type` property in the `headers` schema |
| `@kafkaTopicConfig`           | Kafka channel binding partitions/replicas/config |
| `compacted: true`             | `cleanup.policy: [compact]`                     |

Enable the `asyncapi` plugin in a Smithy build:

```json
{
    "version": "1.0",
    "sources": ["model"],
    "plugins": {
        "asyncapi": {}
    }
}
```

By default, one file is written per protocol service, named
`<ServiceName>.asyncapi.json`. Set the optional `service` setting to a service
shape ID to target one, and `perspective` to pick the viewpoint:

```json
"plugins": { "asyncapi": { "service": "examples.kafka.orders#OrderService", "perspective": "owner" } }
```

Two example modules exercise the generator end-to-end:

- **`examples/kafka`** — Kafka order-service and streetlight-device contracts,
  modelled as provider-owned APIs with operation-level topics.
- **`examples/redis`** — Redis Streams (`chat`) and Redis Pub/Sub (`presence`),
  following the same produce/consume and command/event rules.

Run `gradle build` and inspect each module's
`build/smithyprojections/<module>/source/asyncapi/`.

### Viewing it

The generated document is plain AsyncAPI 3.1, so any AsyncAPI tooling renders it.
The quickest is [AsyncAPI Studio](https://studio.asyncapi.com), which gives a
live, navigable view of the channels, operations, and message schemas:

```shell
just studio                          # kafka / StreetlightDevice
just studio kafka OrderService
just studio redis ChatRoom
```

`just studio [module] [service]` builds, then opens Studio preloaded with that
module's document (it live-reloads, so re-running the build refreshes the view).
Under the hood it runs `npx @asyncapi/cli start studio <file>`, so it needs Node.

## Modules

| Module                       | Coordinates              | Contents                                  |
|------------------------------|--------------------------|-------------------------------------------|
| (root)                       | `io.bote:bote`           | trait definitions, protocol specs, validators |
| `codegen/smithy-asyncapi` | `io.bote:smithy-asyncapi` | the AsyncAPI Smithy build plugin |
| `examples/kafka`             | —                        | Kafka order-service + streetlight-device contracts |
| `examples/redis`             | —                        | Redis Streams + Pub/Sub                    |

## Build

Requires Java 21. A [devenv](https://devenv.sh) environment is provided, which
also supplies [`just`](https://github.com/casey/just) and the formatters. Common
tasks are wrapped as `just` recipes (`just` on its own lists them):

| Recipe                  | What it does                                            |
|-------------------------|---------------------------------------------------------|
| `just build`            | build and validate the models, run the generator + tests |
| `just studio [service]` | build, then open a generated doc in AsyncAPI Studio      |
| `just fmt`              | format everything (`treefmt` + `gradle smithyFormat`)    |
| `just golden`           | regenerate the golden AsyncAPI docs CI diffs against     |
| `just verify-golden`    | check generated docs against the golden files (CI does this) |
| `just publish-local`    | publish the JARs to the local Maven repo (`~/.m2`)       |
| `just clean`            | clean build outputs                                      |
| `just rebuild`          | `clean` then `build`                                     |

Or call Gradle directly — `gradle build`, `gradle publishToMavenLocal`.

## Coordinates

```
io.bote:bote:0.1.0-SNAPSHOT
```

## Status

This is exploratory work. APIs will change. There are no stability guarantees.
