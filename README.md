# bote

> **Proof of concept.** This repo is an exploration — we are experimenting with how to model messaging technologies using [Smithy](https://smithy.io). Kafka is the first target, but the intent is to understand what it looks like to bring the same trait-driven, language-agnostic approach to other messaging systems (AMQP, Pulsar, NATS, etc.). Nothing here should be considered stable.

A [Smithy](https://smithy.io) trait library for Kafka, inspired by [Disney's Alloy](https://github.com/disneystreaming/alloy). The core of this repo is the *contract* — trait definitions, protocol specs, and validators. Codegen consumers (NSmithy, etc.) depend on the `bote` JAR to implement the protocol.

A reference **AsyncAPI generator** ships alongside the contract in a separate module (`io.bote:bote-asyncapi`), demonstrating one way to consume the traits. It stays out of the contract JAR so the trait library has no codegen dependencies.

## What's here

Three Kafka protocol traits, each modelling a different serialization format:

| Trait            | Encoding         | Status        |
|------------------|------------------|---------------|
| `@kafkaJson`     | JSON             | Defined       |
| `@kafkaAvro`     | Avro + Schema Registry | Defined  |
| `@kafkaProtobuf` | Protocol Buffers | Stub          |

Supporting traits:

- `@kafkaTopic` — declares a Kafka topic (an AsyncAPI channel) as a first-class, shareable structure shape, with optional log compaction
- `@kafkaTopicConfig` — captures a topic's infrastructure config (partitions, replication factor, retention, etc.), declared once on the topic shape
- `@channel` — binds an operation to the topic shape it sends to or receives from (an `@idRef`)
- `@send` — marks an operation as sending messages to its channel (must declare `input`)
- `@receive` — marks an operation as receiving messages from its channel (output must contain a `@streaming` union)
- `@kafkaKey` — marks a structure member as the Kafka message key
- `@kafkaHeader` — maps a structure member to a Kafka message header
- `@avroCompatibility` — declares the schema compatibility mode for a topic

`@send` and `@receive` follow [AsyncAPI](https://www.asyncapi.com/)'s send/receive vocabulary and are intentionally broker-agnostic — the protocol trait (`@kafkaJson`, `@kafkaAvro`, etc.) carries the Kafka-specific semantics.

Modelling the topic as a **shape** (rather than a string repeated on each operation) means its name, partitions, retention and compaction are declared once and can be distributed as part of the contract — exactly like the message payload types. A producer and a consumer in different repos bind to the same channel shape, so their generated AsyncAPI channel sections come out identical.

## Example

```smithy
$version: "2"

namespace example

use bote#channel
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaHeader
use bote#kafkaTopic
use bote#kafkaTopicConfig
use bote#send
use bote#receive

@kafkaJson
service OrderService {
    operations: [PublishOrder, ConsumeOrders]
}

/// The orders topic (an AsyncAPI channel). Declared once; bound to by name.
@kafkaTopic(name: "orders")
@kafkaTopicConfig(partitions: 6, replicationFactor: 3)
structure OrdersChannel {}

@send
@channel(OrdersChannel)
operation PublishOrder {
    input: OrderEvent
}

@receive
@channel(OrdersChannel)
operation ConsumeOrders {
    output := {
        events: OrderEventStream
    }
}

@streaming
union OrderEventStream {
    orderEvent: OrderEvent
}

structure OrderEvent {
    @kafkaKey
    orderId: String

    @kafkaHeader(name: "x-trace-id")
    traceId: String

    customerId: String
    totalCents: Integer
}
```

The producer contract is owned by the service that publishes. Consumer services in other repos depend on this JAR and define their own `@receive` operations against the same channel shape, referencing the shared message type — the same pattern as an HTTP client depending on a server's Smithy model.

## Generating AsyncAPI

The `bote-asyncapi` module provides a Smithy build plugin that emits an
[AsyncAPI 3.1](https://www.asyncapi.com/) document per service annotated with a
Kafka protocol trait. AsyncAPI's send/receive vocabulary maps directly onto
bote's `@send`/`@receive` traits:

| bote                        | AsyncAPI 3.1                                   |
|-----------------------------|------------------------------------------------|
| Kafka protocol service      | the document (`info`, `defaultContentType`)    |
| `@kafkaTopic` structure     | a `channel` (with a Kafka channel binding)     |
| topic shape `@documentation` | the channel `description`                     |
| `@channel`                  | the operation's `channel` reference            |
| `@send` / `@receive`        | an `operation` with `action: send` / `receive` |
| message value structure     | a component `message` + JSON Schema `payload`  |
| `@kafkaKey`                 | the Kafka message binding `key`                |
| `@kafkaHeader`              | the message `headers` schema                   |
| `@kafkaTopicConfig`         | Kafka channel binding partitions/replicas/config |
| `@kafkaTopic(compacted: true)` | `cleanup.policy: [compact]`                 |

Enable the `asyncapi` plugin in a consumer's `smithy-build.json`:

```json
{
    "version": "1.0",
    "sources": ["model"],
    "plugins": {
        "asyncapi": {}
    }
}
```

Because each AsyncAPI document describes a single application, one file is
written per service, named `<ServiceName>.asyncapi.json`. By default every Kafka
service is documented; set the optional `service` setting to a service shape ID
to target one (and use one projection per service to emit several):

```json
"plugins": { "asyncapi": { "service": "smartylighting.device#StreetlightDevice" } }
```

The `example/` module wires this up end-to-end against a Kafka port of AsyncAPI's
Streetlights sample. It models the shared channels and message types in
`smartylighting.shared`, then a `StreetlightDevice` and a `StreetlightsBackend`
service in their own namespaces — mirror images that bind to the *same* channel
shapes with opposite `@send`/`@receive`. Run `gradle :example:build` and inspect
`example/build/smithyprojections/example/source/asyncapi/`: the two documents
have identical `channels` sections and opposite `operations` — one shared
contract, two single-perspective documents.

### Viewing it

The generated document is plain AsyncAPI 3.1, so any AsyncAPI tooling renders it.
The quickest is [AsyncAPI Studio](https://studio.asyncapi.com), which gives a
live, navigable view of the channels, operations, and message schemas:

```shell
just studio
```

This builds the example and opens Studio preloaded with the `StreetlightsBackend`
document (it live-reloads, so re-running the build refreshes the view). Pass a
service name to view the other, e.g. `just studio StreetlightDevice`. Under the
hood it runs `npx @asyncapi/cli start studio <file>`, so it needs Node.

Scalar is OpenAPI-only and does not render AsyncAPI.

## Modules

| Module          | Coordinates              | Contents                                  |
|-----------------|--------------------------|-------------------------------------------|
| (root)          | `io.bote:bote`           | trait definitions, protocol specs, validators |
| `asyncapi`      | `io.bote:bote-asyncapi`  | the AsyncAPI Smithy build plugin          |
| `example`       | —                        | demo model exercising the generator       |

## Build

Requires Java 21. A [devenv](https://devenv.sh) environment is provided, which
also supplies [`just`](https://github.com/casey/just) and the formatters. Common
tasks are wrapped as `just` recipes (`just` on its own lists them):

| Recipe                  | What it does                                            |
|-------------------------|---------------------------------------------------------|
| `just build`            | build and validate the models, run the generator + tests |
| `just studio [service]` | build, then open a generated doc in AsyncAPI Studio      |
| `just fmt`              | format everything (`treefmt` + `gradle smithyFormat`)    |
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
