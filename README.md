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

Supporting traits applied at operation and member level:

- `@send` — marks an operation as sending messages to a topic (must declare `input`)
- `@receive` — marks an operation as receiving messages from a topic (output must contain a `@streaming` union)
- `@kafkaTopic` — binds an operation to a named Kafka topic (with optional log compaction)
- `@kafkaKey` — marks a structure member as the Kafka message key
- `@kafkaHeader` — maps a structure member to a Kafka message header
- `@kafkaTopicConfig` — captures infrastructure config (partitions, replication factor, retention, etc.)
- `@avroCompatibility` — declares the schema compatibility mode for a topic

`@send` and `@receive` follow [AsyncAPI](https://www.asyncapi.com/)'s send/receive vocabulary and are intentionally broker-agnostic — the protocol trait (`@kafkaJson`, `@kafkaAvro`, etc.) carries the Kafka-specific semantics.

## Example

```smithy
$version: "2"

namespace example

use bote#kafkaJson
use bote#kafkaTopic
use bote#send
use bote#receive
use bote#kafkaKey
use bote#kafkaHeader

@kafkaJson
service OrderService {
    operations: [PublishOrder, ConsumeOrders]
}

@send
@kafkaTopic(name: "orders")
operation PublishOrder {
    input: OrderEvent
}

@receive
@kafkaTopic(name: "orders")
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

The producer contract is owned by the service that publishes. Consumer services in other repos depend on this JAR and define their own `@receive` operations against the same topic, referencing the shared message type — the same pattern as an HTTP client depending on a server's Smithy model.

## Generating AsyncAPI

The `bote-asyncapi` module provides a Smithy build plugin that emits an
[AsyncAPI 3.1](https://www.asyncapi.com/) document for every service annotated
with a Kafka protocol trait. AsyncAPI's send/receive vocabulary maps directly
onto bote's `@send`/`@receive` traits:

| bote                        | AsyncAPI 3.1                                   |
|-----------------------------|------------------------------------------------|
| Kafka protocol service      | the document (`info`, `defaultContentType`)    |
| `@kafkaTopic`               | a `channel` with a Kafka channel binding       |
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

One file is written per service, named `<ServiceName>.asyncapi.json`. The
`example/` module wires this up end-to-end against a Kafka port of AsyncAPI's
Streetlights sample — run `gradle :example:build` and inspect
`example/build/smithyprojections/example/source/asyncapi/`.

### Viewing it

The generated document is plain AsyncAPI 3.1, so any AsyncAPI tooling renders it.
The quickest is [AsyncAPI Studio](https://studio.asyncapi.com), which gives a
live, navigable view of the channels, operations, and message schemas:

```shell
just studio
```

This builds the example and opens Studio preloaded with the generated spec
(it live-reloads, so re-running the build refreshes the view). Equivalent to:

```shell
npx @asyncapi/cli start studio \
    example/build/smithyprojections/example/source/asyncapi/StreetlightsKafka.asyncapi.json
```

Requires Node. Scalar is OpenAPI-only and does not render AsyncAPI.

## Modules

| Module          | Coordinates              | Contents                                  |
|-----------------|--------------------------|-------------------------------------------|
| (root)          | `io.bote:bote`           | trait definitions, protocol specs, validators |
| `asyncapi`      | `io.bote:bote-asyncapi`  | the AsyncAPI Smithy build plugin          |
| `example`       | —                        | demo model exercising the generator       |

## Build

Requires Java 21. A [devenv](https://devenv.sh) environment is provided.

```shell
gradle build
gradle publishToMavenLocal
```

## Coordinates

```
io.bote:bote:0.1.0-SNAPSHOT
```

## Status

This is exploratory work. APIs will change. There are no stability guarantees.
