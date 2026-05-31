# bote

> **Proof of concept.** This repo is an exploration — we are experimenting with how to model messaging technologies using [Smithy](https://smithy.io). Kafka is the first target, but the intent is to understand what it looks like to bring the same trait-driven, language-agnostic approach to other messaging systems (AMQP, Pulsar, NATS, etc.). Nothing here should be considered stable.

A [Smithy](https://smithy.io) trait library for Kafka, inspired by [Disney's Alloy](https://github.com/disneystreaming/alloy). This repo owns the *contract* — trait definitions, protocol specs, and validators — not the codegen. Codegen consumers (smithy4s, smithy-typescript, etc.) depend on this JAR to implement the protocol.

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
