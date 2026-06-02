# bote

> **Proof of concept.** This repo is an exploration — we are experimenting with how to model messaging technologies using [Smithy](https://smithy.io). Kafka is the first target, but the intent is to understand what it looks like to bring the same trait-driven, language-agnostic approach to other messaging systems (AMQP, Pulsar, NATS, etc.). Nothing here should be considered stable.

A [Smithy](https://smithy.io) trait library for Kafka, inspired by [Disney's Alloy](https://github.com/disneystreaming/alloy). The core of this repo is the *contract* — trait definitions, protocol specs, and validators. Codegen consumers (NSmithy, etc.) depend on the `bote` JAR to implement the protocol.

A reference **AsyncAPI generator** ships alongside the contract in a separate codegen module (`io.bote:smithy-asyncapi-codegen`), demonstrating one way to consume the traits. It stays out of the contract JAR so the trait library has no codegen dependencies.

## What's here

A broker-agnostic core plus per-broker channel traits.

**Application contract** — one trait per service, describing a messaging application view:

| Trait        | Meaning                                   | Status  |
|--------------|-------------------------------------------|---------|
| `@messaging` | Application-level messaging contract     | Defined |

**Legacy protocol traits** — still supported as service markers, but no longer required for mixed-broker documents:

| Trait            | Broker / mode           | Encoding | Status  |
|------------------|-------------------------|----------|---------|
| `@kafkaJson`     | Kafka                   | JSON     | Defined |
| `@kafkaAvro`     | Kafka + Schema Registry | Avro     | Defined |
| `@kafkaProtobuf` | Kafka                   | Protobuf | Stub    |
| `@redisStreamsJson` | Redis Streams        | JSON     | Defined |
| `@redisPubSubJson`  | Redis Pub/Sub        | JSON     | Defined |

**Broker-agnostic core** — these carry no broker-specific meaning:

- `@send` / `@receive` — direction of flow for an operation ([AsyncAPI](https://www.asyncapi.com/)'s send/receive vocabulary)
- `@channel` — binds an operation to a channel shape (an `@idRef`); the single binding mechanism across every broker

**Per-broker address & config** — applied to the channel shape:

- `@kafkaTopic` / `@kafkaTopicConfig` — Kafka topic name, compaction, partitions, retention, …
- `@redisStream` — Redis stream name + `maxLen`
- `@redisChannel` — Redis Pub/Sub channel name

**Message decoration & evolution**:

- `@kafkaKey` — marks the Kafka message key
- `@kafkaHeader` — maps a member to a Kafka header
- `@avroCompatibility` — Avro schema compatibility mode

A **channel** is a marker structure carrying an address trait. Operations bind with `@channel`, and the channel's address/config are declared once and shared. Because the channel is a shape (not a string repeated on each operation), a producer and a consumer in different repos bind to the *same* channel and their generated AsyncAPI channel sections use the same address and bindings. A separate catalog union can document every event type allowed on a channel; `@streaming` lives only on the **consumer's** subscription union, which may be a subset of that catalog.

## Example

The shared model defines the Kafka topic as a marker channel shape, plus the
payload shapes and an optional catalog of every event type allowed on that topic.

```smithy
$version: "2"

namespace example.shared

use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig

// The channel: a marker shape carrying the address + config. Declared once and
// shared by producer and consumer services.
@kafkaTopic(name: "orders")
@kafkaTopicConfig(partitions: 6, replicationFactor: 3)
structure OrdersTopic {}

// Optional catalog: all event types allowed on the orders topic.
union OrderEvent {
    placed: OrderPlaced
    shipped: OrderShipped
}

structure OrderPlaced {
    @kafkaKey
    orderId: String
    customerId: String
}

structure OrderShipped {
    @kafkaKey
    orderId: String
    carrier: String
}
```

The producer service owns only its application view: it binds to the shared topic
and sends concrete event payloads.

```smithy
$version: "2"

namespace example.producer

use bote#channel
use bote#messaging
use bote#send
use example.shared#OrderPlaced
use example.shared#OrderShipped
use example.shared#OrdersTopic

// The producer's perspective: emits individual event types to the channel.
@messaging
service OrderService {
    operations: [PublishOrderPlaced, PublishOrderShipped]
}

@send
@channel(OrdersTopic)
operation PublishOrderPlaced {
    input: OrderPlaced
}

@send
@channel(OrdersTopic)
operation PublishOrderShipped {
    input: OrderShipped
}
```

The consumer service has its own application view. Its `@streaming` subscription
union can be a subset of the shared catalog.

```smithy
$version: "2"

namespace example.consumer

use bote#channel
use bote#messaging
use bote#receive
use example.shared#OrderShipped
use example.shared#OrdersTopic

// The consumer's perspective: subscribes to the subset it cares about.
@messaging
service FulfilmentDashboard {
    operations: [ConsumeOrderUpdates]
}

@receive
@channel(OrdersTopic)
operation ConsumeOrderUpdates {
    output := {
        updates: OrderUpdates
    }
}

@streaming
union OrderUpdates {
    shipped: OrderShipped
}
```

The producer and consumer can live in different repos while depending on the same shared model. Both bind to `OrdersTopic`; payload types are defined once, while the consumer's `@streaming` subscription union is its local view and may be a subset of the catalog.

## Generating AsyncAPI

The `smithy-asyncapi-codegen` module provides a Smithy build plugin that emits an
[AsyncAPI 3.1](https://www.asyncapi.com/) document per service annotated with
`@messaging` or a legacy bote protocol trait. The send/receive vocabulary maps directly
onto bote's `@send`/`@receive`:

| bote                          | AsyncAPI 3.1                                    |
|-------------------------------|-------------------------------------------------|
| `@messaging` service           | the document (`info`, `defaultContentType`)     |
| channel shape (`@kafkaTopic` / `@redisStream` / `@redisChannel`) | a `channel` |
| send `input` / receive subscription | the channel's `messages` and operation's `messages` |
| channel shape `@documentation` | the channel `description`                      |
| `@channel`                    | the operation's `channel` reference             |
| `@send` / `@receive`          | an `operation` with `action: send` / `receive`  |
| payload structure             | a component `message` + JSON Schema `payload`   |
| `@kafkaKey`                   | the Kafka message binding `key`                 |
| `@kafkaHeader`                | the message `headers` schema                    |
| `@kafkaTopicConfig`           | Kafka channel binding partitions/replicas/config |
| `@kafkaTopic(compacted: true)` | `cleanup.policy: [compact]`                    |

Enable the `asyncapi-codegen` plugin in a consumer's `smithy-build.json`:

```json
{
    "version": "1.0",
    "sources": ["model"],
    "plugins": {
        "asyncapi-codegen": {}
    }
}
```

Because each AsyncAPI document describes a single application, one file is
written per service, named `<ServiceName>.asyncapi.json`. By default every
protocol service is documented; set the optional `service` setting to a service
shape ID to target one (and use one projection per service to emit several):

```json
"plugins": { "asyncapi-codegen": { "service": "orders.producer#OrderService" } }
```

Three example modules exercise the generator end-to-end:

- **`examples/kafka-streetlights`** — a faithful Kafka port of AsyncAPI's
  Streetlights sample (marker channels), for comparing the output against the
  official document. A `StreetlightDevice` and a `StreetlightsBackend` bind to the
  *same* channels with opposite `@send`/`@receive` — identical `channels`,
  opposite `operations`.
- **`examples/kafka-orders`** — a multi-event `orders` topic modelled as a
  marker channel plus separate catalog union. The producer emits individual event
  types; the consumer `@receive`s a **subset** via its own `@streaming`
  subscription union.
- **`examples/redis`** — Redis Streams (`chat`) and Redis Pub/Sub (`presence`),
  showing the same `@channel` model on a second broker.

Run `gradle build` and inspect each module's
`build/smithyprojections/<module>/source/asyncapi-codegen/`.

### Viewing it

The generated document is plain AsyncAPI 3.1, so any AsyncAPI tooling renders it.
The quickest is [AsyncAPI Studio](https://studio.asyncapi.com), which gives a
live, navigable view of the channels, operations, and message schemas:

```shell
just studio                          # kafka-streetlights / StreetlightsBackend
just studio kafka-orders OrderService
just studio redis ChatProducer
```

`just studio [module] [service]` builds, then opens Studio preloaded with that
module's document (it live-reloads, so re-running the build refreshes the view).
Under the hood it runs `npx @asyncapi/cli start studio <file>`, so it needs Node.

Scalar is OpenAPI-only and does not render AsyncAPI.

## Modules

| Module                       | Coordinates              | Contents                                  |
|------------------------------|--------------------------|-------------------------------------------|
| (root)                       | `io.bote:bote`           | trait definitions, protocol specs, validators |
| `codegen/smithy-asyncapi-codegen` | `io.bote:smithy-asyncapi-codegen` | the AsyncAPI Smithy build plugin |
| `examples/kafka-streetlights` | —                       | Kafka port of the official Streetlights sample |
| `examples/kafka-orders`      | —                        | multi-event topic with marker channel + catalog |
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
