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

- `@invocation` / `@subscription` — message contract operations; generated
  AsyncAPI still emits `send` / `receive` actions

**Per-broker address & config**:

- `@kafkaTopic` / `@kafkaTopicConfig` — Kafka topic name, compaction, partitions,
  retention, …
- `@redisStream` — Redis stream name + `maxLen`
- `@redisChannel` — Redis Pub/Sub channel name

**Message decoration & evolution**:

- `@event` / `@command` / `@reply` — classifies payload structures by message
  kind
- `@kafkaKey` — marks the Kafka message key
- `@kafkaHeader` — maps a member to a Kafka header
- `@avroCompatibility` — Avro schema compatibility mode

Kafka operations declare their topic directly with `@kafkaTopic`. An
`@invocation` takes a `@command` payload and an optional `@reply`; a
`@subscription` streams `@event` payloads through a `@streaming` union.

## Contract ownership

`bote` models the API a contract owner offers to other applications, not a
broker's internal view and not both sides of a conversation at once. The owner is
the party responsible for the domain semantics of the messages:

- A service owns the commands it accepts.
- A service owns the events it emits.
- A broker or platform may own topic provisioning, retention, ACLs and delivery
  mechanics, but that is separate from owning the message contract.

That is why operation traits describe the client-facing API surface:

- `@invocation` means a client can invoke a capability by writing a command
  message to the broker address on the operation. The input must be a `@command`
  structure. If the operation has an output, that output must be a `@reply`
  structure.
- `@subscription` means a client can subscribe to events from the contract owner.
  The output must contain a `@streaming` union whose members target `@event`
  structures.

Message-kind traits describe payload semantics, not transport direction:

- `@command` is an instruction the contract owner accepts.
- `@event` is a fact the contract owner emits.
- `@reply` is an optional response to an invocation.

The AsyncAPI generator maps this vocabulary to AsyncAPI's transport actions:
`@invocation` becomes `action: send`, and `@subscription` becomes
`action: receive`.

## Example

This order-service contract offers one command and one event subscription. The
command and event topics are declared on the operations.

```smithy
$version: "2"

namespace example.orders

use bote#command
use bote#event
use bote#invocation
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic
use bote#subscription

@kafkaJson
service OrderService {
    operations: [InvokeSubmitOrder, SubscribeToOrderEvents]
}

@invocation
@kafkaTopic(name: "orders.commands")
operation InvokeSubmitOrder {
    input: SubmitOrder
}

@subscription
@kafkaTopic(name: "orders.events")
operation SubscribeToOrderEvents {
    output := { events: OrderEvents }
}

@command
structure SubmitOrder {
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
services. Bote's `@invocation`/`@subscription` operations map to AsyncAPI's
`send`/`receive` actions:

| bote                          | AsyncAPI 3.1                                    |
|-------------------------------|-------------------------------------------------|
| protocol service              | the document (`info`, `defaultContentType`)     |
| operation address trait (`@kafkaTopic` / `@redisStream` / `@redisChannel`) | a `channel` |
| invocation `input` / subscription output | the channel's `messages` and operation's `messages` |
| `@invocation` / `@subscription`  | an `operation` with `action: send` / `receive`   |
| payload structure             | a component `message` + JSON Schema `payload`   |
| `@kafkaKey`                   | the Kafka message binding `key`                 |
| `@kafkaHeader`                | the message `headers` schema                    |
| `@kafkaTopicConfig`           | Kafka channel binding partitions/replicas/config |
| `@kafkaTopic(compacted: true)` | `cleanup.policy: [compact]`                    |

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
shape ID to target one:

```json
"plugins": { "asyncapi": { "service": "examples.kafka.orders#OrderService" } }
```

Two example modules exercise the generator end-to-end:

- **`examples/kafka`** — Kafka order-service and streetlight-device contracts,
  modelled as provider-owned APIs with operation-level topics.
- **`examples/redis`** — Redis Streams (`chat`) and Redis Pub/Sub (`presence`),
  following the same invocation/subscription and command/event rules.

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
