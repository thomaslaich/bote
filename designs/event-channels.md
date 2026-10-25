# Design: Event channels

Status: accepted.

## Summary

A bote contract has one owner: the service. The owner accepts commands and
publishes events. The two are modeled differently because they are different
things:

- A **command** is a request the owner handles. It is a Smithy operation on the
  owner's service.
- An **event** is a fact the owner publishes. It is a structure, grouped into
  a **channel**: a union shape listed on the owning service.

Both are bound to a broker address by the same address traits. Traits name the
address, never an action, so one contract reads the same from either side.

Consumers do not appear in the contract. A consumer reads the owner's contract
and generates its side from it.

## Example

```smithy
$version: "2"

namespace example.orders

use bote#channels
use bote#event
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic

@kafkaJson
@channels([OrderEvents])
service Orders {
    version: "1.0.0"
    operations: [PlaceOrder]
}

/// Ask the orders service to place an order.
@kafkaTopic(name: "orders.commands")
operation PlaceOrder {
    input := {
        @required
        @kafkaKey
        orderId: String

        @required
        items: LineItems
    }
}

/// Order lifecycle events, keyed by order.
@kafkaTopic(name: "orders.events", compacted: false)
union OrderEvents {
    placed: OrderPlaced
    cancelled: OrderCancelled
}

@event
structure OrderPlaced {
    @required
    @kafkaKey
    orderId: String

    @required
    placedAt: Timestamp
}

@event
structure OrderCancelled {
    @required
    @kafkaKey
    orderId: String

    reason: String
}
```

## Model

### Roles

Each role has a marker trait and a position, and validation keeps the two in
agreement, as Smithy does for `@error`:

| Role | Marker | Position |
| --- | --- | --- |
| Command | `@input` | Input of an operation on a bote service |
| Reply | `@output` | Output of a Redis Streams command operation |
| Event | `@bote#event` | Member of a channel union |

Commands and replies use Smithy's own markers. `@event` marks a structure as
an event where it is defined, before any channel references it:

```smithy
@trait(selector: "structure")
structure event {}
```

A library of shared event types is then a closure such as
`structure[trait|bote#event]`, and tools can apply event-specific rules, such
as stricter compatibility checks, from the shape alone.

### Addresses

An address trait binds an operation or a channel to a broker address. Its
target decides the direction: the owner receives commands at an operation's
address and publishes events to a channel's address.

| Protocol | Address trait | Operation output |
| --- | --- | --- |
| `@kafkaJson`, `@kafkaAvro`, `@kafkaProtobuf` | `@kafkaTopic(name:, compacted:)` | None |
| `@redisStreamsJson`, `@redisStreamsCbor` | `@redisStream(key:, maxLen:)` | None, or a reply for request/reply |
| `@redisPubSubJson`, `@redisPubSubCbor` | `@redisChannel(name:)` | None |

```smithy
@trait(selector: ":is(operation, union)")
structure kafkaTopic {
    @required
    name: String

    compacted: Boolean
}
```

Request/reply uses `reply_to` and `correlation_id` transport metadata, a
temporary Pub/Sub reply channel, and at-most-once reply delivery.

### Encodings

Each protocol trait fixes one wire format:

| Broker | JSON | CBOR | Avro | Protobuf |
| --- | --- | --- | --- | --- |
| Kafka | `@kafkaJson` | | `@kafkaAvro` | `@kafkaProtobuf` |
| Redis Streams | `@redisStreamsJson` | `@redisStreamsCbor` | | |
| Redis Pub/Sub | `@redisPubSubJson` | `@redisPubSubCbor` | | |

Kafka encodings follow the schema registry ecosystem. Redis has no schema
registry convention, so Redis encodings follow Smithy's own JSON and CBOR
mappings, the ones `restJson1` and `rpcv2Cbor` define, and need no
per-member field numbers.

A Redis value is a binary string and holds the encoded payload directly.
Redis Streams entries carry a `content-type` field (`application/json` or
`application/cbor`) next to the payload and the `reply_to` and
`correlation_id` metadata.

### Channels

A channel is a union whose members target event structures and that carries
an address trait. The service lists the channels it owns, as it lists its
operations:

```smithy
@trait(selector: "service")
list channels {
    @idRef(failWhenMissing: true, selector: "union")
    member: String
}
```

Union member names are the event discriminators. Each encoding uses them as
follows:

- `@kafkaJson`: `eventDiscrimination` on the protocol trait selects
  `ENVELOPE` (default, `{"placed": {...}}`), `HEADER` (`bote-type` header) or
  `NONE`.
- `@kafkaProtobuf`: the union is a `oneof`, and members carry
  `alloy.proto#protoIndex`.
- `@kafkaAvro`: the union is an Avro union of records.
- Redis protocols: `ENVELOPE`, a one-entry map in CBOR.

The union is a plain tagged union, not `@streaming`. A single-event channel is
a union with one member.

`@kafkaKey` and `@kafkaHeader` apply to members of command inputs and event
structures alike.

### Infrastructure

`bote.infra#kafkaTopicConfig` targets the shape that carries the topic's
`@kafkaTopic` trait (selector `[trait|bote#kafkaTopic]`). A platform team
attaches it with `apply`.

### Validation

- Each channel is listed by exactly one service, and every union with an
  address trait is a listed channel.
- An address trait matches its service's protocol.
- An address has one owner and one use. A topic, stream or Pub/Sub channel is
  either one channel or the address of command operations, never both.
- Command operations that share an address agree on address-level values
  (`compacted`, `maxLen`, `kafkaTopicConfig`).
- Channel members target `@event` structures.
- An `@event` structure is never an operation input or output.
- `eventDiscrimination: NONE` requires every channel of the service to have
  one member.
- Kafka and Redis Pub/Sub command operations have no output.
- `@kafkaProtobuf` requires `protoIndex` on channel members and on every event
  and command member.

## Why events are not operations

Smithy models a subscription as an operation in `smithy.mqtt#subscribe`: the
input binds topic labels, and the output is a single event stream. That fits
MQTT because an MQTT subscription is a request. The client asks the broker
session for a stream, passes parameters, and the stream lives as long as the
subscription. A Smithy event stream has the same shape: it belongs to one
request and ends with it.

Durable logs work differently:

- The owner never receives a request from a consumer. It appends to the log
  whether anyone reads it or not.
- Consumption state (consumer group, offset, start position, replay) belongs
  to the consumer and outlives any single connection.
- Many independent consumer groups read the same channel.

An operation on the owner's service would put consumer concerns into the
owner's contract. Generic tooling would also read it as an RPC that the owner
serves once per subscriber. A channel shape states what the owner guarantees,
which is the address, the event set and the encoding, and nothing about how
it is read.

Subscribe-as-operation stays the right model for protocols where a
subscription is a request with parameters and a lifetime the server takes part
in, such as WebSocket, server-sent events or MQTT. A bote protocol for those
would use operations, and the channel model does not prevent that.

## Generation

### Shapes

Channel unions and their events are not reachable from the service through
member relationships, so a service walk does not find them. Generators build a
shape closure: the service closure plus every channel in `@channels` and its
members. Smithy's `CodegenDirector` supports this as combined mode: a
`ShapeClosure` that contains the primary service.

### Perspectives

One contract generates both sides:

| | Owner | Client |
| --- | --- | --- |
| Commands | Handles them, returns replies | Sends them, awaits replies |
| Channels | Publishes events | Consumes events |

In AsyncAPI, the `perspective` setting selects the side. In C#, the side
follows NSmithy's `generateServer` and `generateClient` settings. Consumer
group names, offsets and start positions are runtime configuration of the
consumer.

### AsyncAPI

- Each channel union is an AsyncAPI channel whose address is the topic, stream
  or Pub/Sub channel, with one message per union member.
- Each command operation is a channel plus an operation: `receive` for the
  owner, `send` for the client.
- Each event channel has an operation: `send` for the owner, `receive` for the
  client.
- Redis Streams request/reply operations carry an AsyncAPI reply with the
  dynamic `$message.header#/reply_to` address.

### C#

The C# generator is a `CSharpIntegration` for NSmithy. It adds the channels
through `additionalShapes()` and generates the messaging surface in place of
NSmithy's HTTP client and server through `serviceGenerator()`.

- Owner: one handler interface per command operation, and one publisher per
  channel with a method per event.
- Client: a command client with a method per command operation, and one
  handler interface per channel with a method per event, registered as a
  hosted consumer. Events the contract does not know go to
  `HandleUnknownAsync`, which skips them by default.
- Each broker's sender is registered by its own type, so one service can use
  several brokers.

Consumers configure the generator explicitly in `smithy-build.json`:

```json
{
    "version": "1.0",
    "maven": {
        "dependencies": [
            "io.github.thomaslaich.bote:bote:<version>",
            "io.github.thomaslaich.bote:smithy-csharp-codegen:<version>"
        ]
    }
}
```

The generator is built against one NSmithy codegen version and fails with a
clear message when it runs against another.

### Documentation

Documentation and diff tools that walk only the shapes a service reaches miss
channels and their events, which are the shapes where breaking changes cost
most. A catalog such as katalog includes them by also walking the shapes the
service's traits reference by `@idRef`, or a configured shape closure, and can
render channels as their own section on top of that.

The AsyncAPI generator is an interop output for AsyncAPI tooling.

## Open questions

1. **Parameterized addresses.** A topic such as `orders.{region}` needs labels
   bound to event members on publish and supplied (or wildcarded) on consume,
   similar to `smithy.mqtt#topicLabel`. This is the strongest case for a
   subscription with input, so the label design should come before any
   protocol that needs it.
2. **Single-event channels.** Allow `@kafkaTopic` on a structure as a shorthand
   for a one-member union, or keep unions only?
3. **AMQP.** An exchange maps to a channel, and routing keys map to union
   members. This needs checking against `designs/amqp.md` in history.
