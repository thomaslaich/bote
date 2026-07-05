# Design: AMQP protocol support

Status: draft for discussion. Nothing here is implemented.

## Goal and scope

Add an AMQP protocol family to bote, bringing the first broker with native
request-reply plumbing. This is where `@reply`, reserved since the Kafka
protocols dropped it, becomes real.

Scope decisions to confirm up front:

- **AMQP 0-9-1 (RabbitMQ), not AMQP 1.0.** They are different protocols with
  different models. 0-9-1 is what most people mean by AMQP, RabbitMQ is its
  dominant implementation, and the AsyncAPI `amqp` binding targets it. AMQP
  1.0 can become a separate protocol trait later if needed.
- **JSON encoding first** (`@amqpJson`), consistent with `@kafkaJson` and the
  Redis protocols. Other encodings can follow the established
  one-trait-per-encoding pattern.

## How AMQP primitives map to the ownership model

AMQP separates the publisher-facing name (exchange) from the consumer-facing
name (queue), connected by bindings. This maps onto bote's ownership thesis
better than any broker so far:

| AMQP primitive | Owner | In the contract? |
|---|---|---|
| Exchange (name, type) | Contract owner | Yes. It is the address clients need, like a Kafka topic. |
| Routing keys the owner uses | Contract owner | Yes. They are how clients select events. |
| The owner's command queue and its binding | Contract owner | No. How the owner consumes its own commands is its deployment. |
| Consumer queues and their bindings | Each consumer | No. Queue names, prefetch, exclusivity are consumer deployment. |
| Durability, TTLs, DLX, lazy mode | Platform or owner | No. Provisioning, goes to `bote.infra`. |

Consequence: a bote AMQP channel is an **exchange**. Queues never appear in
the contract. This is the main structural difference from Kafka and Redis,
where one address string serves both sides.

## Trait sketch

AMQP 0-9-1's native verbs are `basic.publish` and `basic.consume`.

```smithy
/// Client publishes a @command to the exchange. Fire-and-forget.
@trait(selector: "operation")
structure amqpPublish {
    @required
    exchange: String

    /// Routing key the client must use. Defaults to the empty string.
    routingKey: String
}

/// Client publishes a @command and receives a @reply. See request-reply.
@trait(selector: "operation")
structure amqpRequest {
    @required
    exchange: String

    routingKey: String
}

/// Client consumes @events from the exchange via a queue it binds itself.
@trait(selector: "operation")
structure amqpConsume {
    @required
    exchange: String

    /// The exchange type, which defines what binding patterns clients can
    /// use. Part of the contract: changing it breaks every consumer binding.
    @required
    exchangeType: AmqpExchangeType
}

enum AmqpExchangeType {
    DIRECT
    TOPIC
    FANOUT
}

/// Maps a member to an entry in the AMQP headers table (application
/// headers), analogous to @kafkaHeader. Not serialized into the JSON value.
@trait(selector: "structure > member")
structure amqpHeader {
    @required
    name: String
}
```

`bote.infra` gains `amqpExchangeConfig` (durable, autoDelete, alternate
exchange) with the same rules as `kafkaTopicConfig`: applied to at most one
operation per exchange, attachable from a separate model file with `apply`.

Headers exchanges are left out until someone needs them; their binding
arguments do not fit the routing-key model below.

## Request-reply

The reason to do AMQP at all. Proposed contract surface:

- `@amqpRequest` operations have a `@command` input and a `@reply` output.
  This is the first protocol whose validator accepts an operation output.
- The reply address is **not** in the contract. AMQP carries it per message:
  the client sets the `reply_to` property (typically RabbitMQ's Direct
  Reply-To pseudo-queue or an exclusive callback queue) and a
  `correlation_id`; the owner replies to `reply_to` echoing the
  `correlation_id`. Both are standard message properties, so no bote member
  traits are needed. The contract defines only the reply payload shape.
- Wire rule: the reply value is the bare JSON serialization of the `@reply`
  structure, `correlation_id` mandatory on both legs.

Validator changes:

- `OperationBindingValidator` grows a third operation kind. Publish: command
  input, no output. Request: command input, `@reply` output required.
  Consume: streaming `@event` output.
- The "no current protocol supports replies" error message retires.

## Event discrimination: routing keys

AMQP has a native discriminator that Kafka lacks: the routing key travels
with every message and topic exchanges route on it. Proposal:

- Each member of the `@streaming` union gets routing key equal to its member
  name by default, overridable with a member trait:

```smithy
@amqpConsume(exchange: "orders", exchangeType: TOPIC)
operation ConsumeOrderEvents {
    output := { events: OrderEvents }
}

@streaming
union OrderEvents {
    @amqpRoutingKey("order.placed")
    placed: OrderPlaced

    @amqpRoutingKey("order.shipped")
    shipped: OrderShipped
}
```

- Wire rule: event values are bare JSON; the routing key is the
  discriminator. No envelope. This follows the pattern that each protocol
  uses its transport's native mechanism (protobuf: oneof, Avro: schema id,
  AMQP: routing key), and it lets consumers bind selectively, e.g. only
  `order.shipped`.
- For `FANOUT` exchanges routing keys are ignored, so the union may declare
  at most one event type (validator rule, same shape as `NONE` in
  `@kafkaJson`).
- Open question: should `DIRECT`/`TOPIC` consume operations require explicit
  `@amqpRoutingKey` on every member rather than defaulting to member names?
  Explicit matches the `@protoIndex` precedent (renaming a member must not
  silently change the wire).

## Channel consistency

`ChannelConsistencyValidator` extends naturally. Channel identity is the
exchange name (family `amqp`). Rules:

- One owning service per exchange.
- All operations on an exchange agree on `exchangeType` (publish and request
  traits do not carry it; only consume does, so agreement applies among
  consume operations; see open questions).
- `amqpExchangeConfig` on at most one operation per exchange.
- Routing keys of a topic exchange's events must be unique within the
  exchange.

## AsyncAPI mapping

Using the AsyncAPI `amqp` binding (0.3.0):

| bote | AsyncAPI 3.1 |
|---|---|
| exchange | a `channel` with `bindings.amqp.is: routingKey` and the `exchange` object (name, type) |
| `@amqpPublish` / `@amqpConsume` | `operation` with action per perspective, `bindings.amqp.cc` carrying routing keys |
| `@amqpRequest` | `operation` plus AsyncAPI 3 `reply` object; reply address location `$message.header#/replyTo` (dynamic, per message) |
| `@amqpHeader` | message `headers` schema |
| `@amqpRoutingKey` | operation binding `cc` entries |
| `bote.infra#amqpExchangeConfig` | exchange object `durable` / `autoDelete` |

The generator's perspective setting applies unchanged: owner documents
receive commands and requests, send events and replies.

## Out of scope for the first cut

- AMQP 1.0.
- Headers exchanges and binding arguments.
- Modeling consumer queues, prefetch, acknowledgement modes.
- Dead-letter topology (belongs in `bote.infra` later, if at all).
- Avro or Protobuf over AMQP.

## Open questions

1. Routing keys: explicit `@amqpRoutingKey` required everywhere, or member
   name as default? (Lean: required, per the `@protoIndex` argument.)
2. Should `@amqpPublish`/`@amqpRequest` also carry `exchangeType` so
   publish-only contracts document it, at the cost of repetition that the
   consistency validator must then check?
3. Does `@amqpRequest` belong on the exchange model at all, or should
   requests target the default exchange with a queue-name routing key
   (direct-to-queue), which is the more common RPC layout in practice?
4. Is `correlation_id` worth surfacing in the AsyncAPI document as a
   `correlationId` object? (AsyncAPI supports it; cheap to emit.)
5. Naming: `@amqpConsume` vs `@amqpSubscribe`. `basic.consume` is the wire
   verb; subscribe reads better next to Redis. (Lean: consume, wire verb
   wins, consistent with the produce/consume decision for Kafka.)

## Sequencing

Recommended order: NSmithy `@kafkaJson` preview first to validate the core
spec with an external consumer, then this design, revised by whatever the
preview surfaces, then implementation.
