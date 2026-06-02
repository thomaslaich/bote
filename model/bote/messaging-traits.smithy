$version: "2"

namespace bote

// Broker-agnostic core. These traits carry no Kafka- or Redis-specific meaning —
// they describe the *shape* of an event-driven contract (direction of flow, and
// which channel an operation acts on). Each broker supplies its own address/config
// trait on the channel structure (@kafkaTopic, @redisStream, @redisChannel, ...).
/// Marks a service as an application-level messaging contract.
///
/// Operations may bind to channels from different brokers. Code generators can
/// emit this service as a single application view (for example, an AsyncAPI
/// document). The broker-specific address/config traits live on the channel
/// shapes, not on the service.
@trait(selector: "service")
structure messaging {
    /// The generated document's default content type.
    /// Omit for application/json.
    defaultContentType: String
}

/// Binds an operation to the channel it sends to or receives from.
///
/// The referenced shape is a structure carrying a broker address trait
/// (@kafkaTopic, @redisStream or @redisChannel) — the channel. This is the
/// AsyncAPI relationship "operation -> channel": a single operation acts on a
/// single channel. Modelling the channel as a shared shape (rather than a string
/// repeated on each operation) lets its address and configuration be declared once
/// and distributed as part of the contract, like the message payload types.
@trait(selector: "operation")
@idRef(
    failWhenMissing: true
    selector: ":is([trait|bote#kafkaTopic], [trait|bote#redisStream], [trait|bote#redisChannel])"
)
string channel

/// Marks an operation as sending messages to its channel.
/// The operation input is the message value written to the channel.
@trait(
    selector: "operation"
    conflicts: [bote#receive]
)
structure send {}

/// Marks an operation as receiving messages from its channel.
/// The operation output must contain a member targeting a @streaming union,
/// where each union member is a possible event type on the channel.
@trait(
    selector: "operation"
    conflicts: [bote#send]
)
structure receive {}
