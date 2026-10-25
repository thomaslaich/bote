$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Redis Streams using JSON serialization.
///
/// Clients XADD JSON-encoded command entries to an operation's stream, and
/// read the owner's events from a channel's stream with XREAD / XREADGROUP.
///
/// Wire rules: command values are the bare JSON serialization of their
/// structure. @event values are wrapped in a single-key envelope whose key is
/// the channel union member name (the restJson1 tagged-union idiom), so
/// consumers of a multi-event stream can tell event types apart.
///
/// A @redisStream operation with an output is request/reply. The request
/// carries reply_to and correlation_id transport metadata. The requester
/// listens on the temporary Pub/Sub channel named by reply_to before adding
/// the request to the stream. Owner replicas consume requests through one
/// shared consumer group, so each delivery is assigned to one replica; it
/// publishes one reply to reply_to and echoes correlation_id. Requests can be
/// redelivered after failures. Reply delivery is at-most-once even though the
/// request itself is durable.
@protocolDefinition(
    traits: [bote#redisStream, bote#channels, bote#event]
)
@trait(selector: "service")
structure redisStreamsJson {}

/// A Smithy protocol for Redis Streams using CBOR serialization.
///
/// Identical to @redisStreamsJson except for the encoding: values are the
/// CBOR serialization rpcv2Cbor defines, and an @event's envelope is a
/// one-entry map keyed by the channel union member name.
@protocolDefinition(
    traits: [bote#redisStream, bote#channels, bote#event]
)
@trait(selector: "service")
structure redisStreamsCbor {}

/// Binds an operation or a channel to a Redis stream.
///
/// On an operation, the owner reads the operation's input, a command, from
/// the stream. With no output the operation is fire-and-forget; an output
/// makes it request/reply using a temporary Pub/Sub reply channel. On a
/// channel union, the owner appends the union's events to the stream.
@trait(selector: ":is(operation, union)")
structure redisStream {
    /// The Redis stream key (e.g. "chat:messages").
    @required
    key: String

    /// Caps the stream length (approximate XADD MAXLEN). Older entries are
    /// trimmed once the stream grows past this many entries; for a channel
    /// this is the replay bound for readers. Omit for unbounded. Must agree
    /// with every other operation on the same stream.
    @range(min: 1)
    maxLen: Long
}

/// A Smithy protocol for Redis Pub/Sub using JSON serialization.
///
/// Clients PUBLISH JSON commands to an operation's Pub/Sub channel and
/// SUBSCRIBE to a channel union's Pub/Sub channel for the owner's events.
/// Pub/Sub is fire-and-forget: messages are not persisted, there is no
/// replay, every active subscriber receives each message, and the address
/// carries no configuration. Operations must not define an output;
/// request/reply uses Redis Streams instead.
///
/// Wire rules: command values are the bare JSON serialization of their
/// structure. @event values are wrapped in a single-key envelope whose key is
/// the channel union member name (the restJson1 tagged-union idiom), so
/// consumers of a multi-event channel can tell event types apart.
@protocolDefinition(
    traits: [bote#redisChannel, bote#channels, bote#event]
)
@trait(selector: "service")
structure redisPubSubJson {}

/// A Smithy protocol for Redis Pub/Sub using CBOR serialization.
///
/// Identical to @redisPubSubJson except for the encoding: values are the
/// CBOR serialization rpcv2Cbor defines, and an @event's envelope is a
/// one-entry map keyed by the channel union member name.
@protocolDefinition(
    traits: [bote#redisChannel, bote#channels, bote#event]
)
@trait(selector: "service")
structure redisPubSubCbor {}

/// Binds an operation or a channel to a Redis Pub/Sub channel.
///
/// On an operation, the owner receives the operation's input, a command,
/// from the Pub/Sub channel. On a channel union, the owner publishes the
/// union's events to it.
@trait(selector: ":is(operation, union)")
structure redisChannel {
    /// The Redis Pub/Sub channel name (e.g. "presence").
    @required
    name: String
}
