$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Redis Streams using JSON serialization.
///
/// Clients XADD JSON-encoded entries to a stream (@redisStreamAdd) and read
/// them with XREAD / XREADGROUP (@redisStreamRead).
///
/// Wire rules: @command values are the bare JSON serialization of their
/// structure. @event values are wrapped in a single-key envelope whose key is
/// the @streaming union member name (the restJson1 tagged-union idiom), so
/// consumers of a multi-event stream can tell event types apart.
@protocolDefinition(
    traits: [bote#redisStreamAdd, bote#redisStreamRead, bote#event, bote#command]
)
@trait(selector: "service")
structure redisStreamsJson {}

/// Marks an operation as an XADD capability: clients may append the
/// operation's input — a @command structure — to the given Redis stream.
/// Add operations must not define an output.
@trait(
    selector: "operation"
    conflicts: [bote#redisStreamRead]
)
structure redisStreamAdd {
    /// The Redis stream key (e.g. "chat:messages").
    @required
    stream: String

    /// Caps the stream length (approximate XADD MAXLEN). Older entries are
    /// trimmed once the stream grows past this many entries. Omit for
    /// unbounded. Must agree with every other operation on the same stream.
    @range(min: 1)
    maxLen: Long
}

/// Marks an operation as an XREAD capability: clients may read the
/// operation's events from the given Redis stream. The operation output must
/// contain a member targeting a @streaming union whose members are @event
/// structures.
@trait(
    selector: "operation"
    conflicts: [bote#redisStreamAdd]
)
structure redisStreamRead {
    /// The Redis stream key (e.g. "chat:messages").
    @required
    stream: String

    /// Caps the stream length (approximate XADD MAXLEN). Documents the
    /// channel's replay bound for readers. Must agree with every other
    /// operation on the same stream.
    @range(min: 1)
    maxLen: Long
}

/// A Smithy protocol for Redis Pub/Sub using JSON serialization.
///
/// Clients PUBLISH JSON messages to a channel (@redisPublish) and SUBSCRIBE
/// to them (@redisSubscribe). Pub/Sub is fire-and-forget: messages are not
/// persisted, there is no replay, and the channel carries no configuration.
///
/// Wire rules: @command values are the bare JSON serialization of their
/// structure. @event values are wrapped in a single-key envelope whose key is
/// the @streaming union member name (the restJson1 tagged-union idiom), so
/// consumers of a multi-event channel can tell event types apart.
@protocolDefinition(
    traits: [bote#redisPublish, bote#redisSubscribe, bote#event, bote#command]
)
@trait(selector: "service")
structure redisPubSubJson {}

/// Marks an operation as a PUBLISH capability: clients may publish the
/// operation's input — a @command structure — to the given Redis Pub/Sub
/// channel. Publish operations must not define an output.
@trait(
    selector: "operation"
    conflicts: [bote#redisSubscribe]
)
structure redisPublish {
    /// The Redis Pub/Sub channel name (e.g. "presence").
    @required
    channel: String
}

/// Marks an operation as a SUBSCRIBE capability: clients may subscribe to the
/// operation's events on the given Redis Pub/Sub channel. The operation
/// output must contain a member targeting a @streaming union whose members
/// are @event structures.
@trait(
    selector: "operation"
    conflicts: [bote#redisPublish]
)
structure redisSubscribe {
    /// The Redis Pub/Sub channel name (e.g. "presence").
    @required
    channel: String
}
