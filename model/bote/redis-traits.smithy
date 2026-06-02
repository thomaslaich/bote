$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Redis Streams using JSON serialization.
///
/// Invocations append JSON-encoded entries to a stream (XADD); subscriptions read
/// them (XREAD / XREADGROUP). Apply @redisStream directly to the operation.
@protocolDefinition(
    traits: [bote#redisStream, bote#invocation, bote#subscription, bote#event, bote#command, bote#reply]
)
@trait(selector: "service")
structure redisStreamsJson {}

/// Declares the Redis stream an operation invokes or subscribes to.
@trait(selector: "operation")
structure redisStream {
    /// The Redis stream key (e.g. "chat:messages").
    @required
    name: String

    /// Caps the stream length (approximate XADD MAXLEN). Older entries are
    /// trimmed once the stream grows past this many entries. Omit for unbounded.
    @range(min: 1)
    maxLen: Long
}

/// A Smithy protocol for Redis Pub/Sub using JSON serialization.
///
/// Invocations PUBLISH JSON messages to a channel; subscriptions receive them.
/// Pub/Sub is fire-and-forget: messages are not persisted, there is no replay,
/// and the channel carries no configuration. Apply @redisChannel directly to the
/// operation.
@protocolDefinition(
    traits: [bote#redisChannel, bote#invocation, bote#subscription, bote#event, bote#command, bote#reply]
)
@trait(selector: "service")
structure redisPubSubJson {}

/// Declares the Redis Pub/Sub channel an operation invokes or subscribes to.
@trait(selector: "operation")
structure redisChannel {
    /// The Redis Pub/Sub channel name (e.g. "presence").
    @required
    name: String
}
