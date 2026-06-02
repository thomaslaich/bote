$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Redis Streams using JSON serialization.
///
/// Producers append JSON-encoded entries to a stream (XADD); consumers read them
/// (XREAD / XREADGROUP). Streams are durable and replayable. A stream is declared
/// as a @redisStream channel shape, and operations bind to it with @channel — the
/// same first-class-channel model the Kafka protocol uses.
@protocolDefinition(
    traits: [bote#redisStream, bote#channel, bote#send, bote#receive]
)
@trait(selector: "service")
structure redisStreamsJson {}

/// Declares a Redis stream — an AsyncAPI channel — as a first-class, shareable shape.
///
/// Apply to an empty marker structure; operations bind to it with @channel. The
/// stream name and its configuration are declared once on the shape, so a producer
/// and a consumer reference the same channel and cannot drift.
@trait(selector: "structure")
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
/// Producers PUBLISH JSON messages to a channel; consumers SUBSCRIBE to receive
/// them. Pub/Sub is fire-and-forget: messages are not persisted, there is no
/// replay, and the channel carries no configuration. A channel is declared as a
/// @redisChannel shape, and operations bind to it with @channel.
@protocolDefinition(
    traits: [bote#redisChannel, bote#channel, bote#send, bote#receive]
)
@trait(selector: "service")
structure redisPubSubJson {}

/// Declares a Redis Pub/Sub channel as a first-class, shareable shape.
///
/// The channel owns nothing but its name, so this shape is thin — but keeping it a
/// shape (rather than a string on the operation) means every channel binds the same
/// way (@channel) and has a home for channel-level documentation.
@trait(selector: "structure")
structure redisChannel {
    /// The Redis Pub/Sub channel name (e.g. "presence").
    @required
    name: String
}
