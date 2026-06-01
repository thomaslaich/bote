$version: "2"

namespace bote

/// Declares a Kafka topic — an AsyncAPI channel — as a first-class, shareable shape.
///
/// Apply to an empty marker structure; operations bind to it with @channel.
/// Modelling the topic as a shape (rather than repeating a name string on every
/// operation) lets the topic — its name, compaction, partitions and retention —
/// be defined once and distributed as part of the contract, exactly like the
/// message payload types. Producer and consumer services in different repos then
/// reference the same channel shape, so their AsyncAPI channel sections come out
/// identical by construction.
@trait(selector: "structure")
structure kafkaTopic {
    /// The Kafka topic name.
    @required
    name: String

    /// Whether the topic uses log compaction.
    /// Only the latest message per key is retained.
    /// Defaults to false.
    compacted: Boolean
}

/// Binds an operation to the topic (channel) it sends to or receives from.
///
/// The referenced shape must be a structure carrying @kafkaTopic. This is the
/// AsyncAPI relationship "operation -> channel": a single operation acts on a
/// single channel.
@trait(selector: "operation")
@idRef(failWhenMissing: true, selector: "[trait|bote#kafkaTopic]")
string channel

/// Marks an operation as sending messages to a Kafka topic.
/// The operation input is the message value written to the topic.
@trait(
    selector: "operation"
    conflicts: [bote#receive]
)
structure send {}

/// Marks an operation as receiving messages from a Kafka topic.
/// The operation output must contain a member targeting a @streaming union,
/// where each union member is a possible event type on the topic.
@trait(
    selector: "operation"
    conflicts: [bote#send]
)
structure receive {}

/// Marks a structure member as the Kafka message key.
/// Only one member per structure may carry this trait.
/// The member must be a simple type (String, Integer, Long, etc.).
@trait(selector: "structure > member")
structure kafkaKey {}

/// Maps a structure member to a Kafka message header.
@trait(selector: "structure > member")
structure kafkaHeader {
    /// The Kafka header name.
    @required
    name: String
}
