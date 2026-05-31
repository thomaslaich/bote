$version: "2"

namespace bote

/// Binds an operation to a Kafka topic.
@trait(selector: "operation")
structure kafkaTopic {
    /// The Kafka topic name.
    @required
    name: String

    /// Whether the topic uses log compaction.
    /// Only the latest message per key is retained.
    /// Defaults to false.
    compacted: Boolean
}

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
