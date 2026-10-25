$version: "2"

namespace bote

/// Binds an operation or a channel to a Kafka topic.
///
/// On an operation, the owner receives the operation's input, a command, from
/// the topic. Kafka operations have no output. On a channel union, the owner
/// publishes the union's events to the topic.
@trait(selector: ":is(operation, union)")
structure kafkaTopic {
    /// The Kafka topic name.
    @required
    name: String

    /// Whether the topic uses log compaction: only the latest message per
    /// key is retained, giving the channel table semantics rather than log
    /// semantics. This is a contract-level promise consumers rely on, which
    /// is why it lives here and not in bote.infra#kafkaTopicConfig. Must
    /// agree with every other operation on the same topic. Defaults to false.
    compacted: Boolean
}

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
