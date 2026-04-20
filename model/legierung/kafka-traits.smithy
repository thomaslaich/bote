$version: "2"

namespace legierung

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

/// Marks an operation as a Kafka producer.
/// The operation input is the message value written to the topic.
@trait(selector: "operation")
structure kafkaProducer {}

/// Marks an operation as a Kafka consumer.
/// The operation output is the message value read from the topic.
@trait(selector: "operation")
structure kafkaConsumer {
    /// The consumer group ID.
    @required
    group: String
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
