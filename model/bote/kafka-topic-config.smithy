$version: "2"

namespace bote

use smithy.api#range

/// Declares infrastructure configuration for a Kafka topic.
///
/// Applied to a @kafkaTopic structure (the channel). Because the topic is a
/// single shared shape, its configuration is declared exactly once — there is
/// no need to repeat it across operations or validate cross-operation
/// consistency.
///
/// These values are the authoritative specification of the topic's durability
/// and retention contract. Code generators can use them to drive topic
/// creation via IaC (Terraform, Pulumi, etc.) without requiring a separate
/// configuration file.
///
/// All fields are optional. Omitted fields inherit broker-level defaults.
@trait(selector: "structure [trait|bote#kafkaTopic]")
structure kafkaTopicConfig {
    /// Number of partitions.
    ///
    /// Determines maximum consumer parallelism: a consumer group can have
    /// at most one active member per partition. Also affects ordering:
    /// all messages with the same key are routed to the same partition,
    /// preserving per-key order.
    ///
    /// Increasing partitions on an existing topic is possible but breaks
    /// key-based ordering for messages already written.
    @range(min: 1)
    partitions: Integer

    /// Number of replicas across brokers.
    ///
    /// Determines durability: a replication factor of N means the topic
    /// survives the loss of N-1 brokers without data loss. Must not exceed
    /// the number of brokers in the cluster.
    ///
    /// Production topics should use at least 3.
    @range(min: 1)
    replicationFactor: Integer

    /// How long messages are retained, in milliseconds.
    ///
    /// Set to -1 for infinite time-based retention. For compacted topics,
    /// -1 is typical — compaction governs cleanup instead of time.
    ///
    /// Retention is triggered by whichever of retentionMs or retentionBytes
    /// is reached first.
    @range(min: -1)
    retentionMs: Long

    /// Maximum total size of retained messages per partition, in bytes.
    ///
    /// Set to -1 for unlimited size-based retention.
    ///
    /// Retention is triggered by whichever of retentionMs or retentionBytes
    /// is reached first.
    @range(min: -1)
    retentionBytes: Long

    /// Minimum number of in-sync replicas required to acknowledge a write.
    ///
    /// When used with producer acks=all, guarantees that a write survives
    /// the loss of (replicationFactor - minInsyncReplicas) brokers.
    ///
    /// Must be less than or equal to replicationFactor. A value of 1
    /// provides no stronger guarantee than acks=1.
    @range(min: 1)
    minInsyncReplicas: Integer

    /// Maximum size of a single message in bytes.
    ///
    /// Messages exceeding this limit are rejected by the broker at produce
    /// time. Should be coordinated with the broker-level message.max.bytes
    /// and the consumer's fetch.max.bytes settings.
    @range(min: 1)
    maxMessageBytes: Integer
}
