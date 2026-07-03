$version: "2"

// Topic provisioning for the example contracts. This file is deliberately
// separate from the contract models: the services own their commands and
// events, while a platform team owns partitions, replication, and retention.
// bote.infra#kafkaTopicConfig is attached with apply so the two concerns can
// be authored — and owned — independently.
namespace examples.kafka.infra

use bote.infra#kafkaTopicConfig

apply examples.kafka.orders#ConsumeOrderEvents @kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)

apply examples.kafka.streetlights#ConsumeLightingEvents @kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)
