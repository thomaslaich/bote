$version: "2"

// The shared, application-neutral substrate: the topics (channels) and the
// message payload types. In production this would be its own published artifact
// that both the producing and consuming services depend on — neither owns the
// other's perspective, they only share these declarations.
//
// Each channel is a marker structure carrying the topic's
// name/partitions/retention/compaction, declared exactly once. Every service that
// binds to a channel emits an identical AsyncAPI channel section for the messages it
// sends or receives — they differ only in whether they @send or @receive.
//
// This lives in its own namespace (smartylighting.shared) precisely because it is
// the shared artifact both services depend on — modelling, in one repo, what would
// be a published types JAR across repos.
namespace smartylighting.shared

use bote#kafkaHeader
use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig

// ---------------------------------------------------------------------------
// Channels (topics)
// ---------------------------------------------------------------------------
/// Environmental lighting measurements reported by streetlights.
@kafkaTopic(name: "smartylighting.streetlights.lighting.measured")
@kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)
structure LightingMeasuredChannel {}

/// Commands instructing a streetlight to dim its lights.
@kafkaTopic(name: "smartylighting.streetlights.action.dim")
structure DimActionChannel {}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------
/// Light intensity reported by a streetlight.
structure LightMeasured {
    /// Routes all measurements for one streetlight to the same partition.
    @kafkaKey
    streetlightId: String

    /// Light intensity measured in lumens.
    @range(min: 0)
    lumens: Integer

    /// Identifies the producing application; carried as a Kafka header.
    @kafkaHeader(name: "my-app-id")
    appId: String

    /// Date and time when the message was sent.
    sentAt: Timestamp
}

/// Command a particular streetlight to dim the lights.
structure DimLight {
    @kafkaKey
    streetlightId: String

    /// Percentage to which the light should be dimmed to.
    @range(min: 0, max: 100)
    percentage: Integer

    /// Date and time when the message was sent.
    sentAt: Timestamp
}
