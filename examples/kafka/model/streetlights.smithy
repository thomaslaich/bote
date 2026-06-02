$version: "2"

// The streetlight device contract. The device/product owns the messages because
// it defines the events it emits and the commands it accepts.
namespace examples.kafka.streetlights

use bote#command
use bote#event
use bote#invocation
use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig
use bote#subscription

/// The streetlight device API: clients subscribe to lighting events and invoke dimming.
@title("Streetlight Device API")
@kafkaJson
service StreetlightDevice {
    version: "1.0.0"
    operations: [
        SubscribeToLightingEvents
        InvokeDimLight
    ]
}

/// Subscribe to environmental lighting events reported by streetlights.
@subscription
@kafkaTopic(name: "smartylighting.streetlights.lighting.measured")
@kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)
operation SubscribeToLightingEvents {
    output := {
        events: LightMeasuredStream
    }
}

/// Dim a streetlight.
@invocation
@kafkaTopic(name: "smartylighting.streetlights.action.dim")
operation InvokeDimLight {
    input: DimLight
}

/// Light intensity reported by a streetlight.
@event
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
@command
structure DimLight {
    @kafkaKey
    streetlightId: String

    /// Percentage to which the light should be dimmed to.
    @range(min: 0, max: 100)
    percentage: Integer

    /// Date and time when the message was sent.
    sentAt: Timestamp
}

/// The client's subscription view of device lighting events.
@streaming
union LightMeasuredStream {
    lightMeasured: LightMeasured
}
