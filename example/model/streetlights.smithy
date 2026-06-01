$version: "2"

// Example: a Kafka-flavoured port of AsyncAPI's "Streetlights Kafka API"
// sample (https://studio.asyncapi.com).
//
// The official sample templates the streetlight id into the channel address
// (an MQTT/AMQP idiom). Kafka topics are static, so here the per-streetlight
// discriminator is the message *key* (@kafkaKey) instead — the same data,
// modelled the Kafka way.
namespace smartylighting

use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig
use bote#receive
use bote#send

/// The Smartylighting Streetlights API allows you to remotely manage the city lights.
///
/// ### Check out its awesome features:
///
/// * Receive real-time information about environmental lighting conditions 📈
/// * Turn a specific streetlight on/off 🌃
/// * Dim a specific streetlight 😎
@title("Streetlights Kafka API")
@kafkaJson
service StreetlightsKafka {
    version: "1.0.0"
    operations: [
        PublishLightMeasured
        ReceiveLightMeasured
        TurnOn
        TurnOff
        Dim
    ]
}

// ---------------------------------------------------------------------------
// Lighting-measured topic (produced and consumed)
// ---------------------------------------------------------------------------
/// Produce environmental lighting measurements for a streetlight.
@send
@kafkaTopic(name: "smartylighting.streetlights.lighting.measured")
@kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)
operation PublishLightMeasured {
    input: LightMeasured
}

/// Inform about environmental lighting conditions of a particular streetlight.
@receive
@kafkaTopic(name: "smartylighting.streetlights.lighting.measured")
operation ReceiveLightMeasured {
    output := {
        measurements: LightMeasuredStream
    }
}

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

@streaming
union LightMeasuredStream {
    lightMeasured: LightMeasured
}

// ---------------------------------------------------------------------------
// Turn-on / turn-off topics
// ---------------------------------------------------------------------------
/// Command a particular streetlight to turn the lights on.
@send
@kafkaTopic(name: "smartylighting.streetlights.action.turn.on")
operation TurnOn {
    input: TurnOnOff
}

/// Command a particular streetlight to turn the lights off.
@send
@kafkaTopic(name: "smartylighting.streetlights.action.turn.off")
operation TurnOff {
    input: TurnOnOff
}

/// Command a particular streetlight to turn the lights on or off.
structure TurnOnOff {
    @kafkaKey
    streetlightId: String

    /// Whether to turn on or off the light.
    command: Command

    /// Date and time when the message was sent.
    sentAt: Timestamp
}

/// Whether to turn the light on or off.
enum Command {
    ON = "on"
    OFF = "off"
}

// ---------------------------------------------------------------------------
// Dim topic
// ---------------------------------------------------------------------------
/// Command a particular streetlight to dim the lights.
@send
@kafkaTopic(name: "smartylighting.streetlights.action.dim")
operation Dim {
    input: DimLight
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
