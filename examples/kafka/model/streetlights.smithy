$version: "2"

// Avro contract: the device/product owns the messages because it defines the
// events it emits and the commands it accepts. Payload schemas are registered
// with a Schema Registry using topic-name subjects.
namespace examples.kafka.streetlights

use bote#avroCompatibility
use bote#channels
use bote#event
use bote#kafkaAvro
use bote#kafkaHeader
use bote#kafkaKey
use bote#kafkaTopic

/// The streetlight device API: clients consume lighting events and send dim commands.
@title("Streetlight Device API")
@avroCompatibility(mode: "BACKWARD")
@kafkaAvro
@channels([LightingEvents])
service StreetlightDevice {
    version: "1.0.0"
    operations: [
        DimLight
    ]
}

/// Dim a streetlight.
@kafkaTopic(name: "smartylighting.streetlights.action.dim")
operation DimLight {
    input: DimLightCommand
}

/// Command a particular streetlight to dim the lights.
@input
structure DimLightCommand {
    @kafkaKey
    streetlightId: String

    /// Percentage to which the light should be dimmed to.
    @range(min: 0, max: 100)
    percentage: Integer

    /// Date and time when the message was sent.
    sentAt: Timestamp
}

/// Environmental lighting events reported by streetlights.
@kafkaTopic(name: "smartylighting.streetlights.lighting.measured")
union LightingEvents {
    lightMeasured: LightMeasured
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
