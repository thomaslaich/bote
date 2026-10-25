$version: "2"

// Protobuf contract: sensors record readings, the service publishes threshold
// alerts. Every payload member carries an explicit @protoIndex; the alert
// channel union becomes a proto message with a oneof, which doubles as the
// event discriminator on the wire.
namespace examples.kafka.telemetry

use alloy.proto#protoIndex
use bote#channels
use bote#event
use bote#kafkaKey
use bote#kafkaProtobuf
use bote#kafkaTopic

/// The telemetry API: sensors record readings and clients consume alerts.
@title("Telemetry API")
@kafkaProtobuf
@channels([Alerts])
service Telemetry {
    version: "1.0.0"
    operations: [
        RecordReading
    ]
}

/// Record a sensor reading.
@kafkaTopic(name: "telemetry.readings")
operation RecordReading {
    input: RecordReadingCommand
}

/// Command to record a sensor reading.
@input
structure RecordReadingCommand {
    @protoIndex(1)
    @kafkaKey
    sensorId: String

    /// Temperature in thousandths of a degree Celsius.
    @protoIndex(2)
    milliCelsius: Integer

    @protoIndex(3)
    recordedAt: Timestamp
}

/// Threshold alerts derived from the readings.
@kafkaTopic(name: "telemetry.alerts")
union Alerts {
    @protoIndex(1)
    thresholdExceeded: ThresholdExceeded
}

/// A sensor reading crossed its configured threshold.
@event
structure ThresholdExceeded {
    @protoIndex(1)
    @kafkaKey
    sensorId: String

    @protoIndex(2)
    milliCelsius: Integer

    @protoIndex(3)
    thresholdMilliCelsius: Integer

    @protoIndex(4)
    observedAt: Timestamp
}
