$version: "2"

// Protobuf contract: sensors record readings, clients consume threshold
// alerts. Every payload member carries an explicit @protoIndex; the alert
// stream union becomes a proto message with a oneof, which doubles as the
// event discriminator on the wire.
namespace examples.kafka.telemetry

use alloy.proto#protoIndex
use bote#command
use bote#event
use bote#kafkaConsume
use bote#kafkaKey
use bote#kafkaProduce
use bote#kafkaProtobuf

/// The telemetry API: sensors record readings and clients consume alerts.
@title("Telemetry API")
@kafkaProtobuf
service Telemetry {
    version: "1.0.0"
    operations: [
        RecordReading
        ConsumeAlerts
    ]
}

/// Record a sensor reading.
@kafkaProduce(topic: "telemetry.readings")
operation RecordReading {
    input: RecordReadingCommand
}

/// Consume threshold alerts derived from the readings.
@kafkaConsume(topic: "telemetry.alerts")
operation ConsumeAlerts {
    output := {
        alerts: AlertStream
    }
}

/// Command to record a sensor reading.
@command
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

/// The client's subscription view of telemetry alerts.
@streaming
union AlertStream {
    @protoIndex(1)
    thresholdExceeded: ThresholdExceeded
}
