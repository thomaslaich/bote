$version: "2"

namespace tests.header

use bote#channels
use bote#event
use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic

@kafkaJson(eventDiscrimination: "HEADER")
@channels([DeviceEvents])
service Device {
    version: "1"
    operations: [Dim]
}

@kafkaTopic(name: "header.commands")
operation Dim {
    input := {
        @required
        @kafkaKey
        deviceId: String

        @required
        @kafkaHeader(name: "trace-id")
        traceId: String
    }
}

@kafkaTopic(name: "header.events")
union DeviceEvents {
    @jsonName("measured-event")
    measured: Measured
}

@event
structure Measured {
    @required
    @kafkaKey
    deviceId: String

    @required
    @kafkaHeader(name: "source")
    source: String

    @required
    lumens: Integer
}
