$version: "2"

namespace tests.envelope

use bote#channels
use bote#event
use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic

@kafkaJson(eventDiscrimination: "ENVELOPE")
@channels([DeviceEvents])
service Device {
    version: "1"
    operations: [Dim, Reset]
}

@kafkaTopic(name: "envelope.commands")
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

@kafkaTopic(name: "envelope.commands")
operation Reset {
    input := {
        @required
        @kafkaKey
        deviceId: String
    }
}

@kafkaTopic(name: "envelope.events")
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
