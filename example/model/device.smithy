$version: "2"

// One application's perspective: the streetlight device firmware. It *produces*
// measurements and *consumes* dim commands. Generated document:
// StreetlightDevice.asyncapi.json.
//
// It lives in its own namespace and depends on smartylighting.shared for the
// channels and message types — it owns only its operations. The backend
// (backend.smithy) binds to the very same channel shapes with the opposite
// @send/@receive, which is how each document stays single-perspective while the
// underlying contract is shared.
namespace smartylighting.device

use bote#channel
use bote#kafkaJson
use bote#receive
use bote#send
use smartylighting.shared#DimActionChannel
use smartylighting.shared#DimLight
use smartylighting.shared#LightMeasured
use smartylighting.shared#LightingMeasuredChannel

/// The streetlight device firmware: reports measurements, obeys dim commands.
@title("Streetlight Device API")
@kafkaJson
service StreetlightDevice {
    version: "1.0.0"
    operations: [
        ReportLightMeasured
        ReceiveDimCommand
    ]
}

/// Report environmental lighting conditions to the measured topic.
@send
@channel(LightingMeasuredChannel)
operation ReportLightMeasured {
    input: LightMeasured
}

/// Receive dim commands addressed to this streetlight.
@receive
@channel(DimActionChannel)
operation ReceiveDimCommand {
    output := {
        commands: DimLightStream
    }
}

// This device's local view of the dim channel: the messages it consumes.
// The wrapped DimLight type is the shared contract; this union is not.
@streaming
union DimLightStream {
    dimLight: DimLight
}
