$version: "2"

// The mirror-image perspective: the management backend. It *consumes*
// measurements and *produces* dim commands. Generated document:
// StreetlightsBackend.asyncapi.json.
//
// Its own namespace, depending on smartylighting.shared for channels and message
// types. Compare its channel sections to the device's: identical addresses,
// bindings and message schemas (both come from the shared model) — only the
// operation actions are flipped.
namespace smartylighting.backend

use bote#channel
use bote#kafkaJson
use bote#receive
use bote#send
use smartylighting.shared#DimActionChannel
use smartylighting.shared#DimLight
use smartylighting.shared#LightMeasured
use smartylighting.shared#LightingMeasuredChannel

/// The streetlight-management backend: consumes measurements, issues dim commands.
@title("Streetlights Backend API")
@kafkaJson
service StreetlightsBackend {
    version: "1.0.0"
    operations: [
        ReceiveLightMeasured
        SendDimCommand
    ]
}

/// Consume environmental lighting measurements.
@receive
@channel(LightingMeasuredChannel)
operation ReceiveLightMeasured {
    output := {
        measurements: LightMeasuredStream
    }
}

// This backend's local view of the measured channel: the messages it consumes.
// The wrapped LightMeasured type is the shared contract; this union is not.
@streaming
union LightMeasuredStream {
    lightMeasured: LightMeasured
}

/// Command a streetlight to dim its lights.
@send
@channel(DimActionChannel)
operation SendDimCommand {
    input: DimLight
}
