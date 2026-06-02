$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Kafka using JSON serialization.
///
/// Services annotated with this trait exchange JSON-encoded messages over
/// Kafka. Operations must be annotated with @invocation or @subscription and
/// declare their topic with @kafkaTopic.
@protocolDefinition(
    traits: [
        bote#kafkaTopic
        bote#invocation
        bote#subscription
        bote#event
        bote#command
        bote#reply
        bote#kafkaKey
        bote#kafkaHeader
    ]
)
@trait(selector: "service")
structure kafkaJson {}
