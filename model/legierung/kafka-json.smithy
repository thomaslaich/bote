$version: "2"

namespace legierung

use smithy.api#protocolDefinition

/// A Smithy protocol for Kafka using JSON serialization.
///
/// Services annotated with this trait publish and consume JSON-encoded
/// messages over Kafka. Operations must be annotated with @kafkaProducer
/// or @kafkaConsumer and bound to a topic via @kafkaTopic.
@protocolDefinition(
    traits: [
        legierung#kafkaTopic
        legierung#kafkaProducer
        legierung#kafkaConsumer
        legierung#kafkaKey
        legierung#kafkaHeader
    ]
)
@trait(selector: "service")
structure kafkaJson {}
