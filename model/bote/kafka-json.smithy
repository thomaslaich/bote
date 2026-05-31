$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Kafka using JSON serialization.
///
/// Services annotated with this trait publish JSON-encoded messages over
/// Kafka. Operations must be annotated with @kafkaProducer and bound to
/// a topic via @kafkaTopic.
///
/// Consumer scaffolding is generated from the producer model — there is
/// no separate consumer operation. The consumer group is a runtime concern
/// and must not appear in the model.
@protocolDefinition(
    traits: [bote#kafkaTopic, bote#kafkaProducer, bote#kafkaKey, bote#kafkaHeader]
)
@trait(selector: "service")
structure kafkaJson {}
