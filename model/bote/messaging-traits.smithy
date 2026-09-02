$version: "2"

namespace bote

// Broker-agnostic core. These traits carry no Kafka- or Redis-specific meaning:
// they classify message payloads by their role in the contract. Operations are
// bound to a broker with per-broker operation traits (@kafkaProduce,
// @redisSubscribe, ...), which carry the channel address — mirroring how
// Smithy's HTTP and MQTT bindings speak their transport's language while the
// shapes stay transport-neutral.
/// Marks a payload structure as an event message.
@trait(
    selector: "structure"
    conflicts: [bote#command, bote#reply]
)
structure event {}

/// Marks a payload structure as a command message.
@trait(
    selector: "structure"
    conflicts: [bote#event, bote#reply]
)
structure command {}

/// Marks a payload structure as a reply message.
///
/// Redis Streams @redisStreamAdd operations with a @reply output use
/// request/reply semantics: the requester supplies a temporary Pub/Sub reply
/// channel and a correlation ID in the transport metadata. The reply payload
/// is the bare serialization of this structure. Kafka operations and Redis
/// @redisPublish operations do not support reply outputs.
@trait(
    selector: "structure"
    conflicts: [bote#event, bote#command]
)
structure reply {}
