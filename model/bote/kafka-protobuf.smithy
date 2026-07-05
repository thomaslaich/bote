$version: "2"

namespace bote

use smithy.api#protocolDefinition

/// A Smithy protocol for Kafka using Protocol Buffers serialization.
///
/// The Smithy shape graph is mapped to .proto message definitions by code
/// generators that consume this protocol. Proto field numbers come from
/// alloy.proto#protoIndex (Alloy trait library,
/// com.disneystreaming.alloy:alloy-core): every member of every payload
/// structure and streaming union must carry an explicit @protoIndex
/// (validator-enforced), because implicit numbering breaks the wire format
/// when members are reordered or removed.
///
/// Wire rules:
///
/// - A @command message value is the protobuf serialization of its
///   structure's message.
/// - An @event message value is the protobuf serialization of the @streaming
///   union's message: the union maps to a proto message with a oneof whose
///   field numbers come from the union members' @protoIndex. The oneof case
///   is the event discriminator; no separate mechanism is needed.
/// - Members annotated with @kafkaHeader travel only as Kafka headers and
///   are not fields of the proto message. They still carry a @protoIndex
///   (Alloy requires indexes on all members or none); the number is
///   reserved and must not be reused.
/// - The member annotated with @kafkaKey is serialized both as the Kafka
///   message key (using the standard Kafka serializer for its primitive
///   type) and as a field of the message.
///
/// A Schema Registry is not required: .proto files serve as the schema
/// contract. If a Confluent-compatible registry is used, messages carry the
/// Confluent wire-format prefix and subjects follow subjectNamingStrategy;
/// the registry URL and credentials are runtime configuration and must not
/// appear in the model.
///
/// Operations must use @kafkaProduce or @kafkaConsume, which carry the topic.
@protocolDefinition(
    traits: [
        bote#kafkaProduce
        bote#kafkaConsume
        bote#event
        bote#command
        bote#kafkaKey
        bote#kafkaHeader
        alloy.proto#protoIndex
        alloy.proto#protoNumType
        alloy.proto#protoWrapped
        alloy.proto#protoTimestampFormat
        alloy.proto#protoEnumFormat
        alloy.proto#protoCompactUUID
    ]
)
@trait(selector: "service")
structure kafkaProtobuf {
    /// How schema subject names are derived when a Confluent-compatible
    /// Schema Registry is used. Has no effect if no registry is configured.
    ///
    /// Defaults to TOPIC_NAME.
    subjectNamingStrategy: SubjectNamingStrategy
}
