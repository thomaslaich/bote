$version: "2"

namespace bote

// Broker-agnostic core. These traits carry no Kafka- or Redis-specific meaning:
// they describe the shape of a message-driven contract. Broker-specific address
// traits live directly on operations.
/// Marks an operation as an invocation accepted by this contract.
/// The operation input is the message value written to the channel.
@trait(
    selector: "operation"
    conflicts: [bote#subscription]
)
structure invocation {}

/// Marks an operation as a subscription offered by this contract.
/// The operation output must contain a member targeting a @streaming union,
/// where each union member is a possible event type on the channel.
@trait(
    selector: "operation"
    conflicts: [bote#invocation]
)
structure subscription {}

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
@trait(
    selector: "structure"
    conflicts: [bote#event, bote#command]
)
structure reply {}
