$version: "2"

namespace bote

// Broker-agnostic core. A bote service owns its contract: it receives commands,
// which are its operations, and publishes events to channels, which are unions
// it lists in @channels. Per-broker address traits (@kafkaTopic, @redisStream,
// @redisChannel) bind both to a broker address. They name the address, never an
// action, so the contract reads the same from the owner's and a client's side.
/// Lists the channels a bote service owns.
///
/// A channel is a union whose members target @event structures and that
/// carries the address trait of the service's protocol. The owner publishes
/// the union's events to that address. Union member names are the event
/// discriminators. Every union with an address trait is listed by exactly one
/// service.
@trait(selector: "service")
list channels {
    @idRef(failWhenMissing: true, selector: "union")
    member: String
}

/// Marks a structure as an event.
///
/// Events are members of channels. An @event structure is never the input
/// or output of an operation.
@trait(selector: "structure")
structure event {}
