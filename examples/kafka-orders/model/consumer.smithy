$version: "2"

// The consumer's perspective: a fulfilment dashboard. It binds to the same shared
// OrdersTopic channel, but @receives only a SUBSET of the catalog — shipped and
// cancelled, not placed. That subset is this consumer's own @streaming
// subscription union, in its own namespace. The channel is shared; the subscription
// is the consumer's.
namespace orders.consumer

use bote#asyncApi
use bote#channel
use bote#receive
use orders.shared#OrderCancelled
use orders.shared#OrderShipped
use orders.shared#OrdersTopic

/// A fulfilment dashboard: tracks orders once they leave "placed".
@title("Fulfilment Dashboard API")
@asyncApi
service FulfilmentDashboard {
    version: "1.0.0"
    operations: [
        ConsumeOrderUpdates
    ]
}

/// Receive the order updates this dashboard cares about.
@receive
@channel(OrdersTopic)
operation ConsumeOrderUpdates {
    output := {
        updates: OrderUpdates
    }
}

/// This consumer's subscription: the subset of the orders catalog it streams.
/// (The placed event is intentionally not handled here.)
@streaming
union OrderUpdates {
    shipped: OrderShipped
    cancelled: OrderCancelled
}
