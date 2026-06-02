$version: "2"

// The producer's perspective: the order service emits order lifecycle events.
// It binds to the shared OrdersTopic channel and @sends individual event types.
// A producer never touches a @streaming union; it sends single events.
namespace orders.producer

use bote#channel
use bote#kafkaJson
use bote#send
use orders.shared#OrderCancelled
use orders.shared#OrderPlaced
use orders.shared#OrderShipped
use orders.shared#OrdersTopic

/// The order service: emits order lifecycle events to the orders topic.
@title("Order Service API")
@kafkaJson
service OrderService {
    version: "1.0.0"
    operations: [
        PublishOrderPlaced
        PublishOrderShipped
        PublishOrderCancelled
    ]
}

/// Publish an order-placed event.
@send
@channel(OrdersTopic)
operation PublishOrderPlaced {
    input: OrderPlaced
}

/// Publish an order-shipped event.
@send
@channel(OrdersTopic)
operation PublishOrderShipped {
    input: OrderShipped
}

/// Publish an order-cancelled event.
@send
@channel(OrdersTopic)
operation PublishOrderCancelled {
    input: OrderCancelled
}
