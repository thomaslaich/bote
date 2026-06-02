$version: "2"

// The order service contract: clients can submit order commands and subscribe
// to order lifecycle events. Topics are declared directly on the operations.
namespace examples.kafka.orders

use bote#command
use bote#event
use bote#invocation
use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig
use bote#subscription

/// The order service API.
@title("Order Service API")
@kafkaJson
service OrderService {
    version: "1.0.0"
    operations: [
        InvokeSubmitOrder
        SubscribeToOrderEvents
    ]
}

/// Submit a new order for processing.
@invocation
@kafkaTopic(name: "orders.commands")
operation InvokeSubmitOrder {
    input: SubmitOrder
}

/// Subscribe to order lifecycle events.
@subscription
@kafkaTopic(name: "orders.events")
@kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)
operation SubscribeToOrderEvents {
    output := {
        events: OrderEvents
    }
}

/// Command to submit a new order.
@command
structure SubmitOrder {
    @kafkaKey
    orderId: String

    /// Propagates distributed trace context via a Kafka header.
    @kafkaHeader(name: "x-trace-id")
    traceId: String

    customerId: String

    @range(min: 0)
    totalCents: Integer
}

/// An order was placed.
@event
structure OrderPlaced {
    @kafkaKey
    orderId: String

    customerId: String

    @range(min: 0)
    totalCents: Integer

    placedAt: Timestamp
}

/// An order was shipped.
@event
structure OrderShipped {
    @kafkaKey
    orderId: String

    carrier: String

    trackingNumber: String

    shippedAt: Timestamp
}

/// An order was cancelled.
@event
structure OrderCancelled {
    @kafkaKey
    orderId: String

    reason: String

    cancelledAt: Timestamp
}

/// The complete event stream offered by the order service.
@streaming
union OrderEvents {
    placed: OrderPlaced
    shipped: OrderShipped
    cancelled: OrderCancelled
}
