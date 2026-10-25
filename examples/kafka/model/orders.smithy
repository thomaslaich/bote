$version: "2"

// The order service contract: the service receives order commands and
// publishes order lifecycle events. Topics are carried by @kafkaTopic.
namespace examples.kafka.orders

use bote#channels
use bote#event
use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic

/// The order service API.
@title("Order Service API")
@kafkaJson
@channels([OrderEvents])
service OrderService {
    version: "1.0.0"
    operations: [
        SubmitOrder
    ]
}

/// Submit a new order for processing.
@kafkaTopic(name: "orders.commands")
operation SubmitOrder {
    input := {
        @kafkaKey
        orderId: String

        /// Propagates distributed trace context via a Kafka header.
        @kafkaHeader(name: "x-trace-id")
        traceId: String

        customerId: String

        @range(min: 0)
        totalCents: Integer
    }
}

/// Order lifecycle events.
@kafkaTopic(name: "orders.events")
union OrderEvents {
    placed: OrderPlaced
    shipped: OrderShipped
    cancelled: OrderCancelled
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
