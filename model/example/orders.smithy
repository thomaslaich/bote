$version: "2"

// Example: an order events service demonstrating the legierung Kafka protocol.
//
// Shows producer and consumer operations on a single topic, message key
// usage, and header binding.
namespace example

use legierung#kafkaConsumer
use legierung#kafkaHeader
use legierung#kafkaJson
use legierung#kafkaKey
use legierung#kafkaProducer
use legierung#kafkaTopic

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------
@kafkaJson
service OrderService {
    operations: [
        PublishOrder
        ConsumeOrders
        PublishOrderFulfilled
        ConsumeOrderFulfilled
    ]
}

// ---------------------------------------------------------------------------
// Orders topic
// ---------------------------------------------------------------------------
/// Publish an order event to the orders topic.
@kafkaProducer
@kafkaTopic(name: "orders")
operation PublishOrder {
    input: OrderEvent
}

/// Consume order events from the orders topic.
@kafkaConsumer(group: "order-processor")
@kafkaTopic(name: "orders")
operation ConsumeOrders {
    output: OrderEvent
}

/// An order event written to / read from the orders topic.
structure OrderEvent {
    /// Kafka message key — routes all events for the same order to one partition.
    @kafkaKey
    orderId: String

    /// Propagates distributed trace context via a Kafka header.
    @kafkaHeader(name: "x-trace-id")
    traceId: String

    customerId: String

    totalCents: Integer

    status: OrderStatus
}

enum OrderStatus {
    PLACED
    CONFIRMED
    CANCELLED
}

// ---------------------------------------------------------------------------
// Order-fulfilled topic (compacted — only latest state per order key matters)
// ---------------------------------------------------------------------------
/// Publish an order-fulfilled event.
@kafkaProducer
@kafkaTopic(name: "order-fulfilled", compacted: true)
operation PublishOrderFulfilled {
    input: OrderFulfilledEvent
}

/// Consume order-fulfilled events.
@kafkaConsumer(group: "fulfillment-notifier")
@kafkaTopic(name: "order-fulfilled", compacted: true)
operation ConsumeOrderFulfilled {
    output: OrderFulfilledEvent
}

structure OrderFulfilledEvent {
    @kafkaKey
    orderId: String

    warehouseId: String

    fulfilledAt: Timestamp
}
