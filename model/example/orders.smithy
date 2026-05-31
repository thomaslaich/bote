$version: "2"

// Example: an order events service demonstrating the bote Kafka protocol.
//
// Shows producer and consumer operations on a single topic, message key
// usage, and header binding.
namespace example

use bote#kafkaHeader
use bote#kafkaJson
use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig
use bote#receive
use bote#send

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------
@kafkaJson
service OrderService {
    operations: [
        PublishOrder
        PublishOrderFulfilled
        ConsumeOrders
    ]
}

// ---------------------------------------------------------------------------
// Orders topic
// ---------------------------------------------------------------------------
/// Publish an order event to the orders topic.
@send
@kafkaTopic(name: "orders")
@kafkaTopicConfig(
    partitions: 12
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
    minInsyncReplicas: 2
)
operation PublishOrder {
    input: OrderEvent
}

/// An order event written to the orders topic.
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
@send
@kafkaTopic(name: "order-fulfilled", compacted: true)
operation PublishOrderFulfilled {
    input: OrderFulfilledEvent
}

/// Receive order events from the orders topic.
@receive
@kafkaTopic(name: "orders")
operation ConsumeOrders {
    output := {
        events: OrderEventStream
    }
}

@streaming
union OrderEventStream {
    orderEvent: OrderEvent
}

structure OrderFulfilledEvent {
    @kafkaKey
    orderId: String

    warehouseId: String

    fulfilledAt: Timestamp
}
