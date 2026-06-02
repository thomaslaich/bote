$version: "2"

// The shared contract: the channel, catalog, and event payload types.
//
// The channel is a thin marker structure carrying the topic address/config. The
// catalog is a separate union of event types that may travel on the topic. Producers
// and consumers bind to the channel shape; consumers still declare their own
// @streaming subscription unions for the subset they receive.
namespace orders.shared

use bote#kafkaHeader
use bote#kafkaKey
use bote#kafkaTopic
use bote#kafkaTopicConfig

/// The orders topic: its address and durability config.
@kafkaTopic(name: "orders")
@kafkaTopicConfig(
    partitions: 6
    replicationFactor: 3
    retentionMs: 604800000
    // 7 days
)
structure OrdersTopic {}

/// The complete catalog of event types that may travel on the orders topic.
union OrderEvent {
    placed: OrderPlaced
    shipped: OrderShipped
    cancelled: OrderCancelled
}

/// An order was placed.
structure OrderPlaced {
    /// Routes all events for one order to the same partition.
    @kafkaKey
    orderId: String

    /// Propagates distributed trace context via a Kafka header.
    @kafkaHeader(name: "x-trace-id")
    traceId: String

    customerId: String

    @range(min: 0)
    totalCents: Integer

    placedAt: Timestamp
}

/// An order was shipped.
structure OrderShipped {
    @kafkaKey
    orderId: String

    carrier: String

    trackingNumber: String

    shippedAt: Timestamp
}

/// An order was cancelled.
structure OrderCancelled {
    @kafkaKey
    orderId: String

    reason: String

    cancelledAt: Timestamp
}
