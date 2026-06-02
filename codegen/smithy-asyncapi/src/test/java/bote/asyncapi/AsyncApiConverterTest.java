package bote.asyncapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.ObjectNode;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.ShapeId;

class AsyncApiConverterTest {

  private static final String MODEL =
      """
      $version: "2"
      namespace test

use bote#kafkaJson
use bote#kafkaTopic
use bote#kafkaTopicConfig
use bote#kafkaKey
use bote#kafkaHeader
use bote#redisPubSubJson
      use bote#redisChannel
      use bote#invocation
use bote#subscription
use bote#command
use bote#event

      /// Invokes order commands and subscribes to order events.
@title("Order Events API")
@kafkaJson
      service OrderService {
          version: "2024-01-01"
          operations: [PublishOrder, ConsumeOrders, PublishState]
      }

@redisPubSubJson
      service PresenceService {
          version: "2024-01-01"
          operations: [SetPresence]
      }

      @invocation
      @kafkaTopic(name: "orders")
      @kafkaTopicConfig(partitions: 12, replicationFactor: 3, minInsyncReplicas: 2)
      operation PublishOrder { input: SubmitOrder }

      @subscription
      @kafkaTopic(name: "orders")
      @kafkaTopicConfig(partitions: 12, replicationFactor: 3, minInsyncReplicas: 2)
      operation ConsumeOrders {
          output := { events: OrderEventStream }
      }

@invocation
@kafkaTopic(name: "order-state", compacted: true)
      operation PublishState { input: SetOrderState }

@invocation
@redisChannel(name: "presence")
      operation SetPresence { input: SetPresenceCommand }

@streaming
      union OrderEventStream { orderEvent: OrderEvent }

@command
      structure SubmitOrder {
@kafkaKey
          orderId: String
@kafkaHeader(name: "x-trace-id")
          traceId: String
          totalCents: Integer
      }

@event
      structure OrderEvent {
@kafkaKey
          orderId: String
@kafkaHeader(name: "x-trace-id")
          traceId: String
          totalCents: Integer
      }

@command
      structure SetOrderState {
@kafkaKey
          orderId: String
      }

@command
      structure SetPresenceCommand {
          userId: String
          status: String
      }
      """;

  private ObjectNode convert() {
    Model model =
        Model.assembler()
            .discoverModels()
            .addUnparsedModel("test.smithy", MODEL)
            .assemble()
            .unwrap();
    ServiceShape service = model.expectShape(ShapeId.from("test#OrderService"), ServiceShape.class);
    return new AsyncApiConverter(model, service).convert();
  }

  private ObjectNode convertGrouped() {
    Model model =
        Model.assembler()
            .discoverModels()
            .addUnparsedModel("test.smithy", MODEL)
            .assemble()
            .unwrap();
    ServiceShape orders =
        model.expectShape(ShapeId.from("test#OrderService"), ServiceShape.class);
    ServiceShape presence =
        model.expectShape(ShapeId.from("test#PresenceService"), ServiceShape.class);
    return new AsyncApiConverter(model, java.util.List.of(orders, presence), java.util.Optional.of("Grouped API"))
        .convert();
  }

@Test
  void emitsAsyncApi31Header() {
    ObjectNode doc = convert();
    assertEquals("3.1.0", doc.expectStringMember("asyncapi").getValue());
    assertEquals("application/json", doc.expectStringMember("defaultContentType").getValue());
    ObjectNode info = doc.expectObjectMember("info");
    // info.title comes from @title, not the service shape name.
    assertEquals("Order Events API", info.expectStringMember("title").getValue());
    assertEquals("2024-01-01", info.expectStringMember("version").getValue());
    // info.description comes from the service's @documentation.
    assertEquals(
        "Invokes order commands and subscribes to order events.",
        info.expectStringMember("description").getValue());
  }

@Test
  void mapsTopicConfigToKafkaChannelBinding() {
    ObjectNode kafka =
        convert()
            .expectObjectMember("channels")
            .expectObjectMember("orders")
            .expectObjectMember("bindings")
            .expectObjectMember("kafka");
    assertEquals(12, kafka.expectNumberMember("partitions").getValue().intValue());
    assertEquals(3, kafka.expectNumberMember("replicas").getValue().intValue());
    assertEquals(
        2,
        kafka
            .expectObjectMember("topicConfiguration")
            .expectNumberMember("min.insync.replicas")
            .getValue()
            .intValue());
  }

@Test
  void mapsCompactionToCleanupPolicy() {
    ObjectNode topicConfig =
        convert()
            .expectObjectMember("channels")
            .expectObjectMember("order-state")
            .expectObjectMember("bindings")
            .expectObjectMember("kafka")
            .expectObjectMember("topicConfiguration");
    assertEquals(
        "compact",
        topicConfig.expectArrayMember("cleanup.policy").get(0).get().expectStringNode().getValue());
  }

@Test
  void groupedServicesCanContainKafkaAndRedisChannels() {
    ObjectNode channels = convertGrouped().expectObjectMember("channels");
    assertTrue(channels.getMember("orders").isPresent());
    assertTrue(channels.getMember("presence").isPresent());
    assertFalse(channels.expectObjectMember("presence").getMember("bindings").isPresent());
    assertTrue(
        channels
            .expectObjectMember("presence")
            .expectObjectMember("messages")
            .getMember("SetPresenceCommand")
            .isPresent());
    assertTrue(
        convertGrouped()
            .expectObjectMember("components")
            .expectObjectMember("schemas")
            .getMember("SetPresenceCommand")
            .isPresent());
  }

@Test
  void mapsSendAndReceiveActions() {
    ObjectNode operations = convert().expectObjectMember("operations");
    assertEquals(
        "send",
        operations.expectObjectMember("PublishOrder").expectStringMember("action").getValue());
    assertEquals(
        "receive",
        operations.expectObjectMember("ConsumeOrders").expectStringMember("action").getValue());
  }

@Test
  void invocationsAndSubscriptionsShareOneChannel() {
    ObjectNode doc = convert();
    // A single "orders" channel is produced even though two operations bind to it.
    ObjectNode channels = doc.expectObjectMember("channels");
    assertTrue(channels.getMember("orders").isPresent());
    assertTrue(
        channels
            .expectObjectMember("orders")
            .expectObjectMember("messages")
            .getMember("SubmitOrder")
            .isPresent());
    assertTrue(
        channels
            .expectObjectMember("orders")
            .expectObjectMember("messages")
            .getMember("OrderEvent")
            .isPresent());

    // The subscription operation references the event message on the same channel.
    String receiveRef =
        doc.expectObjectMember("operations")
            .expectObjectMember("ConsumeOrders")
            .expectArrayMember("messages")
            .get(0)
            .get()
            .expectObjectNode()
            .expectStringMember("$ref")
            .getValue();
    assertEquals("#/channels/orders/messages/OrderEvent", receiveRef);
  }

@Test
  void buildsMessageWithKeyHeaderAndPayloadRef() {
    ObjectNode message =
        convert()
            .expectObjectMember("components")
            .expectObjectMember("messages")
            .expectObjectMember("SubmitOrder");

    assertEquals(
        "#/components/schemas/SubmitOrder",
        message.expectObjectMember("payload").expectStringMember("$ref").getValue());
    assertEquals(
        "string",
        message
            .expectObjectMember("bindings")
            .expectObjectMember("kafka")
            .expectObjectMember("key")
            .expectStringMember("type")
            .getValue());
    assertTrue(
        message
            .expectObjectMember("headers")
            .expectObjectMember("properties")
            .getMember("x-trace-id")
            .isPresent());
  }

@Test
  void schemasContainMessageClosureButNotStreamingWrapper() {
    ObjectNode schemas = convert().expectObjectMember("components").expectObjectMember("schemas");
    assertTrue(schemas.getMember("SubmitOrder").isPresent());
    assertTrue(schemas.getMember("OrderEvent").isPresent());
    assertTrue(schemas.getMember("SetOrderState").isPresent());
    // The @streaming union wrapper is plumbing, not a message payload.
    assertFalse(schemas.getMember("OrderEventStream").isPresent());
    // Integer renders as JSON Schema integer, not number.
    assertEquals(
        "integer",
        schemas
            .expectObjectMember("OrderEvent")
            .expectObjectMember("properties")
            .expectObjectMember("totalCents")
            .expectStringMember("type")
            .getValue());
  }
}
