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
use bote#kafkaProduce
use bote#kafkaConsume
use bote.infra#kafkaTopicConfig
use bote#kafkaKey
use bote#kafkaHeader
use bote#redisPubSubJson
      use bote#redisPublish
      use bote#command
use bote#event

      /// Produces order commands and consumes order events.
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

      @kafkaProduce(topic: "orders")
      @kafkaTopicConfig(partitions: 12, replicationFactor: 3, minInsyncReplicas: 2)
      operation PublishOrder { input: SubmitOrder }

      @kafkaConsume(topic: "orders")
      operation ConsumeOrders {
          output := { events: OrderEventStream }
      }

@kafkaProduce(topic: "order-state", compacted: true)
      operation PublishState { input: SetOrderState }

@redisPublish(channel: "presence")
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

  private Model assembleModel(String source) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", source)
        .assemble()
        .unwrap();
  }

  private ObjectNode convert() {
    return convert(AsyncApiConverter.Perspective.OWNER);
  }

  private ObjectNode convert(AsyncApiConverter.Perspective perspective) {
    Model model = assembleModel(MODEL);
    ServiceShape service = model.expectShape(ShapeId.from("test#OrderService"), ServiceShape.class);
    return new AsyncApiConverter(
            model, java.util.List.of(service), java.util.Optional.empty(), perspective)
        .convert();
  }

  private ObjectNode convertGrouped() {
    Model model = assembleModel(MODEL);
    ServiceShape orders = model.expectShape(ShapeId.from("test#OrderService"), ServiceShape.class);
    ServiceShape presence =
        model.expectShape(ShapeId.from("test#PresenceService"), ServiceShape.class);
    return new AsyncApiConverter(
            model,
            java.util.List.of(orders, presence),
            java.util.Optional.of("Grouped API"),
            AsyncApiConverter.Perspective.OWNER)
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
        "Produces order commands and consumes order events.",
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
  void ownerPerspectiveReceivesCommandsAndSendsEvents() {
    ObjectNode doc = convert();
    ObjectNode operations = doc.expectObjectMember("operations");
    // The document describes the contract owner: it receives commands, sends events.
    assertEquals(
        "receive",
        operations.expectObjectMember("PublishOrder").expectStringMember("action").getValue());
    assertEquals(
        "send",
        operations.expectObjectMember("ConsumeOrders").expectStringMember("action").getValue());
    // The document records which side it describes.
    assertEquals(
        "owner",
        doc.expectObjectMember("info").expectStringMember("x-bote-perspective").getValue());
  }

  @Test
  void clientPerspectiveSendsCommandsAndReceivesEvents() {
    ObjectNode doc = convert(AsyncApiConverter.Perspective.CLIENT);
    ObjectNode operations = doc.expectObjectMember("operations");
    assertEquals(
        "send",
        operations.expectObjectMember("PublishOrder").expectStringMember("action").getValue());
    assertEquals(
        "receive",
        operations.expectObjectMember("ConsumeOrders").expectStringMember("action").getValue());
    assertEquals(
        "client",
        doc.expectObjectMember("info").expectStringMember("x-bote-perspective").getValue());
  }

  @Test
  void produceAndConsumeShareOneChannel() {
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

    // The consume operation references the event message on the same channel.
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

    // Commands are bare payloads — no envelope.
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
  void envelopeDiscriminationWrapsEventPayloads() {
    // eventDiscrimination defaults to ENVELOPE: the event payload is the
    // single-key tagged-union object keyed by the streaming union member name.
    ObjectNode payload =
        convert()
            .expectObjectMember("components")
            .expectObjectMember("messages")
            .expectObjectMember("OrderEvent")
            .expectObjectMember("payload");
    assertEquals("object", payload.expectStringMember("type").getValue());
    assertEquals(
        "#/components/schemas/OrderEvent",
        payload
            .expectObjectMember("properties")
            .expectObjectMember("orderEvent")
            .expectStringMember("$ref")
            .getValue());
    assertEquals(
        "orderEvent",
        payload.expectArrayMember("required").get(0).get().expectStringNode().getValue());
  }

  @Test
  void headerDiscriminationEmitsTypeHeader() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaConsume
        use bote#event

        @kafkaJson(eventDiscrimination: "HEADER")
        service Lights { operations: [Consume] }

        @kafkaConsume(topic: "lights")
        operation Consume { output := { events: Stream } }

        @streaming
        union Stream { measured: Measured }

        @event
        structure Measured { lumens: Integer }
        """;
    Model assembled = assembleModel(model);
    ServiceShape service = assembled.expectShape(ShapeId.from("test#Lights"), ServiceShape.class);
    ObjectNode message =
        new AsyncApiConverter(assembled, service)
            .convert()
            .expectObjectMember("components")
            .expectObjectMember("messages")
            .expectObjectMember("Measured");

    // Bare payload plus a constant "bote-type" header carrying the member name.
    assertEquals(
        "#/components/schemas/Measured",
        message.expectObjectMember("payload").expectStringMember("$ref").getValue());
    ObjectNode typeHeader =
        message
            .expectObjectMember("headers")
            .expectObjectMember("properties")
            .expectObjectMember("bote-type");
    assertEquals("measured", typeHeader.expectStringMember("const").getValue());
  }

  @Test
  void protobufServiceUsesBarePayloadsAndProtobufContentType() {
    String model =
        """
        $version: "2"
        namespace test
        use alloy.proto#protoIndex
        use bote#kafkaProtobuf
        use bote#kafkaConsume
        use bote#event

        @kafkaProtobuf
        service Telemetry { operations: [ConsumeAlerts] }

        @kafkaConsume(topic: "alerts")
        operation ConsumeAlerts { output := { alerts: Stream } }

        @event
        structure Alert {
            @protoIndex(1)
            sensorId: String
        }

        @streaming
        union Stream {
            @protoIndex(1)
            alert: Alert
        }
        """;
    Model assembled = assembleModel(model);
    ServiceShape service =
        assembled.expectShape(ShapeId.from("test#Telemetry"), ServiceShape.class);
    ObjectNode doc = new AsyncApiConverter(assembled, service).convert();

    assertEquals("application/protobuf", doc.expectStringMember("defaultContentType").getValue());
    ObjectNode message =
        doc.expectObjectMember("components")
            .expectObjectMember("messages")
            .expectObjectMember("Alert");
    assertEquals("application/protobuf", message.expectStringMember("contentType").getValue());
    // The proto oneof is the discriminator, so the payload stays bare.
    assertEquals(
        "#/components/schemas/Alert",
        message.expectObjectMember("payload").expectStringMember("$ref").getValue());
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

  @Test
  void headerMembersAreStrippedFromPayloadSchemas() {
    ObjectNode schemas = convert().expectObjectMember("components").expectObjectMember("schemas");
    // @kafkaHeader members travel only as Kafka headers, never in the JSON value.
    assertFalse(
        schemas
            .expectObjectMember("SubmitOrder")
            .expectObjectMember("properties")
            .getMember("traceId")
            .isPresent());
    assertFalse(
        schemas
            .expectObjectMember("OrderEvent")
            .expectObjectMember("properties")
            .getMember("traceId")
            .isPresent());
    // @kafkaKey members stay in the payload.
    assertTrue(
        schemas
            .expectObjectMember("SubmitOrder")
            .expectObjectMember("properties")
            .getMember("orderId")
            .isPresent());
  }
}
