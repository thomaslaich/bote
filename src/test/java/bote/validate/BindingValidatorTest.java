package bote.validate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

class BindingValidatorTest {

  private static final String KAFKA =
      """
      $version: "2"
      namespace test
      use bote#channels
      use bote#event
      use bote#kafkaJson
      use bote#kafkaTopic
      use bote#redisChannel
      use bote#redisStream
      """;

  private static final String REDIS =
      """
      $version: "2"
      namespace test
      use bote#channels
      use bote#event
      use bote#redisChannel
      use bote#redisPubSubJson
      use bote#redisStream
      use bote#redisStreamsJson
      """;

  private List<ValidationEvent> validate(String model) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(Severity.ERROR);
  }

  private void assertValid(String model) {
    List<ValidationEvent> events = validate(model);
    assertTrue(events.isEmpty(), events::toString);
  }

  private void assertError(String model, String fragment) {
    List<ValidationEvent> events = validate(model);
    assertTrue(
        events.stream().anyMatch(e -> e.getMessage().contains(fragment)),
        () -> "expected an error containing '" + fragment + "', got " + events);
  }

  @Test
  void commandsAndChannelsAreValid() {
    assertValid(
        KAFKA
            + """
            @kafkaJson
            @channels([OrderEvents])
            service Orders { operations: [PlaceOrder] }

            @kafkaTopic(name: "orders.commands")
            operation PlaceOrder { input := { orderId: String } }

            @kafkaTopic(name: "orders.events")
            union OrderEvents { placed: OrderPlaced, cancelled: OrderCancelled }

            @event
            structure OrderPlaced { orderId: String }

            @event
            structure OrderCancelled { orderId: String }
            """);
  }

  @Test
  void operationWithoutAddressIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders { operations: [PlaceOrder] }

            operation PlaceOrder { input := { orderId: String } }
            """,
        "must carry @kafkaTopic");
  }

  @Test
  void operationWithoutInputIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders { operations: [PlaceOrder] }

            @kafkaTopic(name: "orders.commands")
            operation PlaceOrder {}
            """,
        "must define an input");
  }

  @Test
  void kafkaOperationWithOutputIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders { operations: [PlaceOrder] }

            @kafkaTopic(name: "orders.commands")
            operation PlaceOrder {
                input := { orderId: String }
                output := { accepted: Boolean }
            }
            """,
        "must not define an output");
  }

  @Test
  void addressOfAnotherProtocolIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders { operations: [PlaceOrder] }

            @redisStream(key: "orders")
            operation PlaceOrder { input := { orderId: String } }
            """,
        "does not match the protocol");
  }

  @Test
  void channelWithoutAddressIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            @channels([OrderEvents])
            service Orders {}

            union OrderEvents { placed: OrderPlaced }

            @event
            structure OrderPlaced { orderId: String }
            """,
        "Channels of bote service 'test#Orders' must carry @kafkaTopic");
  }

  @Test
  void channelMemberWithoutEventTraitIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            @channels([OrderEvents])
            service Orders {}

            @kafkaTopic(name: "orders.events")
            union OrderEvents { placed: OrderPlaced }

            structure OrderPlaced { orderId: String }
            """,
        "must be a structure annotated with @event");
  }

  @Test
  void unlistedChannelIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders {}

            @kafkaTopic(name: "orders.events")
            union OrderEvents { placed: OrderPlaced }

            @event
            structure OrderPlaced { orderId: String }
            """,
        "must be listed in the @channels");
  }

  @Test
  void channelListedByTwoServicesIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            @channels([OrderEvents])
            service Orders {}

            @kafkaJson
            @channels([OrderEvents])
            service Shipping {}

            @kafkaTopic(name: "orders.events")
            union OrderEvents { placed: OrderPlaced }

            @event
            structure OrderPlaced { orderId: String }
            """,
        "exactly one owning service");
  }

  @Test
  void eventAsOperationInputIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders { operations: [PlaceOrder] }

            @kafkaTopic(name: "orders.commands")
            operation PlaceOrder { input: OrderPlaced }

            @event
            structure OrderPlaced { orderId: String }
            """,
        "is an @event structure");
  }

  @Test
  void channelsWithoutProtocolIsError() {
    assertError(
        KAFKA
            + """
            @channels([OrderEvents])
            service Orders {}

            union OrderEvents { placed: OrderPlaced }

            @event
            structure OrderPlaced { orderId: String }
            """,
        "@channels requires a bote protocol trait");
  }

  @Test
  void noneDiscriminationAllowsOneEventPerChannel() {
    String model =
        KAFKA
            + """
            @kafkaJson(eventDiscrimination: "NONE")
            @channels([OrderEvents])
            service Orders {}

            @kafkaTopic(name: "orders.events")
            union OrderEvents { %s }

            @event
            structure OrderPlaced { orderId: String }

            @event
            structure OrderCancelled { orderId: String }
            """;
    assertValid(model.formatted("placed: OrderPlaced"));
    assertError(
        model.formatted("placed: OrderPlaced, cancelled: OrderCancelled"),
        "eventDiscrimination: NONE");
  }

  @Test
  void redisStreamOperationMayReply() {
    assertValid(
        REDIS
            + """
            @redisStreamsJson
            service Rooms { operations: [GetRoomStatus] }

            @redisStream(key: "rooms:status")
            operation GetRoomStatus {
                input := { roomId: String }
                output := { activeUsers: Integer }
            }
            """);
  }

  @Test
  void redisPubSubOperationWithOutputIsError() {
    assertError(
        REDIS
            + """
            @redisPubSubJson
            service Presence { operations: [SetPresence] }

            @redisChannel(name: "presence")
            operation SetPresence {
                input := { userId: String }
                output := { ok: Boolean }
            }
            """,
        "must not define an output");
  }

  @Test
  void redisChannelsAreValid() {
    assertValid(
        REDIS
            + """
            @redisPubSubJson
            @channels([PresenceEvents])
            service Presence { operations: [SetPresence] }

            @redisChannel(name: "presence.commands")
            operation SetPresence { input := { userId: String } }

            @redisChannel(name: "presence")
            union PresenceEvents { changed: PresenceChanged }

            @event
            structure PresenceChanged { userId: String }
            """);
  }

  @Test
  void namedCommandWithoutInputTraitIsError() {
    assertError(
        KAFKA
            + """
            @kafkaJson
            service Orders { operations: [PlaceOrder] }

            @kafkaTopic(name: "orders.commands")
            operation PlaceOrder { input: PlaceOrderCommand }

            structure PlaceOrderCommand { orderId: String }
            """,
        "must be annotated with @input");
  }

  @Test
  void redisReplyWithoutOutputTraitIsError() {
    assertError(
        REDIS
            + """
            @redisStreamsJson
            service Rooms { operations: [GetRoomStatus] }

            @redisStream(key: "rooms:status")
            operation GetRoomStatus {
                input := { roomId: String }
                output: RoomStatus
            }

            structure RoomStatus { activeUsers: Integer }
            """,
        "must be annotated with @output");
  }
}
