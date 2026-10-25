package bote.csharp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.build.MockManifest;
import software.amazon.smithy.codegen.core.CodegenException;

class BoteCSharpIntegrationTest {

  private static final String KAFKA =
      """
      $version: "2"
      namespace test
      use bote#channels
      use bote#event
      use bote#kafkaJson
      use bote#kafkaKey
      use bote#kafkaTopic
      use bote.infra#kafkaTopicConfig

      @kafkaJson
      @channels([OrderEvents])
      service Orders { version: "1", operations: [PlaceOrder] }

      @kafkaTopic(name: "orders.commands")
      operation PlaceOrder { input := { @required @kafkaKey orderId: String } }

      @kafkaTopic(name: "orders.events", compacted: true)
      @kafkaTopicConfig(partitions: 6, retentionMs: 86400000)
      union OrderEvents { placed: OrderPlaced }

      @event
      structure OrderPlaced { @required @kafkaKey orderId: String }
      """;

  private static final Path SHARED = Path.of("Test/Orders.Messaging.g.cs");
  private static final Path SERVER = Path.of("Test/Orders.Messaging.Server.g.cs");
  private static final Path CLIENT = Path.of("Test/Orders.Messaging.Client.g.cs");
  private static final Path INFRASTRUCTURE = Path.of("Test/Orders.Kafka.Infrastructure.g.cs");

  @Test
  void ownerHandlesCommandsAndPublishesEvents() {
    MockManifest manifest = generate(KAFKA, "test#Orders", true, false);

    String server = file(manifest, SERVER);
    assertTrue(server.contains("public interface IPlaceOrderHandler"));
    assertTrue(server.contains("public interface IOrderEventsPublisher"));
    assertTrue(server.contains("PublishPlacedAsync(Test.OrderPlaced @event"));
    assertFalse(manifest.hasFile(CLIENT));
    assertTrue(manifest.hasFile(SHARED));
  }

  @Test
  void clientSendsCommandsAndHandlesEvents() {
    MockManifest manifest = generate(KAFKA, "test#Orders", false, true);

    String client = file(manifest, CLIENT);
    assertTrue(client.contains("public interface IOrdersClient"));
    assertTrue(client.contains("public interface IOrderEventsHandler"));
    assertTrue(client.contains("HandlePlacedAsync(Test.OrderPlaced @event"));
    assertTrue(client.contains("HandleUnknownAsync(string type"));
    assertFalse(manifest.hasFile(SERVER));
  }

  @Test
  void channelEventsAreGeneratedWithTheService() {
    MockManifest manifest = generate(KAFKA, "test#Orders", true, true);

    assertTrue(manifest.hasFile(Path.of("Test/OrderEvents.g.cs")));
    assertTrue(manifest.hasFile(Path.of("Test/OrderPlaced.g.cs")));
    assertFalse(manifest.hasFile(Path.of("Test/Orders.Client.g.cs")));
  }

  @Test
  void kafkaTopicConfigurationIsGenerated() {
    String infrastructure = file(generate(KAFKA, "test#Orders", true, true), INFRASTRUCTURE);

    assertTrue(infrastructure.contains("Name: \"orders.events\""));
    assertTrue(infrastructure.contains("Partitions: 6"));
    assertTrue(infrastructure.contains("[\"cleanup.policy\"] = \"compact\""));
    assertTrue(infrastructure.contains("[\"retention.ms\"] = \"86400000\""));
    assertFalse(infrastructure.contains("orders.commands"));
  }

  @Test
  void redisStreamRequestReplyUsesTheRequestBinding() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStream
        use bote#redisStreamsJson

        @redisStreamsJson
        service Inventory { version: "1", operations: [GetStock] }

        @redisStream(key: "stock:queries", maxLen: 100)
        operation GetStock {
            input := { productId: String }
            output := { available: Integer }
        }
        """;
    MockManifest manifest = generate(model, "test#Inventory", true, true);

    assertTrue(
        file(manifest, Path.of("Test/Inventory.Messaging.Client.g.cs"))
            .contains("RedisStreamRequestBinding<Test.GetStockInput, Test.GetStockOutput>"));
    assertTrue(
        file(manifest, Path.of("Test/Inventory.Messaging.Server.g.cs"))
            .contains("Task<Test.GetStockOutput> HandleAsync(Test.GetStockInput command"));
  }

  @Test
  void commandsWithoutAnOutputDoNotReply() {
    String server = file(generate(KAFKA, "test#Orders", true, false), SERVER);

    assertTrue(server.contains("System.Threading.Tasks.Task HandleAsync(Test.PlaceOrderInput"));
    assertFalse(server.contains("MessageReplyReceiveBinding"));
  }

  @Test
  void commandsSharingAnAddressAreRejected() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaTopic

        @kafkaJson
        service Lights { version: "1", operations: [TurnOn, TurnOff] }

        @kafkaTopic(name: "lights.commands")
        operation TurnOn { input := { id: String } }

        @kafkaTopic(name: "lights.commands")
        operation TurnOff { input := { id: String } }
        """;

    CodegenException exception =
        assertThrows(CodegenException.class, () -> generate(model, "test#Lights", true, true));
    assertTrue(exception.getMessage().contains("lights.commands"));
  }

  @Test
  void unsupportedProtocolsAreRejected() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaAvro
        use bote#kafkaTopic

        @kafkaAvro
        service Lights { version: "1", operations: [TurnOn] }

        @kafkaTopic(name: "lights.commands")
        operation TurnOn { input := { id: String } }
        """;

    CodegenException exception =
        assertThrows(CodegenException.class, () -> generate(model, "test#Lights", true, true));
    assertTrue(exception.getMessage().contains("@kafkaAvro"));
  }

  private static MockManifest generate(
      String model, String service, boolean server, boolean client) {
    return CodegenTestSupport.generate(CodegenTestSupport.model(model), service, server, client);
  }

  private static String file(MockManifest manifest, Path path) {
    return manifest
        .getFileString(path)
        .orElseThrow(() -> new AssertionError(path + " not in " + manifest.getFiles()));
  }
}
