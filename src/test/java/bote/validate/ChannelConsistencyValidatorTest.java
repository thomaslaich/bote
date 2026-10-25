package bote.validate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

class ChannelConsistencyValidatorTest {

  private List<ValidationEvent> validate(String model, Severity severity) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(severity);
  }

  private boolean hasEvent(String model, Severity severity, String fragment) {
    return validate(model, severity).stream().anyMatch(e -> e.getMessage().contains(fragment));
  }

  @Test
  void twoServicesOnOneAddressIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaTopic

        @kafkaJson
        service A { operations: [PubA] }

        @kafkaJson
        service B { operations: [PubB] }

        @kafkaTopic(name: "orders")
        operation PubA { input := { value: String } }

        @kafkaTopic(name: "orders")
        operation PubB { input := { value: String } }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "exactly one owning service"));
  }

  @Test
  void channelAddressUsedByAnOperationIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#channels
        use bote#event
        use bote#redisStream
        use bote#redisStreamsJson

        @redisStreamsJson
        @channels([ChatEvents])
        service Chat { operations: [Post] }

        @redisStream(key: "chat")
        operation Post { input := { value: String } }

        @redisStream(key: "chat")
        union ChatEvents { posted: Posted }

        @event
        structure Posted { value: String }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "carries only that channel's events"));
  }

  @Test
  void divergingAddressValuesIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStream
        use bote#redisStreamsJson

        @redisStreamsJson
        service Chat { operations: [Post, Edit] }

        @redisStream(key: "chat", maxLen: 10000)
        operation Post { input := { value: String } }

        @redisStream(key: "chat", maxLen: 500)
        operation Edit { input := { value: String } }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "must be identical"));
  }

  @Test
  void agreedAddressValuesAreValid() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStream
        use bote#redisStreamsJson

        @redisStreamsJson
        service Chat { operations: [Post] }

        @redisStream(key: "chat", maxLen: 10000)
        operation Post { input := { value: String } }
        """;
    assertTrue(validate(model, Severity.ERROR).isEmpty());
  }

  @Test
  void topicConfigOnTwoOperationsOfOneAddressIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaTopic
        use bote.infra#kafkaTopicConfig

        @kafkaJson
        service Orders { operations: [Submit, Cancel] }

        @kafkaTopic(name: "orders")
        @kafkaTopicConfig(partitions: 6)
        operation Submit { input := { value: String } }

        @kafkaTopic(name: "orders")
        @kafkaTopicConfig(partitions: 6)
        operation Cancel { input := { value: String } }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "at most one shape per topic"));
  }

  @Test
  void multipleCommandTypesOnOneAddressIsWarning() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaTopic

        @kafkaJson
        service Orders { operations: [Submit, Cancel] }

        @kafkaTopic(name: "orders.commands")
        operation Submit { input := { value: String } }

        @kafkaTopic(name: "orders.commands")
        operation Cancel { input := { value: String } }
        """;
    assertTrue(hasEvent(model, Severity.WARNING, "command types"));
    assertTrue(validate(model, Severity.ERROR).isEmpty());
  }
}
