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
        use bote#kafkaProduce
        use bote#command

        @kafkaJson
        service A { operations: [PubA] }

        @kafkaJson
        service B { operations: [PubB] }

        @kafkaProduce(topic: "orders")
        operation PubA { input: Payload }

        @kafkaProduce(topic: "orders")
        operation PubB { input: Payload }

        @command
        structure Payload { value: String }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "exactly one owning service"));
  }

  @Test
  void divergingChannelValuesIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStreamsJson
        use bote#redisStreamAdd
        use bote#redisStreamRead
        use bote#command
        use bote#event

        @redisStreamsJson
        service Chat { operations: [Post, Consume] }

        @redisStreamAdd(stream: "chat", maxLen: 10000)
        operation Post { input: PostMessage }

        @redisStreamRead(stream: "chat", maxLen: 500)
        operation Consume { output := { events: Stream } }

        @streaming
        union Stream { posted: Posted }

        @command
        structure PostMessage { value: String }

        @event
        structure Posted { value: String }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "must be identical"));
  }

  @Test
  void agreedChannelValuesAreValid() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#redisStreamsJson
        use bote#redisStreamAdd
        use bote#redisStreamRead
        use bote#command
        use bote#event

        @redisStreamsJson
        service Chat { operations: [Post, Consume] }

        @redisStreamAdd(stream: "chat", maxLen: 10000)
        operation Post { input: PostMessage }

        @redisStreamRead(stream: "chat", maxLen: 10000)
        operation Consume { output := { events: Stream } }

        @streaming
        union Stream { posted: Posted }

        @command
        structure PostMessage { value: String }

        @event
        structure Posted { value: String }
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
        use bote#kafkaProduce
        use bote#kafkaConsume
        use bote.infra#kafkaTopicConfig
        use bote#command
        use bote#event

        @kafkaJson
        service Orders { operations: [Pub, Consume] }

        @kafkaProduce(topic: "orders")
        @kafkaTopicConfig(partitions: 6)
        operation Pub { input: Payload }

        @kafkaConsume(topic: "orders")
        @kafkaTopicConfig(partitions: 6)
        operation Consume { output := { events: Stream } }

        @streaming
        union Stream { placed: Placed }

        @command
        structure Payload { value: String }

        @event
        structure Placed { value: String }
        """;
    assertTrue(hasEvent(model, Severity.ERROR, "at most one operation per topic"));
  }

  @Test
  void multipleCommandTypesOnOneAddressIsWarning() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaProduce
        use bote#command

        @kafkaJson
        service Orders { operations: [Submit, Cancel] }

        @kafkaProduce(topic: "orders.commands")
        operation Submit { input: SubmitOrder }

        @kafkaProduce(topic: "orders.commands")
        operation Cancel { input: CancelOrder }

        @command
        structure SubmitOrder { value: String }

        @command
        structure CancelOrder { value: String }
        """;
    assertTrue(hasEvent(model, Severity.WARNING, "command types"));
    assertTrue(validate(model, Severity.ERROR).isEmpty());
  }
}
