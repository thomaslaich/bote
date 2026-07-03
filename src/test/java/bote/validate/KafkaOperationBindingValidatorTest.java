package bote.validate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

class KafkaOperationBindingValidatorTest {

  private static final String PREAMBLE =
      """
      $version: "2"
      namespace test
      use bote#kafkaJson
      use bote#kafkaProduce
      use bote#kafkaConsume
      use bote#command
      use bote#event
      use bote#reply

      @kafkaJson
      service TestService {
          operations: [%s]
      }
      """;

  private List<ValidationEvent> validate(String model) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(Severity.ERROR);
  }

@Test
  void produceWithCommandInputIsValid() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @kafkaProduce(topic: "orders")
            operation PublishOrder { input: Payload }
            @command
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void produceWithoutInputIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @kafkaProduce(topic: "orders")
            operation PublishOrder {}
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void consumeWithStreamingUnionIsValid() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @kafkaConsume(topic: "orders")
            operation ConsumeOrders {
                output := { events: OrderEventStream }
            }
            @streaming
            union OrderEventStream { orderEvent: Payload }
            @event
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void consumeWithoutOutputIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @kafkaConsume(topic: "orders")
            operation ConsumeOrders {}
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void consumeWithoutStreamingUnionIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @kafkaConsume(topic: "orders")
            operation ConsumeOrders { output: Payload }
            @event
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void plainOperationWithoutBrokerTraitsIsIgnored() {
    String model =
        PREAMBLE.formatted("PlainOp")
            + """
            operation PlainOp { input: Payload, output: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void produceInputWithoutCommandTraitIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @kafkaProduce(topic: "orders")
            operation PublishOrder { input: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void produceWithOutputIsError() {
    // No current protocol supports reply semantics, so produce-side outputs are rejected
    // even when the output carries @reply (reserved vocabulary).
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @kafkaProduce(topic: "orders")
            operation PublishOrder { input: Payload, output: Result }
            @command
            structure Payload { value: String }
            @reply
            structure Result { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void brokerTraitOfAnotherProtocolIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#redisPublish
        use bote#command

        @kafkaJson
        service TestService { operations: [SetState] }

        @redisPublish(channel: "state")
        operation SetState { input: Payload }

        @command
        structure Payload { value: String }
        """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getMessage().contains("does not match")));
  }

@Test
  void consumeStreamMembersMustBeEvents() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @kafkaConsume(topic: "orders")
            operation ConsumeOrders {
                output := { events: OrderEventStream }
            }
            @streaming
            union OrderEventStream { orderEvent: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void noneDiscriminationAllowsSingleEventStream() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaConsume
        use bote#event

        @kafkaJson(eventDiscrimination: "NONE")
        service TestService { operations: [ConsumeOrders] }

        @kafkaConsume(topic: "orders")
        operation ConsumeOrders { output := { events: Stream } }

        @streaming
        union Stream { orderEvent: Payload }

        @event
        structure Payload { value: String }
        """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void noneDiscriminationRejectsMultiEventStream() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#kafkaJson
        use bote#kafkaConsume
        use bote#event

        @kafkaJson(eventDiscrimination: "NONE")
        service TestService { operations: [ConsumeOrders] }

        @kafkaConsume(topic: "orders")
        operation ConsumeOrders { output := { events: Stream } }

        @streaming
        union Stream { placed: Placed, shipped: Shipped }

        @event
        structure Placed { value: String }

        @event
        structure Shipped { value: String }
        """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }
}
