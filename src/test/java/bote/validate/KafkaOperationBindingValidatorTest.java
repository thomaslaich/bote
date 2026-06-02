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
      use bote#invocation
      use bote#subscription
      use bote#kafkaTopic
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
  void invocationWithTopicIsValid() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @invocation
            @kafkaTopic(name: "orders")
            operation PublishOrder { input: Payload }
            @command
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void invocationWithoutAddressIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @invocation
            operation PublishOrder { input: Payload }
            @command
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void invocationWithoutInputIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @invocation
            @kafkaTopic(name: "orders")
            operation PublishOrder {}
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void subscriptionWithStreamingUnionIsValid() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @subscription
            @kafkaTopic(name: "orders")
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
  void subscriptionWithoutOutputIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @subscription
            @kafkaTopic(name: "orders")
            operation ConsumeOrders {}
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void subscriptionWithoutStreamingUnionIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @subscription
            @kafkaTopic(name: "orders")
            operation ConsumeOrders { output: Payload }
            @event
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void subscriptionWithoutAddressIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @subscription
            operation ConsumeOrders {
                output := { events: OrderEventStream }
            }
            @streaming
            union OrderEventStream { orderEvent: Payload }
            @event
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void plainOperationWithoutKafkaTraitsIsIgnored() {
    String model =
        PREAMBLE.formatted("PlainOp")
            + """
            operation PlainOp { input: Payload, output: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void invocationInputWithoutCommandTraitIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @invocation
            @kafkaTopic(name: "orders")
            operation PublishOrder { input: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void invocationOutputMustBeReply() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @invocation
            @kafkaTopic(name: "orders")
            operation PublishOrder { input: Payload, output: Result }
            @command
            structure Payload { value: String }
            structure Result { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

@Test
  void invocationOutputWithReplyTraitIsValid() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @invocation
            @kafkaTopic(name: "orders")
            operation PublishOrder { input: Payload, output: Result }
            @command
            structure Payload { value: String }
            @reply
            structure Result { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void subscriptionStreamMembersMustBeEvents() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @subscription
            @kafkaTopic(name: "orders")
            operation ConsumeOrders {
                output := { events: OrderEventStream }
            }
            @streaming
            union OrderEventStream { orderEvent: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }
}
