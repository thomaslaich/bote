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
      use bote#messaging
      use bote#send
      use bote#receive
      use bote#kafkaTopic
      use bote#channel

      @messaging
      service TestService {
          operations: [%s]
      }

      @kafkaTopic(name: "orders")
      structure Orders {}
      """;

  private List<ValidationEvent> validate(String model) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(Severity.ERROR);
  }

  @Test
  void senderWithChannelIsValid() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @send
            @channel(Orders)
            operation PublishOrder { input: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

  @Test
  void senderWithoutChannelIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @send
            operation PublishOrder { input: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

  @Test
  void senderWithoutInputIsError() {
    String model =
        PREAMBLE.formatted("PublishOrder")
            + """
            @send
            @channel(Orders)
            operation PublishOrder {}
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

  @Test
  void receiverWithStreamingUnionIsValid() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @receive
            @channel(Orders)
            operation ConsumeOrders {
                output := { events: OrderEventStream }
            }
            @streaming
            union OrderEventStream { orderEvent: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }

  @Test
  void receiverWithoutOutputIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @receive
            @channel(Orders)
            operation ConsumeOrders {}
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

  @Test
  void receiverWithoutStreamingUnionIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @receive
            @channel(Orders)
            operation ConsumeOrders { output: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

  @Test
  void receiverWithoutChannelIsError() {
    String model =
        PREAMBLE.formatted("ConsumeOrders")
            + """
            @receive
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
  void plainOperationWithoutKafkaTraitsIsIgnored() {
    String model =
        PREAMBLE.formatted("PlainOp")
            + """
            operation PlainOp { input: Payload, output: Payload }
            structure Payload { value: String }
            """;
    assertTrue(validate(model).isEmpty());
  }
}
