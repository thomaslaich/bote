package bote.validate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

class KafkaKeyValidatorTest {

  private static final String PREAMBLE =
      """
      $version: "2"
      namespace test
use bote#kafkaJson
use bote#kafkaProduce
use bote#kafkaKey
use bote#command

@kafkaJson
      service TestService { operations: [Publish] }
      @kafkaProduce(topic: "events")
      operation Publish { input: %s }
      """;

  private List<ValidationEvent> validate(String model) {
    return Model.assembler()
        .discoverModels()
        .addUnparsedModel("test.smithy", model)
        .assemble()
        .getValidationEvents(Severity.ERROR);
  }

@Test
  void commandWithOneKeyMemberIsValid() {
    String model =
        PREAMBLE.formatted("Payload")
            + """
@command
            structure Payload {
@kafkaKey
                id: String
                value: String
            }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void noKeyMemberIsValid() {
    String model =
        PREAMBLE.formatted("Payload")
            + """
@command
            structure Payload {
                id: String
                value: String
            }
            """;
    assertTrue(validate(model).isEmpty());
  }

@Test
  void twoKeyMembersIsError() {
    String model =
        PREAMBLE.formatted("Payload")
            + """
@command
            structure Payload {
@kafkaKey
                id: String
@kafkaKey
                secondKey: String
            }
            """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }
}
