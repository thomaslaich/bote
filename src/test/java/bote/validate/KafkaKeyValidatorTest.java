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
use bote#kafkaKey
use bote#kafkaTopic

@kafkaJson
      service TestService { operations: [Publish] }
      @kafkaTopic(name: "events")
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
            +
"""
            @input
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
            +
"""
            @input
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
            +
"""
            @input
            structure Payload {
@kafkaKey
                id: String
@kafkaKey
                secondKey: String
            }
""";
    assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
  }

  @Test
  void eventWithTwoKeyMembersIsError() {
    String model =
        """
        $version: "2"
        namespace test
        use bote#channels
        use bote#event
        use bote#kafkaJson
        use bote#kafkaKey
        use bote#kafkaTopic

        @kafkaJson
        @channels([Events])
        service TestService {}

        @kafkaTopic(name: "events")
        union Events { happened: Happened }

        @event
        structure Happened {
            @kafkaKey
            id: String
            @kafkaKey
            secondKey: String
        }
        """;
    assertTrue(validate(model).stream().anyMatch(e -> e.getMessage().contains("@kafkaKey")));
  }
}
