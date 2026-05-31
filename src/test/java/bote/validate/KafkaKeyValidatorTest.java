package bote.validate;

import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaKeyValidatorTest {

    private static final String PREAMBLE = """
            $version: "2"
            namespace test
            use bote#kafkaJson
            use bote#send
            use bote#kafkaTopic
            use bote#kafkaKey

            @kafkaJson
            service TestService { operations: [Publish] }

            @send
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
    void oneKeyMemberIsValid() {
        String model = PREAMBLE.formatted("Payload") + """
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
        String model = PREAMBLE.formatted("Payload") + """
                structure Payload {
                    id: String
                    value: String
                }
                """;
        assertTrue(validate(model).isEmpty());
    }

    @Test
    void twoKeyMembersIsError() {
        String model = PREAMBLE.formatted("Payload") + """
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
