package bote.validate;

import org.junit.jupiter.api.Test;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.validation.Severity;
import software.amazon.smithy.model.validation.ValidationEvent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaOperationBindingValidatorTest {

    private static final String PREAMBLE = """
            $version: "2"
            namespace test
            use bote#kafkaJson
            use bote#kafkaProducer
            use bote#kafkaTopic

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
    void producerWithTopicIsValid() {
        String model = PREAMBLE.formatted("PublishOrder") + """
                @kafkaProducer
                @kafkaTopic(name: "orders")
                operation PublishOrder { input: Payload }
                structure Payload { value: String }
                """;
        assertTrue(validate(model).isEmpty());
    }

    @Test
    void producerWithoutTopicIsError() {
        String model = PREAMBLE.formatted("PublishOrder") + """
                @kafkaProducer
                operation PublishOrder { input: Payload }
                structure Payload { value: String }
                """;
        assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
    }

    @Test
    void producerWithoutInputIsError() {
        String model = PREAMBLE.formatted("PublishOrder") + """
                @kafkaProducer
                @kafkaTopic(name: "orders")
                operation PublishOrder {}
                """;
        assertTrue(validate(model).stream().anyMatch(e -> e.getSeverity() == Severity.ERROR));
    }

    @Test
    void plainOperationWithoutKafkaTraitsIsIgnored() {
        String model = PREAMBLE.formatted("PlainOp") + """
                operation PlainOp { input: Payload, output: Payload }
                structure Payload { value: String }
                """;
        assertTrue(validate(model).isEmpty());
    }
}
