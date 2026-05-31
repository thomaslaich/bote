package bote.validate;

import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates that every @kafkaProducer operation is bound to a topic via
 * @kafkaTopic and declares an input shape.
 */
public final class KafkaOperationBindingValidator extends AbstractValidator {

    private static final ShapeId KAFKA_PRODUCER = ShapeId.from("bote#kafkaProducer");
    private static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");

    @Override
    public List<ValidationEvent> validate(Model model) {
        List<ValidationEvent> events = new ArrayList<>();

        for (OperationShape operation : model.getOperationShapes()) {
            if (!operation.hasTrait(KAFKA_PRODUCER)) {
                continue;
            }

            if (!operation.hasTrait(KAFKA_TOPIC)) {
                events.add(error(operation,
                        "@kafkaProducer operations must also declare @kafkaTopic."));
            }

            if (operation.getInput().isEmpty()) {
                events.add(error(operation,
                        "@kafkaProducer operations must define an input shape "
                        + "(the message value written to the topic)."));
            }
        }

        return events;
    }
}
