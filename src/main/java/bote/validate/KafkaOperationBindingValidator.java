package bote.validate;

import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates that every @send or @receive operation is bound to a topic via
 * @kafkaTopic, and that @send declares an input shape and @receive declares
 * an output shape.
 */
public final class KafkaOperationBindingValidator extends AbstractValidator {

    private static final ShapeId SEND = ShapeId.from("bote#send");
    private static final ShapeId RECEIVE = ShapeId.from("bote#receive");
    private static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");

    @Override
    public List<ValidationEvent> validate(Model model) {
        List<ValidationEvent> events = new ArrayList<>();

        for (OperationShape operation : model.getOperationShapes()) {
            boolean isSend = operation.hasTrait(SEND);
            boolean isReceive = operation.hasTrait(RECEIVE);

            if (!isSend && !isReceive) {
                continue;
            }

            if (!operation.hasTrait(KAFKA_TOPIC)) {
                events.add(error(operation,
                        "@send/@receive operations must also declare @kafkaTopic."));
            }

            if (isSend && operation.getInput().isEmpty()) {
                events.add(error(operation,
                        "@send operations must define an input shape "
                        + "(the message value written to the topic)."));
            }

            if (isReceive && operation.getOutput().isEmpty()) {
                events.add(error(operation,
                        "@receive operations must define an output shape "
                        + "(the message value read from the topic)."));
            }
        }

        return events;
    }
}
