package bote.validate;

import java.util.ArrayList;
import java.util.List;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.traits.StreamingTrait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates that every @send or @receive operation is bound to a channel via
 *
 * @channel, that @send declares an input shape, and that @receive declares an output shape
 *     containing a @streaming union member.
 */
public final class KafkaOperationBindingValidator extends AbstractValidator {

  private static final ShapeId SEND = ShapeId.from("bote#send");
  private static final ShapeId RECEIVE = ShapeId.from("bote#receive");
  private static final ShapeId CHANNEL = ShapeId.from("bote#channel");

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();

    for (OperationShape operation : model.getOperationShapes()) {
      boolean isSend = operation.hasTrait(SEND);
      boolean isReceive = operation.hasTrait(RECEIVE);

      if (!isSend && !isReceive) {
        continue;
      }

      if (!operation.hasTrait(CHANNEL)) {
        events.add(
            error(operation, "@send/@receive operations must bind to a channel via @channel."));
      }

      if (isSend && operation.getInput().isEmpty()) {
        events.add(
            error(
                operation,
                "@send operations must define an input shape "
                    + "(the message value written to the topic)."));
      }

      if (isReceive) {
        if (operation.getOutput().isEmpty()) {
          events.add(
              error(
                  operation,
                  "@receive operations must define an output shape "
                      + "containing a @streaming union member."));
        } else {
          operation
              .getOutput()
              .flatMap(id -> model.getShape(id).flatMap(Shape::asStructureShape))
              .ifPresent(output -> validateStreamingUnion(model, operation, output, events));
        }
      }
    }

    return events;
  }

  private void validateStreamingUnion(
      Model model, OperationShape operation, StructureShape output, List<ValidationEvent> events) {
    boolean hasStreamingUnion =
        output.getAllMembers().values().stream()
            .anyMatch(
                member ->
                    model
                        .getShape(member.getTarget())
                        .flatMap(Shape::asUnionShape)
                        .map(union -> union.hasTrait(StreamingTrait.ID))
                        .orElse(false));

    if (!hasStreamingUnion) {
      events.add(
          error(
              operation,
              "@receive operation output must contain a member targeting a @streaming union."));
    }
  }
}
