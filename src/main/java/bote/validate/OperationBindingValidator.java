package bote.validate;

import java.util.ArrayList;
import java.util.List;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.traits.StreamingTrait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates that every @send or @receive operation of a bote messaging service binds to a channel
 * via @channel, that @send declares an input shape, and that @receive declares an output shape
 * containing a @streaming union member (the consumer's subscription).
 *
 * <p>Uniform across brokers: Kafka and Redis both bind operations to a channel shape with @channel,
 * so a single validator covers them.
 */
public final class OperationBindingValidator extends AbstractValidator {

  private static final ShapeId SEND = ShapeId.from("bote#send");
  private static final ShapeId RECEIVE = ShapeId.from("bote#receive");
  private static final ShapeId CHANNEL = ShapeId.from("bote#channel");
  private static final List<ShapeId> PROTOCOLS =
      List.of(
          ShapeId.from("bote#messaging"),
          ShapeId.from("bote#kafkaJson"),
          ShapeId.from("bote#kafkaAvro"),
          ShapeId.from("bote#kafkaProtobuf"),
          ShapeId.from("bote#redisStreamsJson"),
          ShapeId.from("bote#redisPubSubJson"));

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();

    for (ServiceShape service : model.getServiceShapes()) {
      if (PROTOCOLS.stream().noneMatch(service::hasTrait)) {
        continue;
      }
      for (ShapeId operationId : service.getAllOperations()) {
        validateOperation(model, model.expectShape(operationId, OperationShape.class), events);
      }
    }

    return events;
  }

  private void validateOperation(
      Model model, OperationShape operation, List<ValidationEvent> events) {
    boolean isSend = operation.hasTrait(SEND);
    boolean isReceive = operation.hasTrait(RECEIVE);

    if (!isSend && !isReceive) {
      return;
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
                  + "(the message value written to the channel)."));
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
              "@receive operation output must contain a member targeting a @streaming union "
                  + "(the consumer's subscription)."));
    }
  }
}
