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
 * Validates that every messaging operation of a bote service binds to a channel, that invocations
 * use command input and optional reply output shapes, and that subscriptions stream event shapes.
 */
public final class OperationBindingValidator extends AbstractValidator {

  private static final ShapeId INVOCATION = ShapeId.from("bote#invocation");
  private static final ShapeId SUBSCRIPTION = ShapeId.from("bote#subscription");
  private static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");
  private static final ShapeId REDIS_STREAM = ShapeId.from("bote#redisStream");
  private static final ShapeId REDIS_CHANNEL = ShapeId.from("bote#redisChannel");
  private static final ShapeId COMMAND = ShapeId.from("bote#command");
  private static final ShapeId EVENT = ShapeId.from("bote#event");
  private static final ShapeId REPLY = ShapeId.from("bote#reply");
  private static final List<ShapeId> PROTOCOLS =
      List.of(
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
    boolean isInvocation = operation.hasTrait(INVOCATION);
    boolean isSubscription = operation.hasTrait(SUBSCRIPTION);

    if (!isInvocation && !isSubscription) {
      return;
    }

    if (!operation.hasTrait(KAFKA_TOPIC)
        && !operation.hasTrait(REDIS_STREAM)
        && !operation.hasTrait(REDIS_CHANNEL)) {
      events.add(
          error(
              operation,
              "Messaging operations must declare a broker address trait."));
    }

    if (isInvocation) {
      if (operation.getInput().isEmpty()) {
        events.add(
            error(
                operation,
                "@invocation operations must define an input shape "
                    + "(the message value written to the channel)."));
      } else {
        validateMessageTrait(model, operation, operation.getInput().get(), COMMAND, "input", events);
      }
      operation
          .getOutput()
          .ifPresent(output -> validateMessageTrait(model, operation, output, REPLY, "output", events));
    }

    if (isSubscription) {
      if (operation.getOutput().isEmpty()) {
        events.add(
            error(
                operation,
                "@subscription operations must define an output shape "
                    + "containing a @streaming union member."));
      } else {
        operation
            .getOutput()
            .flatMap(id -> model.getShape(id).flatMap(Shape::asStructureShape))
            .ifPresent(output -> validateStreamingUnion(model, operation, output, events));
      }
    }
  }

  private void validateMessageTrait(
      Model model,
      OperationShape operation,
      ShapeId messageId,
      ShapeId expectedTrait,
      String position,
      List<ValidationEvent> events) {
    Shape message = model.expectShape(messageId);
    if (!message.hasTrait(expectedTrait)) {
      events.add(
          error(
              operation,
              String.format(
                  "@invocation %s shape '%s' must be annotated with %s.",
                  position, messageId, expectedTrait)));
    }
  }

  private void validateStreamingUnion(
      Model model, OperationShape operation, StructureShape output, List<ValidationEvent> events) {
    boolean hasStreamingUnion = false;
    for (var member : output.getAllMembers().values()) {
      var union = model.getShape(member.getTarget()).flatMap(Shape::asUnionShape);
      if (union.isEmpty() || !union.get().hasTrait(StreamingTrait.ID)) {
        continue;
      }
      hasStreamingUnion = true;
      for (var eventMember : union.get().getAllMembers().values()) {
        Shape eventShape = model.expectShape(eventMember.getTarget());
        if (!eventShape.hasTrait(EVENT)) {
          events.add(
              error(
                  operation,
                  String.format(
                      "@subscription stream member '%s' targets '%s', which must be annotated"
                          + " with %s.",
                      eventMember.getMemberName(), eventMember.getTarget(), EVENT)));
        }
      }
    }

    if (!hasStreamingUnion) {
      events.add(
          error(
              operation,
              "@subscription operation output must contain a member targeting a @streaming union "
                  + "(the client's subscription view)."));
    }
  }
}
