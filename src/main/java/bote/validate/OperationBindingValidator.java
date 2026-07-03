package bote.validate;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.shapes.UnionShape;
import software.amazon.smithy.model.traits.StreamingTrait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates the operations of bote protocol services: every messaging operation carries exactly
 * one broker operation trait matching the service's protocol, produce-side operations take a
 * {@code @command} input and no output (no current protocol supports replies), and consume-side
 * operations stream {@code @event} shapes. For {@code @kafkaJson} services with {@code
 * eventDiscrimination: NONE}, also enforces that streaming unions declare a single event type.
 */
public final class OperationBindingValidator extends AbstractValidator {

  private static final ShapeId KAFKA_PRODUCE = ShapeId.from("bote#kafkaProduce");
  private static final ShapeId KAFKA_CONSUME = ShapeId.from("bote#kafkaConsume");
  private static final ShapeId REDIS_STREAM_ADD = ShapeId.from("bote#redisStreamAdd");
  private static final ShapeId REDIS_STREAM_READ = ShapeId.from("bote#redisStreamRead");
  private static final ShapeId REDIS_PUBLISH = ShapeId.from("bote#redisPublish");
  private static final ShapeId REDIS_SUBSCRIBE = ShapeId.from("bote#redisSubscribe");
  private static final ShapeId COMMAND = ShapeId.from("bote#command");
  private static final ShapeId EVENT = ShapeId.from("bote#event");
  private static final ShapeId KAFKA_JSON = ShapeId.from("bote#kafkaJson");

  private static final Set<ShapeId> PRODUCE_TRAITS =
      Set.of(KAFKA_PRODUCE, REDIS_STREAM_ADD, REDIS_PUBLISH);
  private static final Set<ShapeId> CONSUME_TRAITS =
      Set.of(KAFKA_CONSUME, REDIS_STREAM_READ, REDIS_SUBSCRIBE);

  private static final Set<ShapeId> KAFKA_OPS = Set.of(KAFKA_PRODUCE, KAFKA_CONSUME);
  private static final Set<ShapeId> REDIS_STREAM_OPS = Set.of(REDIS_STREAM_ADD, REDIS_STREAM_READ);
  private static final Set<ShapeId> REDIS_PUBSUB_OPS = Set.of(REDIS_PUBLISH, REDIS_SUBSCRIBE);

  /** Protocol trait -> the broker operation traits its operations may carry. */
  private static final Map<ShapeId, Set<ShapeId>> PROTOCOL_OPS =
      Map.of(
          KAFKA_JSON, KAFKA_OPS,
          ShapeId.from("bote#kafkaAvro"), KAFKA_OPS,
          ShapeId.from("bote#kafkaProtobuf"), KAFKA_OPS,
          ShapeId.from("bote#redisStreamsJson"), REDIS_STREAM_OPS,
          ShapeId.from("bote#redisPubSubJson"), REDIS_PUBSUB_OPS);

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();

    for (ServiceShape service : model.getServiceShapes()) {
      Set<ShapeId> allowedOps = new LinkedHashSet<>();
      boolean isProtocolService = false;
      for (Map.Entry<ShapeId, Set<ShapeId>> protocol : PROTOCOL_OPS.entrySet()) {
        if (service.hasTrait(protocol.getKey())) {
          isProtocolService = true;
          allowedOps.addAll(protocol.getValue());
        }
      }
      if (!isProtocolService) {
        continue;
      }
      for (ShapeId operationId : service.getAllOperations()) {
        validateOperation(
            model,
            service,
            model.expectShape(operationId, OperationShape.class),
            allowedOps,
            events);
      }
    }

    return events;
  }

  private void validateOperation(
      Model model,
      ServiceShape service,
      OperationShape operation,
      Set<ShapeId> allowedOps,
      List<ValidationEvent> events) {
    List<ShapeId> brokerTraits = new ArrayList<>();
    for (ShapeId traitId : PRODUCE_TRAITS) {
      if (operation.hasTrait(traitId)) {
        brokerTraits.add(traitId);
      }
    }
    for (ShapeId traitId : CONSUME_TRAITS) {
      if (operation.hasTrait(traitId)) {
        brokerTraits.add(traitId);
      }
    }

    if (brokerTraits.isEmpty()) {
      return;
    }
    if (brokerTraits.size() > 1) {
      events.add(
          error(
              operation,
              String.format(
                  "Operations must carry exactly one broker operation trait, found %s.",
                  brokerTraits)));
      return;
    }

    ShapeId brokerTrait = brokerTraits.get(0);
    if (!allowedOps.contains(brokerTrait)) {
      events.add(
          error(
              operation,
              String.format(
                  "Operation carries %s, which does not match the protocol of service '%s' "
                      + "(expected one of %s).",
                  brokerTrait, service.getId(), allowedOps)));
      return;
    }

    if (PRODUCE_TRAITS.contains(brokerTrait)) {
      validateProduce(model, operation, brokerTrait, events);
    } else {
      validateConsume(model, service, operation, brokerTrait, events);
    }
  }

  private void validateProduce(
      Model model, OperationShape operation, ShapeId brokerTrait, List<ValidationEvent> events) {
    if (operation.getInput().isEmpty()) {
      events.add(
          error(
              operation,
              String.format(
                  "@%s operations must define an input shape "
                      + "(the message value written to the channel).",
                  brokerTrait.getName())));
    } else {
      ShapeId inputId = operation.getInput().get();
      if (!model.expectShape(inputId).hasTrait(COMMAND)) {
        events.add(
            error(
                operation,
                String.format(
                    "@%s input shape '%s' must be annotated with %s.",
                    brokerTrait.getName(), inputId, COMMAND)));
      }
    }
    if (operation.getOutput().isPresent()) {
      events.add(
          error(
              operation,
              String.format(
                  "@%s operations must not define an output: no current protocol "
                      + "supports reply semantics.",
                  brokerTrait.getName())));
    }
  }

  private void validateConsume(
      Model model,
      ServiceShape service,
      OperationShape operation,
      ShapeId brokerTrait,
      List<ValidationEvent> events) {
    if (operation.getOutput().isEmpty()) {
      events.add(
          error(
              operation,
              String.format(
                  "@%s operations must define an output shape "
                      + "containing a @streaming union member.",
                  brokerTrait.getName())));
      return;
    }
    Optional<StructureShape> output =
        operation.getOutput().flatMap(id -> model.getShape(id).flatMap(Shape::asStructureShape));
    output.ifPresent(
        shape -> validateStreamingUnion(model, service, operation, brokerTrait, shape, events));
  }

  private void validateStreamingUnion(
      Model model,
      ServiceShape service,
      OperationShape operation,
      ShapeId brokerTrait,
      StructureShape output,
      List<ValidationEvent> events) {
    boolean hasStreamingUnion = false;
    for (var member : output.getAllMembers().values()) {
      Optional<UnionShape> union = model.getShape(member.getTarget()).flatMap(Shape::asUnionShape);
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
                      "@%s stream member '%s' targets '%s', which must be annotated with %s.",
                      brokerTrait.getName(),
                      eventMember.getMemberName(),
                      eventMember.getTarget(),
                      EVENT)));
        }
      }
      if (discriminationIsNone(service) && union.get().getAllMembers().size() > 1) {
        events.add(
            error(
                operation,
                String.format(
                    "@streaming union '%s' declares %d event types, but the service uses "
                        + "eventDiscrimination: NONE, which allows at most one event type "
                        + "per channel.",
                    union.get().getId(), union.get().getAllMembers().size())));
      }
    }

    if (!hasStreamingUnion) {
      events.add(
          error(
              operation,
              String.format(
                  "@%s operation output must contain a member targeting a @streaming union "
                      + "(the client's subscription view).",
                  brokerTrait.getName())));
    }
  }

  private boolean discriminationIsNone(ServiceShape service) {
    return service
        .findTrait(KAFKA_JSON)
        .map(t -> t.toNode().expectObjectNode())
        .flatMap(n -> n.getStringMember("eventDiscrimination"))
        .map(s -> s.getValue().equals("NONE"))
        .orElse(false);
  }
}
