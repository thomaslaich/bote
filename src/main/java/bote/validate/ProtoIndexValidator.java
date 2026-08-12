package bote.validate;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.neighbor.Walker;
import software.amazon.smithy.model.shapes.MemberShape;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.traits.StreamingTrait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates that every member of every payload structure and streaming union reachable from a
 * {@code @kafkaProtobuf} service carries an explicit {@code alloy.proto#protoIndex}. Implicit field
 * numbering breaks the protobuf wire format when members are reordered or removed, which defeats
 * the point of a schema contract.
 *
 * <p>{@code @kafkaHeader} members need an index too, even though they travel only as Kafka headers:
 * Alloy requires indexes on all members of a shape or none, so the header member's number is
 * reserved rather than omitted.
 */
public final class ProtoIndexValidator extends AbstractValidator {

  private static final ShapeId KAFKA_PROTOBUF = ShapeId.from("bote#kafkaProtobuf");
  private static final ShapeId KAFKA_PRODUCE = ShapeId.from("bote#kafkaProduce");
  private static final ShapeId KAFKA_CONSUME = ShapeId.from("bote#kafkaConsume");
  private static final ShapeId PROTO_INDEX = ShapeId.from("alloy.proto#protoIndex");

  @Override
  public List<ValidationEvent> validate(Model model) {
    Set<ShapeId> roots = new LinkedHashSet<>();
    for (ServiceShape service : model.getServiceShapes()) {
      if (!service.hasTrait(KAFKA_PROTOBUF)) {
        continue;
      }
      for (ShapeId operationId : service.getAllOperations()) {
        OperationShape operation = model.expectShape(operationId, OperationShape.class);
        if (operation.hasTrait(KAFKA_PRODUCE)) {
          operation.getInput().ifPresent(roots::add);
        }
        if (operation.hasTrait(KAFKA_CONSUME)) {
          collectStreamingUnions(model, operation, roots);
        }
      }
    }

    List<ValidationEvent> events = new ArrayList<>();
    Set<Shape> closure = new LinkedHashSet<>();
    Walker walker = new Walker(model);
    for (ShapeId root : roots) {
      model.getShape(root).ifPresent(shape -> closure.addAll(walker.walkShapes(shape)));
    }

    for (Shape shape : closure) {
      if (!shape.isStructureShape() && !shape.isUnionShape()) {
        continue;
      }
      for (MemberShape member : shape.members()) {
        if (member.hasTrait(PROTO_INDEX)) {
          continue;
        }
        events.add(
            error(
                member,
                String.format(
                    "Member '%s' is part of a @kafkaProtobuf payload and must carry an explicit "
                        + "alloy.proto#protoIndex: implicit field numbering breaks the wire "
                        + "format when members are reordered or removed.",
                    member.getId())));
      }
    }
    return events;
  }

  private void collectStreamingUnions(Model model, OperationShape operation, Set<ShapeId> roots) {
    operation
        .getOutput()
        .flatMap(id -> model.getShape(id).flatMap(Shape::asStructureShape))
        .ifPresent(
            output -> {
              for (MemberShape member : output.getAllMembers().values()) {
                model
                    .getShape(member.getTarget())
                    .flatMap(Shape::asUnionShape)
                    .filter(union -> union.hasTrait(StreamingTrait.ID))
                    .ifPresent(union -> roots.add(union.getId()));
              }
            });
  }
}
