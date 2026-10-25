package bote.validate;

import bote.BoteIndex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.MemberShape;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.shapes.UnionShape;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates that at most one member in each Kafka message structure is annotated with @kafkaKey.
 *
 * <p>The message key uniquely identifies a record within a topic and determines which partition it
 * is routed to. Having more than one key member is ambiguous and is therefore an error.
 *
 * <p>The structures checked are the inputs of {@code @kafkaTopic} operations and the events of
 * {@code @kafkaTopic} channels.
 */
public final class KafkaKeyValidator extends AbstractValidator {

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();

    Set<ShapeId> messageStructures = new HashSet<>();

    for (OperationShape operation : model.getOperationShapes()) {
      if (operation.hasTrait(BoteIndex.KAFKA_TOPIC)) {
        operation.getInput().ifPresent(messageStructures::add);
      }
    }
    for (UnionShape channel : model.getUnionShapes()) {
      if (channel.hasTrait(BoteIndex.KAFKA_TOPIC)) {
        channel.getAllMembers().values().forEach(m -> messageStructures.add(m.getTarget()));
      }
    }

    for (ShapeId structId : messageStructures) {
      model
          .getShape(structId)
          .flatMap(Shape::asStructureShape)
          .ifPresent(structure -> validateStructure(structure, events));
    }

    return events;
  }

  private void validateStructure(StructureShape structure, List<ValidationEvent> events) {
    List<MemberShape> keyMembers =
        structure.getAllMembers().values().stream()
            .filter(member -> member.hasTrait(BoteIndex.KAFKA_KEY))
            .collect(Collectors.toList());

    if (keyMembers.size() > 1) {
      for (MemberShape member : keyMembers) {
        events.add(
            error(
                member,
                String.format(
                    "Structure '%s' has %d members annotated with @kafkaKey. "
                        + "At most one member may serve as the Kafka message key.",
                    structure.getId().getName(), keyMembers.size())));
      }
    }
  }
}
