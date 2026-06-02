package bote.validate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates that no two @kafkaTopic shapes declare the same topic name.
 *
 * <p>A Kafka topic is a single physical entity. With topics modelled as first-class shapes, the
 * durability/retention contract lives once on the shape, so per-operation consistency no longer
 * needs checking. What remains is that two distinct channel shapes must not claim the same
 * underlying topic name — that would be two contracts for one topic.
 */
public final class KafkaTopicNameValidator extends AbstractValidator {

  private static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();
    Map<String, ShapeId> seen = new LinkedHashMap<>();

    for (Shape shape : model.getShapesWithTrait(KAFKA_TOPIC)) {
      String name =
          shape
              .findTrait(KAFKA_TOPIC)
              .map(t -> t.toNode().expectObjectNode().expectStringMember("name").getValue())
              .orElse(null);
      if (name == null) {
        continue;
      }

      ShapeId existing = seen.putIfAbsent(name, shape.getId());
      if (existing != null) {
        events.add(
            error(
                shape,
                String.format(
                    "Topic name '%s' is already declared by '%s'. "
                        + "Each Kafka topic must be modelled by exactly one @kafkaTopic shape.",
                    name, existing)));
      }
    }

    return events;
  }
}
