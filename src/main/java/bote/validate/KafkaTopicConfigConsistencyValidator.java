package bote.validate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.Node;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.traits.Trait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates that all operations bound to the same Kafka topic declare identical @kafkaTopicConfig.
 *
 * <p>A Kafka topic is a single physical entity. When multiple operations (e.g., a producer and a
 * consumer) reference the same topic name and each declares @kafkaTopicConfig, those declarations
 * must be identical. Diverging declarations indicate a modelling error and would produce
 * inconsistent IaC output.
 *
 * <p>Operations that omit @kafkaTopicConfig are not checked — they are assumed to defer to
 * broker-level defaults.
 */
public final class KafkaTopicConfigConsistencyValidator extends AbstractValidator {

  private static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");
  private static final ShapeId KAFKA_TOPIC_CONFIG = ShapeId.from("bote#kafkaTopicConfig");

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();

    // topic name -> the first operation that declared a config for it
    Map<String, OperationShape> canonicalOp = new LinkedHashMap<>();
    // topic name -> the Node representation of that config
    Map<String, Node> canonicalNode = new LinkedHashMap<>();

    for (OperationShape operation : model.getOperationShapes()) {
      Optional<Trait> topicTrait = operation.findTrait(KAFKA_TOPIC);
      Optional<Trait> configTrait = operation.findTrait(KAFKA_TOPIC_CONFIG);

      if (topicTrait.isEmpty() || configTrait.isEmpty()) {
        continue;
      }

      String topicName =
          topicTrait.get().toNode().expectObjectNode().expectStringMember("name").getValue();

      Node configNode = configTrait.get().toNode();

      if (!canonicalOp.containsKey(topicName)) {
        canonicalOp.put(topicName, operation);
        canonicalNode.put(topicName, configNode);
      } else if (!canonicalNode.get(topicName).equals(configNode)) {
        events.add(
            error(
                operation,
                String.format(
                    "@kafkaTopicConfig for topic '%s' does not match the declaration on '%s'. All"
                        + " operations bound to the same topic must declare identical"
                        + " configuration.",
                    topicName, canonicalOp.get(topicName).getId())));
      }
    }

    return events;
  }
}
