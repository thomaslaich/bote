package bote.csharp;

import bote.BoteIndex;
import bote.BoteIndex.Address;
import io.github.thomaslaich.nsmithy.csharp.codegen.writer.CSharpWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.smithy.model.node.ObjectNode;
import software.amazon.smithy.model.shapes.Shape;

/**
 * Generates the desired state of the Kafka topics a service owns, from {@code
 * bote.infra#kafkaTopicConfig} and the {@code compacted} flag of {@code @kafkaTopic}.
 */
final class KafkaInfrastructureGenerator {

  private record Topic(
      String name,
      Optional<Integer> partitions,
      Optional<Integer> replication,
      Map<String, String> configuration) {}

  private static final Map<String, String> CONFIGURATION_NAMES =
      Map.of(
          "retentionMs", "retention.ms",
          "retentionBytes", "retention.bytes",
          "minInsyncReplicas", "min.insync.replicas",
          "maxMessageBytes", "max.message.bytes");

  private final Contract contract;

  KafkaInfrastructureGenerator(Contract contract) {
    this.contract = contract;
  }

  boolean hasTopics() {
    return !topics().isEmpty();
  }

  void run(CSharpWriter writer) {
    writer.addImport("System.Collections.Generic");
    String entries = topics().stream().map(this::entry).collect(Collectors.joining("\n"));
    writer.write(
        """
        public static class $LKafkaInfrastructure
        {
            public sealed record Topic(
                string Name,
                int? Partitions,
                short? ReplicationFactor,
                IReadOnlyDictionary<string, string> Configuration
            );

            public static IReadOnlyList<Topic> Topics { get; } =
            [
                ${L|}
            ];
        }
        """,
        contract.serviceName(),
        entries);
  }

  private String entry(Topic topic) {
    String configuration =
        topic.configuration().entrySet().stream()
            .map(
                e ->
                    String.format(
                        "        [%s] = %s,",
                        MessagingGenerator.literal(e.getKey()),
                        MessagingGenerator.literal(e.getValue())))
            .collect(Collectors.joining("\n"));
    return String.format(
        """
        new Topic(
            Name: %s,
            Partitions: %s,
            ReplicationFactor: %s,
            Configuration: new Dictionary<string, string>
            {
        %s
            }
        ),\
        """,
        MessagingGenerator.literal(topic.name()),
        topic.partitions().map(String::valueOf).orElse("null"),
        topic.replication().map(String::valueOf).orElse("null"),
        configuration);
  }

  /** One topic per address, from the shapes that carry its configuration. */
  private List<Topic> topics() {
    Map<String, Topic> topics = new LinkedHashMap<>();
    Stream<Shape> shapes =
        Stream.concat(
            contract.commands().stream().map(c -> (Shape) c.operation()),
            contract.channels().stream().map(c -> (Shape) c.union()));
    shapes.forEach(
        shape -> {
          Address address = BoteIndex.address(shape).orElseThrow();
          Optional<ObjectNode> config =
              shape
                  .findTrait(BoteIndex.KAFKA_TOPIC_CONFIG)
                  .map(trait -> trait.toNode().expectObjectNode());
          boolean compacted = BoteIndex.addressFlag(address, "compacted");
          if (config.isEmpty() && !compacted) {
            return;
          }
          Map<String, String> configuration = new LinkedHashMap<>();
          if (compacted) {
            configuration.put("cleanup.policy", "compact");
          }
          config.ifPresent(
              node ->
                  CONFIGURATION_NAMES.keySet().stream()
                      .sorted()
                      .forEach(
                          member ->
                              node.getNumberMember(member)
                                  .ifPresent(
                                      value ->
                                          configuration.put(
                                              CONFIGURATION_NAMES.get(member),
                                              value.getValue().toString()))));
          topics.putIfAbsent(
              address.name(),
              new Topic(
                  address.name(),
                  config
                      .flatMap(n -> n.getNumberMember("partitions"))
                      .map(v -> v.getValue().intValue()),
                  config
                      .flatMap(n -> n.getNumberMember("replicationFactor"))
                      .map(v -> v.getValue().intValue()),
                  configuration));
        });
    return new ArrayList<>(topics.values());
  }
}
