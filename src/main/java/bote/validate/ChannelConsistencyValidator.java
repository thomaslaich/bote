package bote.validate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.Node;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.traits.Trait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Enforces channel-level consistency across the operations that share a broker address, turning the
 * single-owner contract model into checked invariants:
 *
 * <ul>
 *   <li>a channel address is owned by exactly one service
 *   <li>every operation on an address declares identical channel values (topic settings, {@code
 *       maxLen}, ...)
 *   <li>{@code @bote.infra#kafkaTopicConfig} is declared on at most one operation per address
 *   <li>at most one {@code @command} type per address (warning: bote does not yet specify how
 *       command types are discriminated on a shared channel)
 * </ul>
 */
public final class ChannelConsistencyValidator extends AbstractValidator {

  /** One broker's address vocabulary: its operation traits and their address member. */
  private record Broker(
      String family, String addressMember, List<ShapeId> produce, List<ShapeId> consume) {}

  private static final List<Broker> BROKERS =
      List.of(
          new Broker(
              "kafka",
              "topic",
              List.of(ShapeId.from("bote#kafkaProduce")),
              List.of(ShapeId.from("bote#kafkaConsume"))),
          new Broker(
              "redis-stream",
              "stream",
              List.of(ShapeId.from("bote#redisStreamAdd")),
              List.of(ShapeId.from("bote#redisStreamRead"))),
          new Broker(
              "redis-pubsub",
              "channel",
              List.of(ShapeId.from("bote#redisPublish")),
              List.of(ShapeId.from("bote#redisSubscribe"))));

  private static final ShapeId KAFKA_TOPIC_CONFIG = ShapeId.from("bote.infra#kafkaTopicConfig");
  private static final List<ShapeId> PROTOCOLS =
      List.of(
          ShapeId.from("bote#kafkaJson"),
          ShapeId.from("bote#kafkaAvro"),
          ShapeId.from("bote#kafkaProtobuf"),
          ShapeId.from("bote#redisStreamsJson"),
          ShapeId.from("bote#redisPubSubJson"));

  /** One operation's binding to a channel address. */
  private record Binding(
      ServiceShape service, OperationShape operation, Trait brokerTrait, boolean isProduce) {}

  @Override
  public List<ValidationEvent> validate(Model model) {
    // family + address name -> bindings, in model order.
    Map<String, List<Binding>> channels = new LinkedHashMap<>();

    for (ServiceShape service : model.getServiceShapes()) {
      if (PROTOCOLS.stream().noneMatch(service::hasTrait)) {
        continue;
      }
      for (ShapeId operationId : service.getAllOperations()) {
        OperationShape operation = model.expectShape(operationId, OperationShape.class);
        for (Broker broker : BROKERS) {
          collectBindings(service, operation, broker, broker.produce(), true, channels);
          collectBindings(service, operation, broker, broker.consume(), false, channels);
        }
      }
    }

    List<ValidationEvent> events = new ArrayList<>();
    for (Map.Entry<String, List<Binding>> channel : channels.entrySet()) {
      String address = channel.getKey().substring(channel.getKey().indexOf(':') + 1);
      validateChannel(address, channel.getValue(), events);
    }
    return events;
  }

  private void collectBindings(
      ServiceShape service,
      OperationShape operation,
      Broker broker,
      List<ShapeId> traitIds,
      boolean isProduce,
      Map<String, List<Binding>> channels) {
    for (ShapeId traitId : traitIds) {
      Optional<Trait> trait = operation.findTrait(traitId);
      if (trait.isEmpty()) {
        continue;
      }
      String address =
          trait
              .get()
              .toNode()
              .expectObjectNode()
              .expectStringMember(broker.addressMember())
              .getValue();
      channels
          .computeIfAbsent(broker.family() + ":" + address, k -> new ArrayList<>())
          .add(new Binding(service, operation, trait.get(), isProduce));
    }
  }

  private void validateChannel(
      String address, List<Binding> bindings, List<ValidationEvent> events) {
    Binding first = bindings.get(0);

    Set<ShapeId> owners = new LinkedHashSet<>();
    bindings.forEach(b -> owners.add(b.service().getId()));
    if (owners.size() > 1) {
      for (Binding binding : bindings) {
        events.add(
            error(
                binding.operation(),
                String.format(
                    "Channel address '%s' is bound by operations of multiple services (%s). "
                        + "A channel has exactly one owning service.",
                    address, owners)));
      }
    }

    // The produce and consume traits of a broker share their member shape, so the channel
    // values (compacted, maxLen, ...) are comparable as nodes even across trait types.
    Node firstNode = first.brokerTrait().toNode();
    for (Binding binding : bindings.subList(1, bindings.size())) {
      if (!binding.brokerTrait().toNode().equals(firstNode)) {
        events.add(
            error(
                binding.operation(),
                String.format(
                    "Operations sharing channel address '%s' declare different channel values; "
                        + "they must be identical (first declared on '%s').",
                    address, first.operation().getId())));
      }
    }

    List<Binding> configured =
        bindings.stream().filter(b -> b.operation().hasTrait(KAFKA_TOPIC_CONFIG)).toList();
    if (configured.size() > 1) {
      for (Binding binding : configured.subList(1, configured.size())) {
        events.add(
            error(
                binding.operation(),
                String.format(
                    "@kafkaTopicConfig for channel address '%s' is already declared on '%s'; "
                        + "declare it on at most one operation per topic.",
                    address, configured.get(0).operation().getId())));
      }
    }

    Set<ShapeId> commandTypes = new LinkedHashSet<>();
    for (Binding binding : bindings) {
      if (binding.isProduce()) {
        binding.operation().getInput().ifPresent(commandTypes::add);
      }
    }
    if (commandTypes.size() > 1) {
      events.add(
          warning(
              first.operation(),
              String.format(
                  "Channel address '%s' carries %d command types (%s), but bote does not "
                      + "specify how command types are discriminated on a shared channel. "
                      + "Consider one command topic per command type.",
                  address, commandTypes.size(), commandTypes)));
    }
  }
}
