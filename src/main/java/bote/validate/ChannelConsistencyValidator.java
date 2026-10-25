package bote.validate;

import bote.BoteIndex;
import bote.BoteIndex.Address;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.UnionShape;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Enforces address-level consistency across the operations and channels that share a broker
 * address, turning the single-owner contract model into checked invariants:
 *
 * <ul>
 *   <li>an address is owned by exactly one service
 *   <li>an address has one use: it is one channel, or the address of command operations, never both
 *   <li>operations on one address declare identical address values (compaction, {@code maxLen},
 *       ...)
 *   <li>{@code @bote.infra#kafkaTopicConfig} is declared on at most one shape per address
 * </ul>
 */
public final class ChannelConsistencyValidator extends AbstractValidator {

  /** One operation's or channel's binding to an address. */
  private record Binding(ServiceShape service, Shape shape, Address address) {
    boolean isChannel() {
      return shape.isUnionShape();
    }
  }

  @Override
  public List<ValidationEvent> validate(Model model) {
    BoteIndex index = BoteIndex.of(model);
    // broker + address name -> bindings, in model order.
    Map<String, List<Binding>> addresses = new LinkedHashMap<>();

    for (ServiceShape service : index.services()) {
      for (ShapeId operationId : service.getAllOperations()) {
        collect(service, model.expectShape(operationId), addresses);
      }
      for (UnionShape channel : index.channels(service)) {
        collect(service, channel, addresses);
      }
    }

    List<ValidationEvent> events = new ArrayList<>();
    for (List<Binding> bindings : addresses.values()) {
      validateAddress(bindings, events);
    }
    return events;
  }

  private void collect(ServiceShape service, Shape shape, Map<String, List<Binding>> addresses) {
    for (Address address : BoteIndex.addresses(shape)) {
      addresses
          .computeIfAbsent(address.trait().broker() + ":" + address.name(), k -> new ArrayList<>())
          .add(new Binding(service, shape, address));
    }
  }

  private void validateAddress(List<Binding> bindings, List<ValidationEvent> events) {
    Binding first = bindings.get(0);
    String name = first.address().name();

    Set<ShapeId> owners = new LinkedHashSet<>();
    bindings.forEach(b -> owners.add(b.service().getId()));
    if (owners.size() > 1) {
      for (Binding binding : bindings) {
        events.add(
            error(
                binding.shape(),
                String.format(
                    "Address '%s' is bound by multiple services (%s). An address has exactly one "
                        + "owning service.",
                    name, owners)));
      }
    }

    Optional<Binding> channel = bindings.stream().filter(Binding::isChannel).findFirst();
    if (channel.isPresent() && bindings.size() > 1) {
      for (Binding binding : bindings) {
        if (!binding.equals(channel.get())) {
          events.add(
              error(
                  binding.shape(),
                  String.format(
                      "Address '%s' is the address of channel '%s'. A channel's address carries "
                          + "only that channel's events.",
                      name, channel.get().shape().getId())));
        }
      }
      return;
    }

    for (Binding binding : bindings.subList(1, bindings.size())) {
      if (!binding.address().value().equals(first.address().value())) {
        events.add(
            error(
                binding.shape(),
                String.format(
                    "Operations sharing address '%s' declare different address values; they must "
                        + "be identical (first declared on '%s').",
                    name, first.shape().getId())));
      }
    }

    List<Binding> configured =
        bindings.stream().filter(b -> b.shape().hasTrait(BoteIndex.KAFKA_TOPIC_CONFIG)).toList();
    for (Binding binding : configured.stream().skip(1).toList()) {
      events.add(
          error(
              binding.shape(),
              String.format(
                  "@kafkaTopicConfig for address '%s' is already declared on '%s'; declare it on "
                      + "at most one shape per topic.",
                  name, configured.get(0).shape().getId())));
    }
  }
}
