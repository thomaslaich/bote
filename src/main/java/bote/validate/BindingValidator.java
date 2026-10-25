package bote.validate;

import bote.BoteIndex;
import bote.BoteIndex.Address;
import bote.BoteIndex.AddressTrait;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.MemberShape;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.UnionShape;
import software.amazon.smithy.model.traits.InputTrait;
import software.amazon.smithy.model.traits.OutputTrait;
import software.amazon.smithy.model.validation.AbstractValidator;
import software.amazon.smithy.model.validation.ValidationEvent;

/**
 * Validates how bote services bind their operations and channels:
 *
 * <ul>
 *   <li>every operation of a bote service carries the address trait of the service's protocol and
 *       takes an {@code @input} structure, its command; only Redis Streams operations may have an
 *       output, their reply, which is an {@code @output} structure
 *   <li>every channel listed in {@code @channels} carries that address trait, and its members target
 *       {@code @event} structures; with {@code eventDiscrimination: NONE} it has one member
 *   <li>every union with an address trait is a channel of exactly one bote service
 *   <li>an {@code @event} structure is never an operation input or output
 * </ul>
 */
public final class BindingValidator extends AbstractValidator {

  @Override
  public List<ValidationEvent> validate(Model model) {
    List<ValidationEvent> events = new ArrayList<>();
    BoteIndex index = BoteIndex.of(model);

    // Channel union -> the services that list it.
    Map<ShapeId, List<ShapeId>> listedBy = new LinkedHashMap<>();
    for (ServiceShape service : model.getServiceShapes()) {
      Optional<AddressTrait> addressTrait = BoteIndex.addressTrait(service);
      if (addressTrait.isEmpty()) {
        if (service.hasTrait(BoteIndex.CHANNELS)) {
          events.add(
              error(
                  service,
                  "@channels requires a bote protocol trait on the service, such as @kafkaJson."));
        }
        continue;
      }
      for (ShapeId operationId : service.getAllOperations()) {
        validateOperation(
            model,
            service,
            model.expectShape(operationId, OperationShape.class),
            addressTrait.get(),
            events);
      }
      for (UnionShape channel : index.channels(service)) {
        listedBy.computeIfAbsent(channel.getId(), id -> new ArrayList<>()).add(service.getId());
        validateChannel(model, service, channel, addressTrait.get(), events);
      }
    }

    for (UnionShape union : model.getUnionShapes()) {
      if (BoteIndex.addresses(union).isEmpty()) {
        continue;
      }
      List<ShapeId> services = listedBy.getOrDefault(union.getId(), List.of());
      if (services.isEmpty()) {
        events.add(
            error(
                union,
                "A union with an address trait is a channel and must be listed in the @channels "
                    + "of the bote service that owns it."));
      } else if (services.size() > 1) {
        events.add(
            error(
                union,
                String.format(
                    "Channel is listed by %d services (%s). A channel has exactly one owning "
                        + "service.",
                    services.size(), services)));
      }
    }

    for (OperationShape operation : model.getOperationShapes()) {
      operation.getInput().ifPresent(id -> validateNotEvent(model, operation, id, "input", events));
      operation
          .getOutput()
          .ifPresent(id -> validateNotEvent(model, operation, id, "output", events));
    }

    return events;
  }

  private void validateOperation(
      Model model,
      ServiceShape service,
      OperationShape operation,
      AddressTrait expected,
      List<ValidationEvent> events) {
    if (!validateAddress(service, operation, "Operations", expected, events)) {
      return;
    }
    String trait = expected.id().getName();
    if (operation.getInput().isEmpty()) {
      events.add(
          error(
              operation,
              String.format(
                  "@%s operations must define an input: the command the owner receives.",
                  trait)));
    } else {
      validateMarker(model, operation, operation.getInput().get(), InputTrait.ID, "input", events);
    }
    if (operation.getOutput().isPresent() && expected.id().equals(BoteIndex.REDIS_STREAM)) {
      validateMarker(
          model, operation, operation.getOutput().get(), OutputTrait.ID, "output", events);
    } else if (operation.getOutput().isPresent()) {
      events.add(
          error(
              operation,
              String.format(
                  "@%s operations must not define an output: only Redis Streams operations "
                      + "support request/reply.",
                  trait)));
    }
  }

  private void validateChannel(
      Model model,
      ServiceShape service,
      UnionShape channel,
      AddressTrait expected,
      List<ValidationEvent> events) {
    validateAddress(service, channel, "Channels", expected, events);
    for (MemberShape member : channel.getAllMembers().values()) {
      Optional<Shape> target = model.getShape(member.getTarget());
      if (target.isEmpty() || !target.get().isStructureShape() || !target.get().hasTrait(BoteIndex.EVENT)) {
        events.add(
            error(
                member,
                String.format(
                    "Channel member '%s' targets '%s', which must be a structure annotated with "
                        + "@event.",
                    member.getMemberName(), member.getTarget())));
      }
    }
    boolean none = BoteIndex.eventDiscrimination(service).map("NONE"::equals).orElse(false);
    if (none && channel.getAllMembers().size() > 1) {
      events.add(
          error(
              channel,
              String.format(
                  "Channel declares %d event types, but service '%s' uses eventDiscrimination: "
                      + "NONE, which allows one event type per channel.",
                  channel.getAllMembers().size(), service.getId())));
    }
  }

  /** Checks that the shape carries exactly the expected address trait; returns whether it does. */
  private boolean validateAddress(
      ServiceShape service,
      Shape shape,
      String kind,
      AddressTrait expected,
      List<ValidationEvent> events) {
    List<Address> addresses = BoteIndex.addresses(shape);
    if (addresses.isEmpty()) {
      events.add(
          error(
              shape,
              String.format(
                  "%s of bote service '%s' must carry @%s.",
                  kind, service.getId(), expected.id().getName())));
      return false;
    }
    if (addresses.size() > 1) {
      events.add(
          error(
              shape,
              String.format(
                  "Shapes carry at most one address trait, found %s.",
                  addresses.stream().map(a -> a.trait().id().toString()).toList())));
      return false;
    }
    ShapeId actual = addresses.get(0).trait().id();
    if (!actual.equals(expected.id())) {
      events.add(
          error(
              shape,
              String.format(
                  "Shape carries @%s, which does not match the protocol of service '%s' "
                      + "(expected @%s).",
                  actual.getName(), service.getId(), expected.id().getName())));
      return false;
    }
    return true;
  }

  private void validateMarker(
      Model model,
      OperationShape operation,
      ShapeId shape,
      ShapeId marker,
      String position,
      List<ValidationEvent> events) {
    if (model.getShape(shape).filter(s -> s.hasTrait(marker)).isEmpty()) {
      events.add(
          error(
              operation,
              String.format(
                  "Operation %s '%s' must be annotated with @%s, as every %s of a bote service "
                      + "is. Inline `%s := { ... }` adds it.",
                  position, shape, marker.getName(), position, position)));
    }
  }

  private void validateNotEvent(
      Model model,
      OperationShape operation,
      ShapeId shape,
      String position,
      List<ValidationEvent> events) {
    if (model.getShape(shape).filter(s -> s.hasTrait(BoteIndex.EVENT)).isPresent()) {
      events.add(
          error(
              operation,
              String.format(
                  "Operation %s '%s' is an @event structure. Events belong to channels and are "
                      + "never operation inputs or outputs.",
                  position, shape)));
    }
  }
}
