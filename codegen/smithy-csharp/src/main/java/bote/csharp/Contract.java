package bote.csharp;

import bote.BoteIndex;
import bote.BoteIndex.Address;
import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpNaming;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.smithy.codegen.core.CodegenException;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.knowledge.TopDownIndex;
import software.amazon.smithy.model.shapes.MemberShape;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.shapes.UnionShape;
import software.amazon.smithy.model.traits.UnitTypeTrait;
import software.amazon.smithy.model.traits.synthetic.OriginalShapeIdTrait;

/** A bote service's commands and channels, as the C# generators use them. */
record Contract(ServiceShape service, List<Command> commands, List<Channel> channels) {

  /**
   * An operation the owner receives at its address.
   *
   * @param name the C# name of the operation
   */
  record Command(
      OperationShape operation,
      String name,
      StructureShape input,
      Optional<StructureShape> output,
      Address address) {}

  /**
   * A union of events the owner publishes to its address.
   *
   * @param name the C# name of the union
   */
  record Channel(UnionShape union, String name, List<Event> events, Address address) {}

  /**
   * One member of a channel.
   *
   * @param name the C# name of the union variant
   */
  record Event(MemberShape member, String name, StructureShape shape) {}

  static Contract of(Model model, ServiceShape service) {
    List<Command> commands = new ArrayList<>();
    for (OperationShape operation : TopDownIndex.of(model).getContainedOperations(service)) {
      Address address = BoteIndex.address(operation).orElseThrow(() -> missingAddress(operation));
      commands.add(
          new Command(
              operation,
              CSharpNaming.typeName(operation.getId().getName()),
              model.expectShape(operation.getInputShape(), StructureShape.class),
              operation
                  .getOutput()
                  .map(id -> model.expectShape(id, StructureShape.class))
                  .filter(output -> !isUnit(output)),
              address));
    }

    List<Channel> channels = new ArrayList<>();
    for (UnionShape union : BoteIndex.of(model).channels(service)) {
      Address address = BoteIndex.address(union).orElseThrow(() -> missingAddress(union));
      List<Event> events =
          union.members().stream()
              .map(
                  member ->
                      new Event(
                          member,
                          CSharpNaming.variantName(member),
                          model.expectShape(member.getTarget(), StructureShape.class)))
              .toList();
      channels.add(
          new Channel(union, CSharpNaming.typeName(union.getId().getName()), events, address));
    }

    Contract contract = new Contract(service, List.copyOf(commands), List.copyOf(channels));
    contract.requireOneCommandPerAddress();
    return contract;
  }

  /** The C# name of the service. */
  String serviceName() {
    return CSharpNaming.typeName(service.getId().getName());
  }

  boolean isEmpty() {
    return commands.isEmpty() && channels.isEmpty();
  }

  // The wire format names no command type, so a consumer can only decode one command per address.
  private void requireOneCommandPerAddress() {
    Map<String, Command> byAddress = new LinkedHashMap<>();
    for (Command command : commands) {
      Command other = byAddress.putIfAbsent(command.address().name(), command);
      if (other != null) {
        throw new CodegenException(
            "bote C# code generation needs one command per address, but "
                + other.operation().getId()
                + " and "
                + command.operation().getId()
                + " share `"
                + command.address().name()
                + "`.");
      }
    }
  }

  // Code generation transforms give a Unit output a synthetic structure that remembers it.
  private static boolean isUnit(StructureShape output) {
    return output
        .getTrait(OriginalShapeIdTrait.class)
        .map(trait -> trait.getOriginalId().equals(UnitTypeTrait.UNIT))
        .orElse(false);
  }

  private static CodegenException missingAddress(Shape shape) {
    return new CodegenException(shape.getId() + " has no bote address trait.");
  }
}
