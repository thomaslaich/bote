package bote.asyncapi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import software.amazon.smithy.build.PluginContext;
import software.amazon.smithy.build.SmithyBuildException;
import software.amazon.smithy.build.SmithyBuildPlugin;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.ObjectNode;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.ShapeId;

/**
 * A {@link SmithyBuildPlugin} that emits AsyncAPI 3.1.0 documents from services annotated with bote
 * protocol traits ({@code @kafkaJson}, {@code @kafkaAvro}, {@code @kafkaProtobuf}, or Redis
 * protocol traits).
 *
 * <p>By default, one file is written per protocol service, named
 * {@code <ServiceName>.asyncapi.json}.
 *
 * <p>Set {@code perspective} to choose whose viewpoint the document takes: {@code "owner"} (the
 * default — the document describes the contract owner, which receives commands and sends events)
 * or {@code "client"} (the document describes a client, which sends commands and receives events).
 *
 * <p>Set {@code service} to target exactly one service. Set {@code services} to aggregate multiple
 * protocol services into one application document:
 *
 * <pre>{@code
 * {
 *     "version": "1.0",
 *     "plugins": {
 *         "asyncapi": {
 *             "services": [
 *                 "examples.kafka.orders#OrderService",
 *                 "examples.kafka.streetlights#StreetlightDevice"
 *             ],
 *             "title": "Kafka Examples",
 *             "filename": "KafkaExamples.asyncapi.json"
 *         }
 *     }
 * }
 * }</pre>
 */
public final class AsyncApiPlugin implements SmithyBuildPlugin {

  @Override
  public String getName() {
    return "asyncapi";
  }

  @Override
  public void execute(PluginContext context) {
    Model model = context.getModel();
    ObjectNode settings = context.getSettings();
    Optional<ShapeId> target =
        settings.getStringMember("service").map(n -> ShapeId.from(n.getValue()));
    Optional<List<ShapeId>> targets =
        settings
            .getArrayMember("services")
            .map(
                array -> {
                  List<ShapeId> result = new ArrayList<>();
                  array
                      .getElements()
                      .forEach(node -> result.add(ShapeId.from(node.expectStringNode().getValue())));
                  return result;
                });
    Optional<String> title = settings.getStringMember("title").map(n -> n.getValue());
    Optional<String> filename = settings.getStringMember("filename").map(n -> n.getValue());
    AsyncApiConverter.Perspective perspective = resolvePerspective(settings);

    if (target.isPresent() && targets.isPresent()) {
      throw new SmithyBuildException(
          "asyncapi: configure either `service` or `services`, not both.");
    }

    if (targets.isPresent()) {
      List<ServiceShape> services = resolveServices(model, targets.get());
      ObjectNode document =
          new AsyncApiConverter(model, services, title, perspective).convert();
      context
          .getFileManifest()
          .writeJson(filename.orElse("AsyncApi.asyncapi.json"), document);
      return;
    }

    boolean matched = false;
    for (ServiceShape service : model.getServiceShapes()) {
      if (!AsyncApiConverter.isBoteService(service)) {
        continue;
      }
      if (target.isPresent() && !service.getId().equals(target.get())) {
        continue;
      }
      matched = true;
      ObjectNode document =
          new AsyncApiConverter(
                  model, List.of(service), Optional.empty(), perspective)
              .convert();
      context
          .getFileManifest()
          .writeJson(filename.orElse(service.getId().getName() + ".asyncapi.json"), document);
    }

    if (target.isPresent() && !matched) {
      throw new SmithyBuildException(
          "asyncapi: `service` "
              + target.get()
              + " is not a bote protocol service in the model.");
    }
  }

  private AsyncApiConverter.Perspective resolvePerspective(ObjectNode settings) {
    String value =
        settings.getStringMember("perspective").map(n -> n.getValue()).orElse("owner");
    return switch (value) {
      case "owner" -> AsyncApiConverter.Perspective.OWNER;
      case "client" -> AsyncApiConverter.Perspective.CLIENT;
      default ->
          throw new SmithyBuildException(
              "asyncapi: `perspective` must be \"owner\" or \"client\", got \"" + value + "\".");
    };
  }

  private List<ServiceShape> resolveServices(Model model, List<ShapeId> serviceIds) {
    List<ServiceShape> services = new ArrayList<>();
    for (ShapeId serviceId : serviceIds) {
      ServiceShape service = model.expectShape(serviceId, ServiceShape.class);
      if (!AsyncApiConverter.isBoteService(service)) {
        throw new SmithyBuildException(
            "asyncapi: `services` member "
                + serviceId
                + " is not a bote protocol service in the model.");
      }
      services.add(service);
    }
    if (services.isEmpty()) {
      throw new SmithyBuildException("asyncapi: `services` must not be empty.");
    }
    return services;
  }
}
