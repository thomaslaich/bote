package bote.asyncapi;

import java.util.Optional;
import software.amazon.smithy.build.PluginContext;
import software.amazon.smithy.build.SmithyBuildException;
import software.amazon.smithy.build.SmithyBuildPlugin;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.node.ObjectNode;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.ShapeId;

/**
 * A {@link SmithyBuildPlugin} that emits an AsyncAPI 3.1.0 document for services annotated with a
 * bote protocol trait ({@code @kafkaJson}, {@code @kafkaAvro}, {@code @kafkaProtobuf}, or a Redis
 * protocol trait).
 *
 * <p>Each AsyncAPI document describes a single application, so one file is written per service,
 * named {@code <ServiceName>.asyncapi.json}.
 *
 * <p>By default every bote protocol service in the model is documented. Set the optional {@code service}
 * setting to a service shape ID to target exactly one — the idiomatic way to emit several documents
 * is then one projection per service (mirroring smithy-openapi):
 *
 * <pre>{@code
 * {
 *     "version": "1.0",
 *     "plugins": {
 *         "asyncapi-codegen": {
 *             "service": "smartylighting.device#StreetlightDevice"
 *         }
 *     }
 * }
 * }</pre>
 */
public final class AsyncApiPlugin implements SmithyBuildPlugin {

  @Override
  public String getName() {
    return "asyncapi-codegen";
  }

  @Override
  public void execute(PluginContext context) {
    Model model = context.getModel();
    Optional<ShapeId> target =
        context.getSettings().getStringMember("service").map(n -> ShapeId.from(n.getValue()));

    boolean matched = false;
    for (ServiceShape service : model.getServiceShapes()) {
      if (!AsyncApiConverter.isBoteService(service)) {
        continue;
      }
      if (target.isPresent() && !service.getId().equals(target.get())) {
        continue;
      }
      matched = true;
      ObjectNode document = new AsyncApiConverter(model, service).convert();
      context.getFileManifest().writeJson(service.getId().getName() + ".asyncapi.json", document);
    }

    if (target.isPresent() && !matched) {
      throw new SmithyBuildException(
          "asyncapi-codegen: `service` "
              + target.get()
              + " is not a bote protocol service in the model.");
    }
  }
}
