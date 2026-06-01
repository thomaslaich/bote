package bote.asyncapi;

import software.amazon.smithy.build.PluginContext;
import software.amazon.smithy.build.SmithyBuildPlugin;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.ServiceShape;

/**
 * A {@link SmithyBuildPlugin} that emits an AsyncAPI 3.0.0 document for every service annotated
 * with a bote Kafka protocol trait ({@code @kafkaJson}, {@code @kafkaAvro}, or
 * {@code @kafkaProtobuf}).
 *
 * <p>Enable it in {@code smithy-build.json}:
 *
 * <pre>{@code
 * {
 *     "version": "1.0",
 *     "plugins": {
 *         "asyncapi": {}
 *     }
 * }
 * }</pre>
 *
 * <p>One file is written per service, named {@code <ServiceName>.asyncapi.json}.
 */
public final class AsyncApiPlugin implements SmithyBuildPlugin {

  @Override
  public String getName() {
    return "asyncapi";
  }

  @Override
  public void execute(PluginContext context) {
    Model model = context.getModel();
    for (ServiceShape service : model.getServiceShapes()) {
      if (!AsyncApiConverter.isKafkaService(service)) {
        continue;
      }
      var document = new AsyncApiConverter(model, service).convert();
      context.getFileManifest().writeJson(service.getId().getName() + ".asyncapi.json", document);
    }
  }
}
