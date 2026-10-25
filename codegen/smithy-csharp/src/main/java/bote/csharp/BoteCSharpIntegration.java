package bote.csharp;

import bote.BoteIndex;
import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpSettings;
import io.github.thomaslaich.nsmithy.csharp.codegen.GenerationContext;
import io.github.thomaslaich.nsmithy.csharp.codegen.integrations.CSharpIntegration;
import io.github.thomaslaich.nsmithy.csharp.codegen.integrations.CSharpServiceGenerator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import software.amazon.smithy.codegen.core.CodegenException;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.ShapeId;

/**
 * Generates the C# messaging surface of bote services with NSmithy: handlers, publishers, the
 * command client and channel consumers, in place of NSmithy's HTTP client and server.
 */
public final class BoteCSharpIntegration implements CSharpIntegration {

  @Override
  public Set<ShapeId> additionalShapes(Model model, CSharpSettings settings, ServiceShape service) {
    return new LinkedHashSet<>(BoteIndex.channelIds(service));
  }

  @Override
  public Optional<CSharpServiceGenerator> serviceGenerator(
      GenerationContext context, ServiceShape service) {
    return BoteIndex.protocol(service)
        .map(protocol -> (ctx, shape) -> generate(ctx, shape, protocol));
  }

  private static void generate(GenerationContext context, ServiceShape service, ShapeId protocol) {
    Contract contract = Contract.of(context.model(), service);
    if (protocol.equals(BoteIndex.KAFKA_JSON)) {
      new KafkaGenerator(context, contract).run();
      KafkaInfrastructureGenerator infrastructure = new KafkaInfrastructureGenerator(contract);
      if (infrastructure.hasTopics()) {
        String namespace = context.settings().csharpNamespace(service.getId().getNamespace());
        context
            .writerDelegator()
            .useFileWriter(
                namespace.replace('.', '/')
                    + "/"
                    + contract.serviceName()
                    + ".Kafka.Infrastructure.g.cs",
                namespace,
                infrastructure::run);
      }
    } else if (protocol.equals(BoteIndex.REDIS_STREAMS_JSON)) {
      redis(context, contract, RedisGenerator.Kind.STREAMS, RedisGenerator.Encoding.JSON);
    } else if (protocol.equals(BoteIndex.REDIS_STREAMS_CBOR)) {
      redis(context, contract, RedisGenerator.Kind.STREAMS, RedisGenerator.Encoding.CBOR);
    } else if (protocol.equals(BoteIndex.REDIS_PUBSUB_JSON)) {
      redis(context, contract, RedisGenerator.Kind.PUB_SUB, RedisGenerator.Encoding.JSON);
    } else if (protocol.equals(BoteIndex.REDIS_PUBSUB_CBOR)) {
      redis(context, contract, RedisGenerator.Kind.PUB_SUB, RedisGenerator.Encoding.CBOR);
    } else {
      throw new CodegenException(
          "bote C# code generation does not support @"
              + protocol.getName()
              + " yet, on "
              + service.getId()
              + ".");
    }
  }

  private static void redis(
      GenerationContext context,
      Contract contract,
      RedisGenerator.Kind kind,
      RedisGenerator.Encoding encoding) {
    new RedisGenerator(context, contract, kind, encoding).run();
  }
}
