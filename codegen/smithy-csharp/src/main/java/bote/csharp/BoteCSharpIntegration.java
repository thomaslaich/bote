package bote.csharp;

import bote.BoteIndex;
import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpSettings;
import io.github.thomaslaich.nsmithy.csharp.codegen.GenerationContext;
import io.github.thomaslaich.nsmithy.csharp.codegen.integrations.CSharpIntegration;
import io.github.thomaslaich.nsmithy.csharp.codegen.integrations.CSharpServiceGenerator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
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

  private static final Map<ShapeId, BiConsumer<GenerationContext, Contract>> GENERATORS =
      Map.of(
          BoteIndex.KAFKA_JSON, BoteCSharpIntegration::kafka,
          BoteIndex.REDIS_STREAMS_JSON,
              redis(RedisGenerator.Kind.STREAMS, RedisGenerator.Encoding.JSON),
          BoteIndex.REDIS_STREAMS_CBOR,
              redis(RedisGenerator.Kind.STREAMS, RedisGenerator.Encoding.CBOR),
          BoteIndex.REDIS_PUBSUB_JSON,
              redis(RedisGenerator.Kind.PUB_SUB, RedisGenerator.Encoding.JSON),
          BoteIndex.REDIS_PUBSUB_CBOR,
              redis(RedisGenerator.Kind.PUB_SUB, RedisGenerator.Encoding.CBOR));

  private static void generate(GenerationContext context, ServiceShape service, ShapeId protocol) {
    Optional.ofNullable(GENERATORS.get(protocol))
        .orElseThrow(
            () ->
                new CodegenException(
                    "bote C# code generation does not support @"
                        + protocol.getName()
                        + " yet, on "
                        + service.getId()
                        + "."))
        .accept(context, Contract.of(context.model(), service));
  }

  private static void kafka(GenerationContext context, Contract contract) {
    new KafkaGenerator(context, contract).run();
    KafkaInfrastructureGenerator infrastructure = new KafkaInfrastructureGenerator(contract);
    if (infrastructure.hasTopics()) {
      String namespace =
          context.settings().csharpNamespace(contract.service().getId().getNamespace());
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
  }

  private static BiConsumer<GenerationContext, Contract> redis(
      RedisGenerator.Kind kind, RedisGenerator.Encoding encoding) {
    return (context, contract) -> new RedisGenerator(context, contract, kind, encoding).run();
  }
}
