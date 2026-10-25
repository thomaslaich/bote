package bote.csharp;

import bote.BoteIndex.Address;
import bote.csharp.Contract.Channel;
import bote.csharp.Contract.Command;
import bote.csharp.Contract.Event;
import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpNaming;
import io.github.thomaslaich.nsmithy.csharp.codegen.GenerationContext;
import io.github.thomaslaich.nsmithy.csharp.codegen.writer.CSharpWriter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import software.amazon.smithy.model.shapes.Shape;

/**
 * Generates the C# messaging surface of a Redis Streams or Redis Pub/Sub service, JSON or CBOR. A
 * channel's events are serialized as its union, whose JSON and CBOR forms are the single-key
 * envelope.
 */
final class RedisGenerator extends MessagingGenerator {

  enum Kind {
    STREAMS,
    PUB_SUB
  }

  /** The value encoding, after Smithy's own JSON and CBOR mappings. */
  enum Encoding {
    JSON("NSmithy.Codecs.Json", "JsonCodecFactory", "application/json"),
    CBOR("NSmithy.Codecs.Cbor", "CborCodecFactory", "application/cbor");

    private final String codecNamespace;
    private final String codecFactory;
    private final String contentType;

    Encoding(String codecNamespace, String codecFactory, String contentType) {
      this.codecNamespace = codecNamespace;
      this.codecFactory = codecFactory;
      this.contentType = contentType;
    }
  }

  private final Kind kind;
  private final Encoding encoding;
  private final String contentType;

  RedisGenerator(GenerationContext context, Contract contract, Kind kind, Encoding encoding) {
    super(context, contract);
    this.kind = kind;
    this.encoding = encoding;
    this.contentType = literal(encoding.contentType);
  }

  @Override
  String codecNamespace() {
    return encoding.codecNamespace;
  }

  @Override
  String runtimeNamespace() {
    return "Bote.Redis";
  }

  @Override
  String outboundOptions(Address address) {
    return kind == Kind.STREAMS ? ", maxLength: " + maxLength(address) : "";
  }

  @Override
  String consumerRegistration(List<String> inbounds) {
    return "services.Add" + transport() + "Consumer(options, " + String.join(", ", inbounds) + ")";
  }

  @Override
  String senderType() {
    return kind == Kind.STREAMS ? "RedisStreamsSender" : "RedisPubSubSender";
  }

  @Override
  String consumerParameters() {
    return transport() + "ConsumerOptions? options = null";
  }

  @Override
  void writeCodecs(CSharpWriter writer) {
    Set<Shape> payloads = new LinkedHashSet<>();
    for (Command command : contract.commands()) {
      payloads.add(command.input());
      command.output().ifPresent(payloads::add);
    }
    contract.channels().forEach(channel -> payloads.add(channel.union()));
    for (Shape payload : payloads) {
      writer.write(
          "private static readonly ICodec<$L> $L = $L.Default.FromSchema($L.Schema);",
          typeName(payload),
          codec(payload),
          encoding.codecFactory,
          schema(writer, payload));
    }

    for (Command command : contract.commands()) {
      writer.write(
          """
          internal static MessagePayload Encode$1L($2L message) =>
              new(
                  $3L.Serialize(message),
                  Headers: new System.Collections.Generic.Dictionary<string, byte[]>
                  {
                      [InboundBinding.TypeHeader] = $4Lu8.ToArray(),
                  },
                  ContentType: $5L
              );

          internal static $2L Decode$1L(MessagePayload payload) => $3L.Deserialize(payload.Value);
          """,
          command.name(),
          typeName(command.input()),
          codec(command.input()),
          literal(command.type()),
          contentType);
      command
          .output()
          .ifPresent(output -> writeCodecMethods(writer, command.name() + "Reply", output));
    }
    for (Channel channel : contract.channels()) {
      String union = typeName(channel.union());
      for (Event event : channel.events()) {
        writer.write(
            """
            internal static MessagePayload Encode$L$L($L message) =>
                new($L.Serialize($L.$L(message)), ContentType: $L);
            """,
            channel.name(),
            event.name(),
            typeName(event.shape()),
            codec(channel.union()),
            union,
            CSharpNaming.variantFactoryName(event.member()),
            contentType);
      }
      writer.write(
          "internal static $L Decode$L(MessagePayload payload) => $L.Deserialize(payload.Value);",
          union,
          channel.name(),
          codec(channel.union()));
    }
  }

  private void writeCodecMethods(CSharpWriter writer, String name, Shape payload) {
    writer.write(
        """
        internal static MessagePayload Encode$1L($2L message) =>
            new($3L.Serialize(message), ContentType: $4L);

        internal static $2L Decode$1L(MessagePayload payload) => $3L.Deserialize(payload.Value);
        """,
        name,
        typeName(payload),
        codec(payload),
        contentType);
  }

  private String transport() {
    return kind == Kind.STREAMS ? "RedisStream" : "RedisPubSub";
  }

  private static String maxLength(Address address) {
    return address
        .value()
        .getNumberMember("maxLen")
        .map(n -> n.getValue().longValue() + "L")
        .orElse("null");
  }
}
