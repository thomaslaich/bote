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
 * Generates the C# messaging surface of a {@code @redisStreamsJson} or {@code @redisPubSubJson}
 * service. Values are JSON; a channel's events are serialized as its union, whose JSON form is the
 * single-key envelope.
 */
final class RedisGenerator extends MessagingGenerator {

  enum Kind {
    STREAMS,
    PUB_SUB
  }

  private static final String CONTENT_TYPE = "\"application/json\"";

  private final Kind kind;

  RedisGenerator(GenerationContext context, Contract contract, Kind kind) {
    super(context, contract);
    this.kind = kind;
  }

  @Override
  String runtimeNamespace() {
    return "Bote.Messaging.Redis";
  }

  @Override
  String commandSendBindingType(Command command) {
    if (kind == Kind.PUB_SUB) {
      return super.commandSendBindingType(command);
    }
    return command
        .output()
        .map(
            o ->
                "RedisStreamRequestBinding<" + typeName(command.input()) + ", " + typeName(o) + ">")
        .orElse("RedisStreamSendBinding<" + typeName(command.input()) + ">");
  }

  @Override
  String commandSendBinding(Command command) {
    String reply = command.output().map(o -> ", Decode" + command.name() + "Reply").orElse("");
    return "new(" + facts(command) + ", Encode" + command.name() + reply + maxLength(command) + ")";
  }

  @Override
  String eventSendBinding(Channel channel, Event event) {
    return "new("
        + facts(channel)
        + ", Encode"
        + channel.name()
        + event.name()
        + maxLength(channel)
        + ")";
  }

  @Override
  String eventSendBindingType(Event event) {
    String type = typeName(event.shape());
    return kind == Kind.STREAMS
        ? "RedisStreamSendBinding<" + type + ">"
        : "MessageSendBinding<" + type + ">";
  }

  @Override
  String consumerRegistration(List<String> bindings) {
    return "services.Add" + transport() + "Consumer(options, " + String.join(", ", bindings) + ")";
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
          "private static readonly ICodec<$L> $L = JsonCodecFactory.Default.FromSchema($L.Schema);",
          typeName(payload),
          codec(payload),
          schema(writer, payload));
    }

    for (Command command : contract.commands()) {
      writeCodecMethods(writer, command.name(), command.input());
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
            CONTENT_TYPE);
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
        CONTENT_TYPE);
  }

  private String transport() {
    return kind == Kind.STREAMS ? "RedisStream" : "RedisPubSub";
  }

  private String maxLength(Command command) {
    return kind == Kind.STREAMS ? ", " + maxLength(command.address()) : "";
  }

  private String maxLength(Channel channel) {
    return kind == Kind.STREAMS ? ", " + maxLength(channel.address()) : "";
  }

  private static String maxLength(Address address) {
    return address
        .value()
        .getNumberMember("maxLen")
        .map(n -> n.getValue().longValue() + "L")
        .orElse("null");
  }
}
