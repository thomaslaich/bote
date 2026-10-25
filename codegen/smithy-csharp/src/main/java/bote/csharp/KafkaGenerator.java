package bote.csharp;

import bote.BoteIndex;
import bote.csharp.Contract.Channel;
import bote.csharp.Contract.Command;
import bote.csharp.Contract.Event;
import io.github.thomaslaich.nsmithy.csharp.codegen.CSharpNaming;
import io.github.thomaslaich.nsmithy.csharp.codegen.GenerationContext;
import io.github.thomaslaich.nsmithy.csharp.codegen.support.ShapeSupport;
import io.github.thomaslaich.nsmithy.csharp.codegen.writer.CSharpWriter;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.smithy.model.shapes.MemberShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeType;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.traits.JsonNameTrait;

/**
 * Generates the C# messaging surface of a {@code @kafkaJson} service. Payloads are JSON; members
 * with {@code @kafkaKey} become the record key and members with {@code @kafkaHeader} become record
 * headers instead of JSON properties.
 */
final class KafkaGenerator extends MessagingGenerator {

  private enum Discrimination {
    ENVELOPE,
    HEADER,
    NONE
  }

  private static final String TYPE_HEADER = "bote-type";
  private static final String INVARIANT = "System.Globalization.CultureInfo.InvariantCulture";

  private final Discrimination discrimination;

  KafkaGenerator(GenerationContext context, Contract contract) {
    super(context, contract);
    this.discrimination =
        BoteIndex.eventDiscrimination(contract.service())
            .map(Discrimination::valueOf)
            .orElse(Discrimination.ENVELOPE);
  }

  @Override
  String runtimeNamespace() {
    return "Bote.Messaging.Kafka";
  }

  @Override
  String commandSendBinding(Command command) {
    return "new(" + facts(command) + ", Encode" + command.name() + ")";
  }

  @Override
  String eventSendBinding(Channel channel, Event event) {
    return "new(" + facts(channel) + ", Encode" + channel.name() + event.name() + ")";
  }

  @Override
  String consumerRegistration(List<String> bindings) {
    return "services.AddKafkaMessageConsumer(" + String.join(", ", bindings) + ")";
  }

  @Override
  String consumerParameters() {
    return "";
  }

  @Override
  String senderType() {
    return "KafkaMessageSender";
  }

  @Override
  void writeCodecs(CSharpWriter writer) {
    writer.addImport("System.Collections.Generic");
    writer.addImport("System.Text");

    Set<StructureShape> payloads = new LinkedHashSet<>();
    contract.commands().forEach(c -> payloads.add(c.input()));
    contract.channels().forEach(ch -> ch.events().forEach(e -> payloads.add(e.shape())));
    for (StructureShape payload : payloads) {
      writeCodecField(writer, payload);
      if (hasHeaders(payload)) {
        writeHeaderDeserializer(writer, payload);
      }
    }

    for (Command command : contract.commands()) {
      writer.write(
          """
          internal static MessagePayload Encode$1L($2L message)
          {
              var value = $3L.Serialize(message);
              ${4L|}
          }

          internal static $2L Decode$1L(MessagePayload payload)
          {
              return $5L;
          }
          """,
          command.name(),
          typeName(command.input()),
          codec(command.input()),
          payloadExpression(command.input(), null),
          deserialize(command.input(), "payload.Value"));
    }

    for (Channel channel : contract.channels()) {
      for (Event event : channel.events()) {
        String value =
            discrimination == Discrimination.ENVELOPE
                ? String.format(
                    "WrapEvent(%s, %s.Serialize(message))",
                    literal(envelopeName(event.member())), codec(event.shape()))
                : codec(event.shape()) + ".Serialize(message)";
        writer.write(
            """
            internal static MessagePayload Encode$1L$2L($3L message)
            {
                var value = $4L;
                ${5L|}
            }
            """,
            channel.name(),
            event.name(),
            typeName(event.shape()),
            value,
            payloadExpression(
                event.shape(),
                discrimination == Discrimination.HEADER ? event.member().getMemberName() : null));
      }
      writeChannelDecode(writer, channel);
    }

    if (discrimination == Discrimination.ENVELOPE && !contract.channels().isEmpty()) {
      writer.write(
          """
          private static byte[] WrapEvent(string type, byte[] payload)
          {
              using var stream = new System.IO.MemoryStream();
              using (var json = new System.Text.Json.Utf8JsonWriter(stream))
              {
                  json.WriteStartObject();
                  json.WritePropertyName(type);
                  json.WriteRawValue(payload);
                  json.WriteEndObject();
              }
              return stream.ToArray();
          }
          """);
    }
  }

  private void writeChannelDecode(CSharpWriter writer, Channel channel) {
    String union = typeName(channel.union());
    StringBuilder body = new StringBuilder();
    switch (discrimination) {
      case ENVELOPE -> {
        body.append(
            """
            using var envelope = System.Text.Json.JsonDocument.Parse(payload.Value);
            if (envelope.RootElement.ValueKind != System.Text.Json.JsonValueKind.Object)
                throw new System.Text.Json.JsonException("Expected an event envelope object.");
            var properties = envelope.RootElement.EnumerateObject();
            if (!properties.MoveNext())
                throw new System.Text.Json.JsonException("Expected one event envelope member.");
            var property = properties.Current;
            if (properties.MoveNext())
                throw new System.Text.Json.JsonException("Expected one event envelope member.");
            var type = property.Name;
            var value = Encoding.UTF8.GetBytes(property.Value.GetRawText());
            """);
        for (Event event : channel.events()) {
          body.append(
              String.format(
                  "if (type == %s)\n    return %s.%s(%s);\n",
                  literal(envelopeName(event.member())),
                  union,
                  CSharpNaming.variantFactoryName(event.member()),
                  deserialize(event.shape(), "value")));
        }
        body.append(
            String.format(
                "return %s.FromUnknown(type,"
                    + " NSmithy.Core.Document.FromJsonElement(property.Value));",
                union));
      }
      case HEADER -> {
        body.append(
            String.format(
                """
                if (payload.Headers is null || !payload.Headers.TryGetValue(%s, out var typeBytes))
                    throw new System.Text.Json.JsonException("Missing %s event header.");
                var type = Encoding.UTF8.GetString(typeBytes);
                """,
                literal(TYPE_HEADER), TYPE_HEADER));
        for (Event event : channel.events()) {
          body.append(
              String.format(
                  "if (type == %s)\n    return %s.%s(%s);\n",
                  literal(event.member().getMemberName()),
                  union,
                  CSharpNaming.variantFactoryName(event.member()),
                  deserialize(event.shape(), "payload.Value")));
        }
        body.append(
            String.format(
                """
                using var unknown = System.Text.Json.JsonDocument.Parse(payload.Value);
                return %s.FromUnknown(type, NSmithy.Core.Document.FromJsonElement(unknown.RootElement));\
                """,
                union));
      }
      case NONE -> {
        Event event = channel.events().get(0);
        body.append(
            String.format(
                "return %s.%s(%s);",
                union,
                CSharpNaming.variantFactoryName(event.member()),
                deserialize(event.shape(), "payload.Value")));
      }
    }
    writer.write(
        """
        internal static $L Decode$L(MessagePayload payload)
        {
            ${L|}
        }
        """,
        union,
        channel.name(),
        body.toString().strip());
  }

  // Codecs. Header-bound members are not part of the JSON value, so a payload with headers is
  // serialized through a projection that leaves them out.

  private void writeCodecField(CSharpWriter writer, StructureShape payload) {
    String type = typeName(payload);
    String schema = schema(writer, payload);
    if (!hasHeaders(payload)) {
      writer.write(
          "private static readonly ICodec<$L> $L = JsonCodecFactory.Default.FromSchema($L.Schema);",
          type,
          codec(payload),
          schema);
      return;
    }
    String excluded =
        headerMembers(payload).stream()
            .map(member -> "member.Name != " + literal(member.getMemberName()))
            .collect(Collectors.joining(" && "));
    writer.write(
        """
        private static readonly StructSchema<$1L, $2L.Builder> $3L = $2L.Schema;
        private static readonly IProjectionCodec<$1L, $2L.Builder> $4L =
            JsonCodecFactory.Default.FromProjection(global::NSmithy.Core.Serde.Schemas.Project($3L, member => $5L));
        """,
        type,
        schema,
        structSchema(payload),
        codec(payload),
        excluded);
  }

  private String deserialize(StructureShape payload, String value) {
    return hasHeaders(payload)
        ? String.format("Deserialize%s(%s, payload.Headers)", shortName(payload), value)
        : String.format("%s.Deserialize(%s)", codec(payload), value);
  }

  private void writeHeaderDeserializer(CSharpWriter writer, StructureShape payload) {
    String headers =
        headerMembers(payload).stream().map(this::decodedHeader).collect(Collectors.joining("\n"));
    writer.write(
        """
        private static $1L Deserialize$2L(byte[] value, IReadOnlyDictionary<string, byte[]>? headers)
        {
            var builder = $3L.CreateTypedBuilder();
            $4L.ReadInto(value, builder);
            ${5L|}
            return $3L.Build(builder);
        }
        """,
        typeName(payload),
        shortName(payload),
        structSchema(payload),
        codec(payload),
        headers);
  }

  private String decodedHeader(MemberShape member) {
    String bytes = CSharpNaming.parameterName(member.getMemberName()) + "Bytes";
    String assignment = headerAssignment(member, bytes);
    String header = literal(headerName(member));
    if (ShapeSupport.isRequired(member)) {
      return String.format(
          """
          if (headers is null || !headers.TryGetValue(%s, out var %s))
              throw new MissingRequiredMemberException(%s);
          %s\
          """,
          header, bytes, literal(member.getMemberName()), assignment);
    }
    return String.format(
        """
        if (headers is { } && headers.TryGetValue(%s, out var %s))
        {
            %s
        }\
        """,
        header, bytes, assignment.replace("\n", "\n    "));
  }

  private String headerAssignment(MemberShape member, String bytes) {
    Shape target = context.model().expectShape(member.getTarget());
    String property = CSharpNaming.propertyName(member.getMemberName());
    if (target.getType() == ShapeType.BLOB) {
      return String.format("builder.%s = %s;", property, bytes);
    }
    String text = CSharpNaming.parameterName(member.getMemberName()) + "Text";
    return String.format(
        "var %s = Encoding.UTF8.GetString(%s);\nbuilder.%s = %s;",
        text, bytes, property, headerParse(target, text));
  }

  // Encoding. Expects the JSON value in `value` and writes the key and headers around it.

  private String payloadExpression(StructureShape payload, String typeHeader) {
    StringBuilder out = new StringBuilder();
    payload.members().stream()
        .filter(m -> m.hasTrait(BoteIndex.KAFKA_KEY))
        .findFirst()
        .ifPresentOrElse(
            key -> out.append("var key = ").append(keyExpression(key)).append(";\n"),
            () -> out.append("string? key = null;\n"));

    List<MemberShape> headers = headerMembers(payload);
    if (headers.isEmpty() && typeHeader == null) {
      out.append("Dictionary<string, byte[]>? headers = null;\n");
    } else {
      out.append("var headers = new Dictionary<string, byte[]>();\n");
      if (typeHeader != null) {
        out.append(
            String.format(
                "headers.Add(%s, Encoding.UTF8.GetBytes(%s));\n",
                literal(TYPE_HEADER), literal(typeHeader)));
      }
      for (MemberShape member : headers) {
        out.append(encodedHeader(member)).append("\n");
      }
    }
    out.append("return new MessagePayload(value, key, headers);");
    return out.toString();
  }

  private String encodedHeader(MemberShape member) {
    String property = CSharpNaming.propertyName(member.getMemberName());
    String local = CSharpNaming.parameterName(member.getMemberName());
    String header = literal(headerName(member));
    if (ShapeSupport.isNullable(member)) {
      return String.format(
          """
          if (message.%s is { } %s)
              headers.Add(%s, %s);\
          """,
          property, local, header, headerBytes(member, local));
    }
    return String.format(
        "headers.Add(%s, %s);", header, headerBytes(member, "message." + property));
  }

  private String keyExpression(MemberShape member) {
    String property = "message." + CSharpNaming.propertyName(member.getMemberName());
    ShapeType type = context.model().expectShape(member.getTarget()).getType();
    boolean nullable = ShapeSupport.isNullable(member);
    return switch (type) {
      case STRING -> property;
      case ENUM -> nullable ? property + "?.Value" : property + ".Value";
      default -> nullable ? property + "?.ToString()" : property + ".ToString()";
    };
  }

  private String headerBytes(MemberShape member, String value) {
    Shape target = context.model().expectShape(member.getTarget());
    if (target.getType() == ShapeType.BLOB) {
      return value;
    }
    return "Encoding.UTF8.GetBytes(" + headerText(target, value) + ")";
  }

  private static String headerText(Shape target, String value) {
    return switch (target.getType()) {
      case STRING -> value;
      case ENUM -> value + ".Value";
      case INT_ENUM -> "((int)" + value + ").ToString(" + INVARIANT + ")";
      case BOOLEAN -> value + " ? \"true\" : \"false\"";
      case FLOAT, DOUBLE -> value + ".ToString(\"R\", " + INVARIANT + ")";
      case TIMESTAMP -> value + ".ToUniversalTime().ToString(\"O\", " + INVARIANT + ")";
      case BYTE, SHORT, INTEGER, LONG, BIG_INTEGER, BIG_DECIMAL ->
          value + ".ToString(null, " + INVARIANT + ")";
      default -> throw unsupportedHeader(target);
    };
  }

  private String headerParse(Shape target, String text) {
    String type = typeName(target);
    return switch (target.getType()) {
      case STRING -> text;
      case ENUM -> "new " + type + "(" + text + ")";
      case INT_ENUM -> "(" + type + ")int.Parse(" + text + ", " + INVARIANT + ")";
      case BOOLEAN -> "bool.Parse(" + text + ")";
      case BYTE -> "sbyte.Parse(" + text + ", " + INVARIANT + ")";
      case SHORT -> "short.Parse(" + text + ", " + INVARIANT + ")";
      case INTEGER -> "int.Parse(" + text + ", " + INVARIANT + ")";
      case LONG -> "long.Parse(" + text + ", " + INVARIANT + ")";
      case FLOAT -> "float.Parse(" + text + ", " + INVARIANT + ")";
      case DOUBLE -> "double.Parse(" + text + ", " + INVARIANT + ")";
      case BIG_INTEGER -> "System.Numerics.BigInteger.Parse(" + text + ", " + INVARIANT + ")";
      case BIG_DECIMAL -> "decimal.Parse(" + text + ", " + INVARIANT + ")";
      case TIMESTAMP ->
          "System.DateTimeOffset.Parse("
              + text
              + ", "
              + INVARIANT
              + ", System.Globalization.DateTimeStyles.RoundtripKind)";
      default -> throw unsupportedHeader(target);
    };
  }

  private static IllegalArgumentException unsupportedHeader(Shape target) {
    return new IllegalArgumentException(
        "@kafkaHeader supports simple types and blobs; "
            + target.getId()
            + " is a "
            + target.getType()
            + ".");
  }

  // Model helpers.

  private static List<MemberShape> headerMembers(StructureShape payload) {
    return payload.members().stream()
        .filter(member -> member.hasTrait(BoteIndex.KAFKA_HEADER))
        .sorted(Comparator.comparing(MemberShape::getMemberName))
        .toList();
  }

  private static boolean hasHeaders(StructureShape payload) {
    return payload.members().stream().anyMatch(m -> m.hasTrait(BoteIndex.KAFKA_HEADER));
  }

  private static String headerName(MemberShape member) {
    return member
        .findTrait(BoteIndex.KAFKA_HEADER)
        .orElseThrow()
        .toNode()
        .expectObjectNode()
        .expectStringMember("name")
        .getValue();
  }

  private static String envelopeName(MemberShape member) {
    return member
        .getTrait(JsonNameTrait.class)
        .map(JsonNameTrait::getValue)
        .orElse(member.getMemberName());
  }

  private static String shortName(Shape shape) {
    return CSharpNaming.typeName(shape.getId().getName());
  }

  private static String structSchema(Shape shape) {
    return shortName(shape) + "StructSchema";
  }
}
