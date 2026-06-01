package bote.asyncapi;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import software.amazon.smithy.jsonschema.JsonSchemaConfig;
import software.amazon.smithy.jsonschema.JsonSchemaConverter;
import software.amazon.smithy.jsonschema.Schema;
import software.amazon.smithy.jsonschema.SchemaDocument;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.neighbor.Walker;
import software.amazon.smithy.model.node.ArrayNode;
import software.amazon.smithy.model.node.Node;
import software.amazon.smithy.model.node.ObjectNode;
import software.amazon.smithy.model.shapes.MemberShape;
import software.amazon.smithy.model.shapes.OperationShape;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.ShapeType;
import software.amazon.smithy.model.shapes.StructureShape;
import software.amazon.smithy.model.traits.DocumentationTrait;
import software.amazon.smithy.model.traits.StreamingTrait;
import software.amazon.smithy.model.traits.TitleTrait;

/**
 * Converts a single bote-annotated Smithy service into an AsyncAPI 3.1.0 document.
 *
 * <p>The mapping mirrors AsyncAPI's send/receive vocabulary, which bote's
 * {@code @send}/{@code @receive} traits were modelled on:
 *
 * <ul>
 *   <li>the Kafka protocol service -&gt; the AsyncAPI document
 *   <li>{@code @kafkaTopic} -&gt; a channel (with Kafka channel binding)
 *   <li>{@code @send}/{@code @receive} -&gt; an operation with action send/receive
 *   <li>each message value structure -&gt; a component message + JSON Schema payload
 *   <li>{@code @kafkaKey} -&gt; the Kafka message binding key
 *   <li>{@code @kafkaHeader} -&gt; the message headers schema
 *   <li>{@code @kafkaTopicConfig} -&gt; Kafka channel binding partitions/replicas/config
 * </ul>
 */
final class AsyncApiConverter {

  private static final String ASYNCAPI_VERSION = "3.1.0";
  private static final String KAFKA_BINDING_VERSION = "0.5.0";
  private static final String SCHEMAS_POINTER = "#/components/schemas";

  private static final ShapeId KAFKA_JSON = ShapeId.from("bote#kafkaJson");
  private static final ShapeId KAFKA_AVRO = ShapeId.from("bote#kafkaAvro");
  private static final ShapeId KAFKA_PROTOBUF = ShapeId.from("bote#kafkaProtobuf");
  private static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");
  private static final ShapeId KAFKA_TOPIC_CONFIG = ShapeId.from("bote#kafkaTopicConfig");
  private static final ShapeId KAFKA_KEY = ShapeId.from("bote#kafkaKey");
  private static final ShapeId KAFKA_HEADER = ShapeId.from("bote#kafkaHeader");
  private static final ShapeId SEND = ShapeId.from("bote#send");
  private static final ShapeId RECEIVE = ShapeId.from("bote#receive");

  private final Model model;
  private final ServiceShape service;
  private final String contentType;

  // Message shapes referenced by this service, in first-seen order.
  private final Set<ShapeId> messageShapes = new LinkedHashSet<>();
  // topic name -> accumulated channel state.
  private final Map<String, Channel> channels = new LinkedHashMap<>();
  // operation name -> operation object.
  private final Map<String, Node> operations = new LinkedHashMap<>();

  AsyncApiConverter(Model model, ServiceShape service) {
    this.model = model;
    this.service = service;
    this.contentType = contentTypeFor(service);
  }

  /** Returns true if the service carries a bote Kafka protocol trait. */
  static boolean isKafkaService(ServiceShape service) {
    return service.hasTrait(KAFKA_JSON)
        || service.hasTrait(KAFKA_AVRO)
        || service.hasTrait(KAFKA_PROTOBUF);
  }

  private static String contentTypeFor(ServiceShape service) {
    if (service.hasTrait(KAFKA_AVRO)) {
      return "application/avro";
    }
    if (service.hasTrait(KAFKA_PROTOBUF)) {
      return "application/protobuf";
    }
    return "application/json";
  }

  ObjectNode convert() {
    for (ShapeId operationId : service.getAllOperations()) {
      OperationShape operation = model.expectShape(operationId, OperationShape.class);
      if (operation.hasTrait(SEND)) {
        addOperation(operation, "send", set(operation.getInputShape()));
      } else if (operation.hasTrait(RECEIVE)) {
        addOperation(operation, "receive", receivedMessages(operation));
      }
    }

    return Node.objectNodeBuilder()
        .withMember("asyncapi", ASYNCAPI_VERSION)
        .withMember("info", buildInfo())
        .withMember("defaultContentType", contentType)
        .withMember("channels", buildChannels())
        .withMember("operations", buildOperations())
        .withMember("components", buildComponents())
        .build();
  }

  // -- operations & channels ---------------------------------------------

  private void addOperation(OperationShape operation, String action, Set<ShapeId> messages) {
    String topic = topicName(operation);
    Channel channel = channels.computeIfAbsent(topic, Channel::new);
    channel.observe(operation);
    for (ShapeId messageId : messages) {
      messageShapes.add(messageId);
      channel.messages.add(messageId);
    }
    operations.put(operation.getId().getName(), operationNode(operation, action, topic, messages));
  }

  /**
   * A {@code @receive} operation's output contains a member targeting a {@code @streaming} union;
   * each union member is a possible message type.
   */
  private Set<ShapeId> receivedMessages(OperationShape operation) {
    Set<ShapeId> result = new LinkedHashSet<>();
    operation
        .getOutput()
        .flatMap(id -> model.getShape(id).flatMap(Shape::asStructureShape))
        .ifPresent(
            output -> {
              for (MemberShape member : output.getAllMembers().values()) {
                model
                    .getShape(member.getTarget())
                    .flatMap(Shape::asUnionShape)
                    .filter(union -> union.hasTrait(StreamingTrait.ID))
                    .ifPresent(
                        union -> {
                          for (MemberShape um : union.getAllMembers().values()) {
                            result.add(um.getTarget());
                          }
                        });
              }
            });
    return result;
  }

  private Node operationNode(
      OperationShape operation, String action, String topic, Set<ShapeId> messages) {
    ArrayNode.Builder refs = ArrayNode.builder();
    for (ShapeId messageId : messages) {
      refs.withValue(ref("#/channels/" + topic + "/messages/" + messageId.getName()));
    }
    ObjectNode.Builder builder =
        Node.objectNodeBuilder()
            .withMember("action", action)
            .withMember("channel", ref("#/channels/" + topic))
            .withMember("messages", refs.build());
    documentation(operation).ifPresent(doc -> builder.withMember("summary", doc));
    return builder.build();
  }

  private Node buildChannels() {
    ObjectNode.Builder builder = Node.objectNodeBuilder();
    for (Channel channel : channels.values()) {
      ObjectNode.Builder messages = Node.objectNodeBuilder();
      for (ShapeId messageId : channel.messages) {
        messages.withMember(
            messageId.getName(), ref("#/components/messages/" + messageId.getName()));
      }
      builder.withMember(
          channel.topic,
          Node.objectNodeBuilder()
              .withMember("address", channel.topic)
              .withMember("messages", messages.build())
              .withMember("bindings", channelBindings(channel))
              .build());
    }
    return builder.build();
  }

  private Node channelBindings(Channel channel) {
    ObjectNode.Builder kafka =
        Node.objectNodeBuilder()
            .withMember("topic", channel.topic)
            .withMember("bindingVersion", KAFKA_BINDING_VERSION);

    ObjectNode.Builder topicConfig = Node.objectNodeBuilder();
    boolean hasTopicConfig = false;

    if (channel.topicConfig != null) {
      ObjectNode c = channel.topicConfig;
      copyNumber(c, "partitions", kafka, "partitions");
      copyNumber(c, "replicationFactor", kafka, "replicas");
      hasTopicConfig |= copyNumber(c, "retentionMs", topicConfig, "retention.ms");
      hasTopicConfig |= copyNumber(c, "retentionBytes", topicConfig, "retention.bytes");
      hasTopicConfig |= copyNumber(c, "minInsyncReplicas", topicConfig, "min.insync.replicas");
      hasTopicConfig |= copyNumber(c, "maxMessageBytes", topicConfig, "max.message.bytes");
    }

    if (channel.compacted) {
      topicConfig.withMember("cleanup.policy", ArrayNode.builder().withValue("compact").build());
      hasTopicConfig = true;
    }

    if (hasTopicConfig) {
      kafka.withMember("topicConfiguration", topicConfig.build());
    }

    return Node.objectNodeBuilder().withMember("kafka", kafka.build()).build();
  }

  /** Copies a numeric member from {@code source} to {@code target}; returns true if present. */
  private boolean copyNumber(ObjectNode source, String from, ObjectNode.Builder target, String to) {
    Optional<Node> value = source.getMember(from);
    if (value.isPresent() && value.get().isNumberNode()) {
      target.withMember(to, value.get());
      return true;
    }
    return false;
  }

  // -- components ---------------------------------------------------------

  private Node buildComponents() {
    ObjectNode.Builder messages = Node.objectNodeBuilder();
    for (ShapeId messageId : messageShapes) {
      messages.withMember(messageId.getName(), buildMessage(messageId));
    }
    return Node.objectNodeBuilder()
        .withMember("messages", messages.build())
        .withMember("schemas", buildSchemas())
        .build();
  }

  private Node buildMessage(ShapeId messageId) {
    StructureShape structure = model.expectShape(messageId, StructureShape.class);
    String name = messageId.getName();

    ObjectNode.Builder message =
        Node.objectNodeBuilder()
            .withMember("name", name)
            .withMember("title", name)
            .withMember("contentType", contentType)
            .withMember("payload", ref(SCHEMAS_POINTER + "/" + name));

    documentation(structure).ifPresent(doc -> message.withMember("summary", doc));

    keyMember(structure)
        .ifPresent(
            key ->
                message.withMember(
                    "bindings",
                    Node.objectNodeBuilder()
                        .withMember(
                            "kafka",
                            Node.objectNodeBuilder()
                                .withMember("key", jsonTypeOf(key))
                                .withMember("bindingVersion", KAFKA_BINDING_VERSION)
                                .build())
                        .build()));

    Node headers = buildHeaders(structure);
    if (headers != null) {
      message.withMember("headers", headers);
    }

    return message.build();
  }

  private Optional<MemberShape> keyMember(StructureShape structure) {
    return structure.getAllMembers().values().stream()
        .filter(m -> m.hasTrait(KAFKA_KEY))
        .findFirst();
  }

  private Node buildHeaders(StructureShape structure) {
    ObjectNode.Builder properties = Node.objectNodeBuilder();
    boolean any = false;
    for (MemberShape member : structure.getAllMembers().values()) {
      Optional<String> headerName =
          member
              .findTrait(KAFKA_HEADER)
              .map(t -> t.toNode().expectObjectNode().expectStringMember("name").getValue());
      if (headerName.isPresent()) {
        properties.withMember(headerName.get(), jsonTypeOf(member));
        any = true;
      }
    }
    if (!any) {
      return null;
    }
    return Node.objectNodeBuilder()
        .withMember("type", "object")
        .withMember("properties", properties.build())
        .build();
  }

  /** Generates the JSON Schema definitions for every referenced message shape and its closure. */
  private Node buildSchemas() {
    Set<ShapeId> closure = new LinkedHashSet<>();
    Walker walker = new Walker(model);
    for (ShapeId messageId : messageShapes) {
      model
          .getShape(messageId)
          .ifPresent(shape -> walker.walkShapes(shape).forEach(s -> closure.add(s.getId())));
    }

    JsonSchemaConfig config = new JsonSchemaConfig();
    config.setDefinitionPointer(SCHEMAS_POINTER);
    config.setUseIntegerType(true);
    SchemaDocument document =
        JsonSchemaConverter.builder()
            .model(model)
            .config(config)
            .shapePredicate(shape -> closure.contains(shape.getId()))
            .build()
            .convert();

    ObjectNode.Builder schemas = Node.objectNodeBuilder();
    for (Map.Entry<String, Schema> entry : document.getDefinitions().entrySet()) {
      String name = entry.getKey().substring(SCHEMAS_POINTER.length() + 1);
      schemas.withMember(name, entry.getValue().toNode());
    }
    return schemas.build();
  }

  // -- info ---------------------------------------------------------------

  private Node buildInfo() {
    String version = service.getVersion().isEmpty() ? "1.0.0" : service.getVersion();
    String title =
        service
            .getTrait(TitleTrait.class)
            .map(TitleTrait::getValue)
            .orElseGet(() -> service.getId().getName());
    ObjectNode.Builder info =
        Node.objectNodeBuilder().withMember("title", title).withMember("version", version);
    documentation(service).ifPresent(doc -> info.withMember("description", doc));
    return info.build();
  }

  private Node buildOperations() {
    ObjectNode.Builder builder = Node.objectNodeBuilder();
    for (Map.Entry<String, Node> entry : operations.entrySet()) {
      builder.withMember(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  // -- helpers ------------------------------------------------------------

  private String topicName(OperationShape operation) {
    return operation
        .findTrait(KAFKA_TOPIC)
        .map(t -> t.toNode().expectObjectNode().expectStringMember("name").getValue())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Operation " + operation.getId() + " is missing @kafkaTopic"));
  }

  private Optional<String> documentation(Shape shape) {
    return shape.getTrait(DocumentationTrait.class).map(DocumentationTrait::getValue);
  }

  /** Minimal JSON Schema type node for a Kafka key or header member. */
  private Node jsonTypeOf(MemberShape member) {
    ShapeType type = model.expectShape(member.getTarget()).getType();
    String jsonType;
    switch (type) {
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
      case BIG_INTEGER:
        jsonType = "integer";
        break;
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        jsonType = "number";
        break;
      case BOOLEAN:
        jsonType = "boolean";
        break;
      default:
        jsonType = "string";
        break;
    }
    return Node.objectNodeBuilder().withMember("type", jsonType).build();
  }

  private static Node ref(String pointer) {
    return Node.objectNodeBuilder().withMember("$ref", pointer).build();
  }

  private static Set<ShapeId> set(ShapeId id) {
    Set<ShapeId> single = new LinkedHashSet<>();
    single.add(id);
    return single;
  }

  /** Accumulates the state of one Kafka topic across the operations bound to it. */
  private final class Channel {
    private final String topic;
    private final Set<ShapeId> messages = new LinkedHashSet<>();
    private ObjectNode topicConfig;
    private boolean compacted;

    Channel(String topic) {
      this.topic = topic;
    }

    /** Folds an operation's @kafkaTopic/@kafkaTopicConfig into this channel. */
    void observe(OperationShape operation) {
      operation
          .findTrait(KAFKA_TOPIC)
          .map(t -> t.toNode().expectObjectNode())
          .flatMap(n -> n.getBooleanMember("compacted"))
          .ifPresent(b -> compacted |= b.getValue());

      if (topicConfig == null) {
        operation
            .findTrait(KAFKA_TOPIC_CONFIG)
            .map(t -> t.toNode().expectObjectNode())
            .ifPresent(c -> topicConfig = c);
      }
    }
  }
}
