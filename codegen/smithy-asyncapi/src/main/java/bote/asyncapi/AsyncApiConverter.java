package bote.asyncapi;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
 * Converts one or more bote-annotated Smithy services into an AsyncAPI 3.1.0 document.
 *
 * <p>AsyncAPI 3 actions describe the application the document is about, so the mapping depends on
 * the chosen {@link Perspective}. With the default OWNER perspective the document describes the
 * contract owner: produce-side operations ({@code @kafkaProduce}, {@code @redisStreamAdd},
 * {@code @redisPublish} — the owner accepts commands) become {@code action: receive}, and
 * consume-side operations ({@code @kafkaConsume}, {@code @redisStreamRead},
 * {@code @redisSubscribe} — the owner emits events) become {@code action: send}. The CLIENT
 * perspective flips both.
 *
 * <ul>
 *   <li>bote protocol service(s) -&gt; the AsyncAPI document
 *   <li>the broker operation trait's address (topic / stream / channel) -&gt; a channel
 *   <li>a broker operation trait -&gt; an operation (action per perspective)
 *   <li>each message value structure -&gt; a component message + JSON Schema payload
 *   <li>event messages -&gt; payload/headers per the protocol's event discrimination (ENVELOPE
 *       wraps the payload in a single-key object; HEADER adds a "bote-type" header)
 *   <li>{@code @kafkaKey} -&gt; the Kafka message binding key
 *   <li>{@code @kafkaHeader} -&gt; the message headers schema; header members are stripped from
 *       payload schemas because they travel only as Kafka headers
 *   <li>{@code @bote.infra#kafkaTopicConfig} -&gt; Kafka channel binding partitions/replicas/config
 * </ul>
 */
final class AsyncApiConverter {

  private static final String ASYNCAPI_VERSION = "3.1.0";
  private static final String KAFKA_BINDING_VERSION = "0.5.0";
  private static final String SCHEMAS_POINTER = "#/components/schemas";
  private static final String TYPE_HEADER_NAME = "bote-type";

  private static final ShapeId KAFKA_JSON = ShapeId.from("bote#kafkaJson");
  private static final ShapeId KAFKA_AVRO = ShapeId.from("bote#kafkaAvro");
  private static final ShapeId KAFKA_PROTOBUF = ShapeId.from("bote#kafkaProtobuf");
  private static final ShapeId KAFKA_TOPIC_CONFIG = ShapeId.from("bote.infra#kafkaTopicConfig");
  private static final ShapeId KAFKA_KEY = ShapeId.from("bote#kafkaKey");
  private static final ShapeId KAFKA_HEADER = ShapeId.from("bote#kafkaHeader");
  private static final ShapeId REDIS_STREAMS_JSON = ShapeId.from("bote#redisStreamsJson");
  private static final ShapeId REDIS_PUBSUB_JSON = ShapeId.from("bote#redisPubSubJson");
  private static final ShapeId KAFKA_PRODUCE = ShapeId.from("bote#kafkaProduce");
  private static final ShapeId KAFKA_CONSUME = ShapeId.from("bote#kafkaConsume");
  private static final ShapeId REDIS_STREAM_ADD = ShapeId.from("bote#redisStreamAdd");
  private static final ShapeId REDIS_STREAM_READ = ShapeId.from("bote#redisStreamRead");
  private static final ShapeId REDIS_PUBLISH = ShapeId.from("bote#redisPublish");
  private static final ShapeId REDIS_SUBSCRIBE = ShapeId.from("bote#redisSubscribe");

  private static final List<ShapeId> PRODUCE_TRAITS =
      List.of(KAFKA_PRODUCE, REDIS_STREAM_ADD, REDIS_PUBLISH);
  private static final List<ShapeId> CONSUME_TRAITS =
      List.of(KAFKA_CONSUME, REDIS_STREAM_READ, REDIS_SUBSCRIBE);

  /** Broker operation trait -> the member of the trait carrying the channel address. */
  private static final Map<ShapeId, String> ADDRESS_MEMBERS =
      Map.of(
          KAFKA_PRODUCE, "topic",
          KAFKA_CONSUME, "topic",
          REDIS_STREAM_ADD, "stream",
          REDIS_STREAM_READ, "stream",
          REDIS_PUBLISH, "channel",
          REDIS_SUBSCRIBE, "channel");

  /** Whose viewpoint the generated document takes. */
  enum Perspective {
    /** The document describes the contract owner: it receives commands and sends events. */
    OWNER,
    /** The document describes a client: it sends commands and receives events. */
    CLIENT
  }

  /** How a protocol identifies event types on a multi-event channel. */
  private enum Discrimination {
    ENVELOPE,
    HEADER,
    NONE
  }

  private final Model model;
  private final List<ServiceShape> services;
  private final Optional<String> title;
  private final Perspective perspective;
  private final String defaultContentType;

  // Message shapes referenced by this service, in first-seen order.
  private final Set<ShapeId> messageShapes = new LinkedHashSet<>();
  // Message shape -> content type inherited from the first service that references it.
  private final Map<ShapeId, String> messageContentTypes = new LinkedHashMap<>();
  // Event shape -> envelope key (streaming union member name), for ENVELOPE discrimination.
  private final Map<ShapeId, String> envelopeNames = new LinkedHashMap<>();
  // Event shape -> type header value (streaming union member name), for HEADER discrimination.
  private final Map<ShapeId, String> typeHeaderNames = new LinkedHashMap<>();
  // topic name -> accumulated channel state.
  private final Map<String, Channel> channels = new LinkedHashMap<>();
  // operation name -> operation object.
  private final Map<String, Node> operations = new LinkedHashMap<>();

  AsyncApiConverter(Model model, ServiceShape service) {
    this(model, List.of(service), Optional.empty(), Perspective.OWNER);
  }

  AsyncApiConverter(
      Model model, List<ServiceShape> services, Optional<String> title, Perspective perspective) {
    this.model = model;
    this.services = List.copyOf(services);
    this.title = title;
    this.perspective = perspective;
    this.defaultContentType = defaultContentTypeFor(services);
  }

  /** Returns true if the service carries any bote messaging protocol trait. */
  static boolean isBoteService(ServiceShape service) {
    return isKafkaService(service) || isRedisService(service);
  }

  private static boolean isRedisService(ServiceShape service) {
    return service.hasTrait(REDIS_STREAMS_JSON) || service.hasTrait(REDIS_PUBSUB_JSON);
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

  private static String defaultContentTypeFor(List<ServiceShape> services) {
    return services.stream()
        .map(AsyncApiConverter::contentTypeFor)
        .distinct()
        .findFirst()
        .orElse("application/json");
  }

  /** The event discrimination mode of a service's protocol. */
  private static Discrimination discriminationFor(ServiceShape service) {
    if (service.hasTrait(KAFKA_JSON)) {
      return service
          .findTrait(KAFKA_JSON)
          .map(t -> t.toNode().expectObjectNode())
          .flatMap(n -> n.getStringMember("eventDiscrimination"))
          .map(s -> Discrimination.valueOf(s.getValue()))
          .orElse(Discrimination.ENVELOPE);
    }
    if (isRedisService(service)) {
      return Discrimination.ENVELOPE;
    }
    // Avro / Protobuf: the wire format (schema ID, proto descriptor) types the message.
    return Discrimination.NONE;
  }

  ObjectNode convert() {
    for (ServiceShape service : services) {
      String contentType = contentTypeFor(service);
      Discrimination discrimination = discriminationFor(service);
      for (ShapeId operationId : service.getAllOperations()) {
        OperationShape operation = model.expectShape(operationId, OperationShape.class);
        if (PRODUCE_TRAITS.stream().anyMatch(operation::hasTrait)) {
          String action = perspective == Perspective.OWNER ? "receive" : "send";
          Map<ShapeId, String> messages = new LinkedHashMap<>();
          messages.put(operation.getInputShape(), null);
          addOperation(operation, action, messages, contentType, discrimination);
        } else if (CONSUME_TRAITS.stream().anyMatch(operation::hasTrait)) {
          String action = perspective == Perspective.OWNER ? "send" : "receive";
          addOperation(operation, action, receivedMessages(operation), contentType, discrimination);
        }
      }
    }

    return Node.objectNodeBuilder()
        .withMember("asyncapi", ASYNCAPI_VERSION)
        .withMember("info", buildInfo())
        .withMember("defaultContentType", defaultContentType)
        .withMember("channels", buildChannels())
        .withMember("operations", buildOperations())
        .withMember("components", buildComponents())
        .build();
  }

  // -- operations & channels ---------------------------------------------

  private void addOperation(
      OperationShape operation,
      String action,
      Map<ShapeId, String> messages,
      String contentType,
      Discrimination discrimination) {
    String address = channelAddress(operation);
    Channel channel =
        channels.computeIfAbsent(address, a -> new Channel(a, channelBindings(a, operation)));
    for (Map.Entry<ShapeId, String> message : messages.entrySet()) {
      ShapeId messageId = message.getKey();
      String memberName = message.getValue();
      messageShapes.add(messageId);
      messageContentTypes.putIfAbsent(messageId, contentType);
      if (memberName != null) {
        if (discrimination == Discrimination.ENVELOPE) {
          envelopeNames.putIfAbsent(messageId, memberName);
        } else if (discrimination == Discrimination.HEADER) {
          typeHeaderNames.putIfAbsent(messageId, memberName);
        }
      }
      channel.messages.add(messageId);
    }
    operations.put(
        operationName(operation), operationNode(operation, action, address, messages.keySet()));
  }

  /**
   * A consume operation's output contains a member targeting a {@code @streaming} union; each
   * union member is a possible message type, keyed here by its member name (the wire-level tag).
   */
  private Map<ShapeId, String> receivedMessages(OperationShape operation) {
    Map<ShapeId, String> result = new LinkedHashMap<>();
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
                            result.putIfAbsent(um.getTarget(), um.getMemberName());
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
      ObjectNode.Builder channelNode =
          Node.objectNodeBuilder()
              .withMember("address", channel.topic)
              .withMember("messages", messages.build());
      if (channel.bindings != null) {
        channelNode.withMember("bindings", channel.bindings);
      }
      builder.withMember(channel.topic, channelNode.build());
    }
    return builder.build();
  }

  /**
   * The Kafka channel binding, derived once from the operation's Kafka trait and
   * {@code @kafkaTopicConfig}.
   */
  private Node kafkaChannelBindings(String topic, Shape topicShape) {
    ObjectNode.Builder kafka =
        Node.objectNodeBuilder()
            .withMember("topic", topic)
            .withMember("bindingVersion", KAFKA_BINDING_VERSION);

    ObjectNode.Builder topicConfig = Node.objectNodeBuilder();
    boolean hasTopicConfig = false;

    Optional<ObjectNode> config =
        topicShape.findTrait(KAFKA_TOPIC_CONFIG).map(t -> t.toNode().expectObjectNode());
    if (config.isPresent()) {
      ObjectNode c = config.get();
      copyNumber(c, "partitions", kafka, "partitions");
      copyNumber(c, "replicationFactor", kafka, "replicas");
      hasTopicConfig |= copyNumber(c, "retentionMs", topicConfig, "retention.ms");
      hasTopicConfig |= copyNumber(c, "retentionBytes", topicConfig, "retention.bytes");
      hasTopicConfig |= copyNumber(c, "minInsyncReplicas", topicConfig, "min.insync.replicas");
      hasTopicConfig |= copyNumber(c, "maxMessageBytes", topicConfig, "max.message.bytes");
    }

    boolean compacted =
        kafkaOperationTrait(topicShape)
            .flatMap(n -> n.getBooleanMember("compacted"))
            .map(b -> b.getValue())
            .orElse(false);
    if (compacted) {
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
            .withMember(
                "contentType", messageContentTypes.getOrDefault(messageId, defaultContentType))
            .withMember("payload", buildPayload(messageId));

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

    Node headers = buildHeaders(messageId, structure);
    if (headers != null) {
      message.withMember("headers", headers);
    }

    return message.build();
  }

  /**
   * The message payload schema: a bare schema reference, or — for ENVELOPE-discriminated events —
   * the single-key wrapper object keyed by the streaming union member name.
   */
  private Node buildPayload(ShapeId messageId) {
    Node bare = ref(SCHEMAS_POINTER + "/" + messageId.getName());
    String envelopeKey = envelopeNames.get(messageId);
    if (envelopeKey == null) {
      return bare;
    }
    return Node.objectNodeBuilder()
        .withMember("type", "object")
        .withMember("properties", Node.objectNodeBuilder().withMember(envelopeKey, bare).build())
        .withMember("required", ArrayNode.builder().withValue(envelopeKey).build())
        .build();
  }

  private Optional<MemberShape> keyMember(StructureShape structure) {
    return structure.getAllMembers().values().stream()
        .filter(m -> m.hasTrait(KAFKA_KEY))
        .findFirst();
  }

  private Node buildHeaders(ShapeId messageId, StructureShape structure) {
    ObjectNode.Builder properties = Node.objectNodeBuilder();
    boolean any = false;

    String typeHeader = typeHeaderNames.get(messageId);
    if (typeHeader != null) {
      properties.withMember(
          TYPE_HEADER_NAME,
          Node.objectNodeBuilder()
              .withMember("type", "string")
              .withMember("const", typeHeader)
              .build());
      any = true;
    }

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
      schemas.withMember(name, stripHeaderMembers(name, entry.getValue().toNode(), closure));
    }
    return schemas.build();
  }

  /**
   * Removes {@code @kafkaHeader}-bound properties from a structure's payload schema: header
   * members travel only as Kafka headers, never in the serialized value.
   */
  private Node stripHeaderMembers(String schemaName, Node schemaNode, Set<ShapeId> closure) {
    Optional<StructureShape> structure =
        closure.stream()
            .filter(id -> id.getName().equals(schemaName))
            .findFirst()
            .flatMap(model::getShape)
            .flatMap(Shape::asStructureShape);
    if (structure.isEmpty() || !schemaNode.isObjectNode()) {
      return schemaNode;
    }

    Set<String> headerMembers = new LinkedHashSet<>();
    for (MemberShape member : structure.get().getAllMembers().values()) {
      if (member.hasTrait(KAFKA_HEADER)) {
        headerMembers.add(member.getMemberName());
      }
    }
    if (headerMembers.isEmpty()) {
      return schemaNode;
    }

    ObjectNode schema = schemaNode.expectObjectNode();
    Optional<ObjectNode> properties = schema.getObjectMember("properties");
    if (properties.isPresent()) {
      ObjectNode updated = properties.get();
      for (String headerMember : headerMembers) {
        updated = updated.withoutMember(headerMember);
      }
      schema = schema.withMember("properties", updated);
    }
    Optional<Node> required = schema.getMember("required");
    if (required.isPresent() && required.get().isArrayNode()) {
      ArrayNode.Builder remaining = ArrayNode.builder();
      for (Node element : required.get().expectArrayNode().getElements()) {
        if (!headerMembers.contains(element.expectStringNode().getValue())) {
          remaining.withValue(element);
        }
      }
      ArrayNode remainingNode = remaining.build();
      schema =
          remainingNode.isEmpty()
              ? schema.withoutMember("required")
              : schema.withMember("required", remainingNode);
    }
    return schema;
  }

  // -- info ---------------------------------------------------------------

  private Node buildInfo() {
    ServiceShape first = services.get(0);
    String version = first.getVersion().isEmpty() ? "1.0.0" : first.getVersion();
    String title =
        this.title.orElseGet(
            () ->
                first
                    .getTrait(TitleTrait.class)
                    .map(TitleTrait::getValue)
                    .orElseGet(() -> first.getId().getName()));
    ObjectNode.Builder info =
        Node.objectNodeBuilder().withMember("title", title).withMember("version", version);
    if (services.size() == 1) {
      documentation(first).ifPresent(doc -> info.withMember("description", doc));
    }
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

  private String operationName(OperationShape operation) {
    String name = operation.getId().getName();
    if (!operations.containsKey(name)) {
      return name;
    }
    String qualified = operation.getId().getNamespace().replace('.', '_') + "_" + name;
    if (!operations.containsKey(qualified)) {
      return qualified;
    }
    int suffix = 2;
    while (operations.containsKey(qualified + suffix)) {
      suffix++;
    }
    return qualified + suffix;
  }

  /** The channel address, read from whichever broker operation trait the operation carries. */
  private String channelAddress(Shape channelShape) {
    for (Map.Entry<ShapeId, String> entry : ADDRESS_MEMBERS.entrySet()) {
      Optional<String> name =
          channelShape
              .findTrait(entry.getKey())
              .map(t -> t.toNode().expectObjectNode().expectStringMember(entry.getValue()).getValue());
      if (name.isPresent()) {
        return name.get();
      }
    }
    throw new IllegalStateException(
        "Operation " + channelShape.getId() + " has no broker operation trait");
  }

  /** The Kafka operation trait's value node, if the operation carries one. */
  private Optional<ObjectNode> kafkaOperationTrait(Shape channelShape) {
    return channelShape
        .findTrait(KAFKA_PRODUCE)
        .or(() -> channelShape.findTrait(KAFKA_CONSUME))
        .map(t -> t.toNode().expectObjectNode());
  }

  /** The channel's protocol binding, or null where the broker has no standard AsyncAPI binding. */
  private Node channelBindings(String address, Shape channelShape) {
    if (kafkaOperationTrait(channelShape).isPresent()) {
      return kafkaChannelBindings(address, channelShape);
    }
    return null; // Redis has no standard AsyncAPI channel binding.
  }

  private Optional<String> documentation(Shape shape) {
    return shape.getTrait(DocumentationTrait.class).map(DocumentationTrait::getValue);
  }

  /** Minimal JSON Schema type node for a Kafka key or header member. */
  private Node jsonTypeOf(MemberShape member) {
    ShapeType type = model.expectShape(member.getTarget()).getType();
    String jsonType =
        switch (type) {
          case BYTE, SHORT, INTEGER, LONG, BIG_INTEGER -> "integer";
          case FLOAT, DOUBLE, BIG_DECIMAL -> "number";
          case BOOLEAN -> "boolean";
          default -> "string";
        };
    return Node.objectNodeBuilder().withMember("type", jsonType).build();
  }

  private static Node ref(String pointer) {
    return Node.objectNodeBuilder().withMember("$ref", pointer).build();
  }

  /** One channel (a Kafka topic or Redis address), accumulated from operation address traits. */
  private static final class Channel {
    private final String topic;
    private final Node bindings;
    private final Set<ShapeId> messages = new LinkedHashSet<>();

    Channel(String topic, Node bindings) {
      this.topic = topic;
      this.bindings = bindings;
    }
  }
}
