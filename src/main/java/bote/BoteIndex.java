package bote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.smithy.model.Model;
import software.amazon.smithy.model.knowledge.KnowledgeIndex;
import software.amazon.smithy.model.node.Node;
import software.amazon.smithy.model.node.ObjectNode;
import software.amazon.smithy.model.shapes.ServiceShape;
import software.amazon.smithy.model.shapes.Shape;
import software.amazon.smithy.model.shapes.ShapeId;
import software.amazon.smithy.model.shapes.UnionShape;

/**
 * Resolves the bote structure of a model: which services use a bote protocol, which address trait
 * that protocol binds with, and which channels each service owns.
 *
 * <p>A bote service receives commands, which are its operations, and publishes events to channels,
 * which are the unions it lists in {@code @bote#channels}. Both are bound to a broker address by
 * the address trait of the service's protocol.
 */
public final class BoteIndex implements KnowledgeIndex {

  public static final ShapeId KAFKA_JSON = ShapeId.from("bote#kafkaJson");
  public static final ShapeId KAFKA_AVRO = ShapeId.from("bote#kafkaAvro");
  public static final ShapeId KAFKA_PROTOBUF = ShapeId.from("bote#kafkaProtobuf");
  public static final ShapeId REDIS_STREAMS_JSON = ShapeId.from("bote#redisStreamsJson");
  public static final ShapeId REDIS_PUBSUB_JSON = ShapeId.from("bote#redisPubSubJson");
  public static final ShapeId REDIS_STREAMS_CBOR = ShapeId.from("bote#redisStreamsCbor");
  public static final ShapeId REDIS_PUBSUB_CBOR = ShapeId.from("bote#redisPubSubCbor");

  public static final ShapeId KAFKA_TOPIC = ShapeId.from("bote#kafkaTopic");
  public static final ShapeId REDIS_STREAM = ShapeId.from("bote#redisStream");
  public static final ShapeId REDIS_CHANNEL = ShapeId.from("bote#redisChannel");

  public static final ShapeId CHANNELS = ShapeId.from("bote#channels");
  public static final ShapeId EVENT = ShapeId.from("bote#event");
  public static final ShapeId KAFKA_KEY = ShapeId.from("bote#kafkaKey");
  public static final ShapeId KAFKA_HEADER = ShapeId.from("bote#kafkaHeader");
  public static final ShapeId KAFKA_TOPIC_CONFIG = ShapeId.from("bote.infra#kafkaTopicConfig");

  /**
   * A broker address trait.
   *
   * @param id the trait's shape id
   * @param addressMember the trait member holding the address
   * @param broker the broker family, which scopes address names
   */
  public record AddressTrait(ShapeId id, String addressMember, String broker) {}

  public static final AddressTrait KAFKA_TOPIC_TRAIT =
      new AddressTrait(KAFKA_TOPIC, "name", "kafka");
  public static final AddressTrait REDIS_STREAM_TRAIT =
      new AddressTrait(REDIS_STREAM, "key", "redis-stream");
  public static final AddressTrait REDIS_CHANNEL_TRAIT =
      new AddressTrait(REDIS_CHANNEL, "name", "redis-pubsub");

  /** Every address trait. */
  public static final List<AddressTrait> ADDRESS_TRAITS =
      List.of(KAFKA_TOPIC_TRAIT, REDIS_STREAM_TRAIT, REDIS_CHANNEL_TRAIT);

  /** The bote protocol traits, in the order a service's protocol is resolved. */
  public static final List<ShapeId> PROTOCOLS =
      List.of(
          KAFKA_JSON,
          KAFKA_AVRO,
          KAFKA_PROTOBUF,
          REDIS_STREAMS_JSON,
          REDIS_STREAMS_CBOR,
          REDIS_PUBSUB_JSON,
          REDIS_PUBSUB_CBOR);

  private static final Map<ShapeId, AddressTrait> PROTOCOL_ADDRESS_TRAITS =
      Map.of(
          KAFKA_JSON, KAFKA_TOPIC_TRAIT,
          KAFKA_AVRO, KAFKA_TOPIC_TRAIT,
          KAFKA_PROTOBUF, KAFKA_TOPIC_TRAIT,
          REDIS_STREAMS_JSON, REDIS_STREAM_TRAIT,
          REDIS_STREAMS_CBOR, REDIS_STREAM_TRAIT,
          REDIS_PUBSUB_JSON, REDIS_CHANNEL_TRAIT,
          REDIS_PUBSUB_CBOR, REDIS_CHANNEL_TRAIT);

  /**
   * A shape's binding to a broker address.
   *
   * @param trait the address trait
   * @param name the address, such as a topic name
   * @param value the trait's full value, including address-level settings
   */
  public record Address(AddressTrait trait, String name, ObjectNode value) {}

  private final Model model;

  public BoteIndex(Model model) {
    this.model = model;
  }

  public static BoteIndex of(Model model) {
    return model.getKnowledge(BoteIndex.class, BoteIndex::new);
  }

  /** The services that carry a bote protocol trait, in model order. */
  public List<ServiceShape> services() {
    return model.getServiceShapes().stream().filter(s -> protocol(s).isPresent()).toList();
  }

  /** The service's bote protocol trait, if it has one. */
  public static Optional<ShapeId> protocol(ServiceShape service) {
    return PROTOCOLS.stream().filter(service::hasTrait).findFirst();
  }

  /** The address trait the service's protocol binds operations and channels with. */
  public static Optional<AddressTrait> addressTrait(ServiceShape service) {
    return protocol(service).map(PROTOCOL_ADDRESS_TRAITS::get);
  }

  /** The ids the service lists in {@code @bote#channels}, in declaration order. */
  public static List<ShapeId> channelIds(ServiceShape service) {
    return service
        .findTrait(CHANNELS)
        .map(
            trait ->
                trait.toNode().expectArrayNode().getElements().stream()
                    .map(node -> ShapeId.from(node.expectStringNode().getValue()))
                    .toList())
        .orElse(List.of());
  }

  /** The unions the service lists as its channels, skipping ids the model does not resolve. */
  public List<UnionShape> channels(ServiceShape service) {
    List<UnionShape> channels = new ArrayList<>();
    for (ShapeId id : channelIds(service)) {
      model.getShape(id).flatMap(Shape::asUnionShape).ifPresent(channels::add);
    }
    return channels;
  }

  /** Every address trait the shape carries. Valid models have at most one. */
  public static List<Address> addresses(Shape shape) {
    List<Address> addresses = new ArrayList<>();
    for (AddressTrait trait : ADDRESS_TRAITS) {
      shape
          .findTrait(trait.id())
          .map(t -> t.toNode().expectObjectNode())
          .ifPresent(
              value ->
                  addresses.add(
                      new Address(
                          trait,
                          value.expectStringMember(trait.addressMember()).getValue(),
                          value)));
    }
    return addresses;
  }

  /** The shape's address, if it carries an address trait. */
  public static Optional<Address> address(Shape shape) {
    List<Address> addresses = addresses(shape);
    return addresses.isEmpty() ? Optional.empty() : Optional.of(addresses.get(0));
  }

  /** The value of a boolean member of the shape's address, such as Kafka's {@code compacted}. */
  public static boolean addressFlag(Address address, String member) {
    return address.value().getBooleanMember(member).map(b -> b.getValue()).orElse(false);
  }

  /** The {@code eventDiscrimination} of a {@code @kafkaJson} service, if it sets one. */
  public static Optional<String> eventDiscrimination(ServiceShape service) {
    return service
        .findTrait(KAFKA_JSON)
        .map(t -> t.toNode().expectObjectNode())
        .flatMap(n -> n.getStringMember("eventDiscrimination"))
        .map(Node::expectStringNode)
        .map(s -> s.getValue());
  }
}
