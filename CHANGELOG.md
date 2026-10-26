# Changelog

All notable changes to bote are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and bote aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Exploratory.** bote has not had a release yet. Traits, protocol specs, and
> generated output will change without notice before 1.0.

## [Unreleased]

### Changed

- Commands carry their operation name as `bote-type`, a Kafka header or a
  Redis Streams entry field, so several commands can share an address. The
  "multiple command types" warning is gone.
- Redis Pub/Sub services have no operations, and `@redisChannel` applies to
  unions only: Pub/Sub keeps no messages and delivers each one to every
  subscriber, so commands use Redis Streams.
- Events are published to channels: unions a service lists in `@channels`,
  whose members are `@event` structures. Consume, read, and subscribe
  operations and `@streaming` event unions are gone.
- `@kafkaTopic`, `@redisStream`, and `@redisChannel` bind both operations and
  channels to an address, replacing `@kafkaProduce`, `@kafkaConsume`,
  `@redisStreamAdd`, `@redisStreamRead`, `@redisPublish`, and
  `@redisSubscribe`.
- Commands and replies are Smithy `@input` and `@output` structures;
  `@command` and `@reply` are gone.
- Every operation of a bote service needs an address trait, and an address is
  either one channel or the address of command operations.
- `bote.infra#kafkaTopicConfig` and `@avroCompatibility` apply to the shape
  that carries the topic, including channels.

### Added

- C# code generation for `@kafkaJson`, `@redisStreamsJson`, and
  `@redisPubSubJson` services, as the `smithy-csharp-codegen` integration for
  NSmithy 0.14.0, and the `Bote`, `Bote.Kafka`, and
  `Bote.Redis` runtime packages.
- C# code generation for `@redisStreamsCbor` and `@redisPubSubCbor`; Redis
  Streams replies use the protocol's encoding.
- Redis Streams entries carry a `content-type` field.
- Generated Kafka consumer registrations take `KafkaConsumerOptions`, which
  override the group ID and offset reset per consumer.
- `@redisStreamsCbor` and `@redisPubSubCbor` protocols, which encode Redis
  values with the CBOR serialization `rpcv2Cbor` defines.
- `BoteIndex`, a knowledge index that resolves a model's bote services,
  address traits, and channels for generators.

## [0.1.0] - 2026-09-02

### Added

- Smithy protocol and binding traits for Kafka JSON, Avro, and Protobuf;
  Redis Streams JSON; and Redis Pub/Sub JSON.
- Broker-independent `@command`, `@event`, and `@reply` payload roles, plus
  Kafka key, header, event-discrimination, Avro compatibility, and topic
  infrastructure traits.
- Build-time validation for protocol bindings, channel ownership and shared
  configuration, Kafka keys, and explicit Protobuf field indexes.
- Redis Streams request/reply operations: `@redisStreamAdd` operations may
  return an `@reply` shape using a dynamic Pub/Sub reply channel and
  correlation ID.
- An AsyncAPI 3.1 generator with owner and client perspectives, Kafka channel
  bindings, JSON Schema payloads, and Redis Streams reply and correlation
  metadata.
- Kafka and Redis example models with checked-in AsyncAPI documents for JSON,
  Avro, Protobuf, Streams, and Pub/Sub contracts.
- Maven Central publication for the `bote` and `smithy-asyncapi` artifacts,
  targeting Java 21.

[Unreleased]: https://github.com/toml-ai/bote/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/toml-ai/bote/releases/tag/v0.1.0
