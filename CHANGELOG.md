# Changelog

All notable changes to bote are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and bote aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Exploratory.** bote has not had a release yet. Traits, protocol specs, and
> generated output will change without notice before 1.0.

## [Unreleased]

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

[Unreleased]: https://github.com/thomaslaich/bote/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/thomaslaich/bote/releases/tag/v0.1.0
