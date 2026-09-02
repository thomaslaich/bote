# Changelog

All notable changes to bote are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and bote aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Exploratory.** bote has not had a release yet. Traits, protocol specs, and
> generated output will change without notice before 1.0.

## [Unreleased]

### Added

- Redis Streams request/reply operations: `@redisStreamAdd` operations may
  return an `@reply` shape using a dynamic Pub/Sub reply channel and
  correlation ID.
- AsyncAPI reply objects and correlation metadata for Redis Streams
  request/reply.

### Changed

- The `bote` and `smithy-asyncapi` artifacts now target Java 21.
