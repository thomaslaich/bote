# Roadmap

The design story — ownership model, trait surface, wire rules — lives in the
[README](README.md). This file only tracks what's next. Nothing here is a
commitment; the repo is exploratory.

## Schema evolution enforcement (the differentiator)

The reason to model messaging contracts in Smithy at all is build-time
enforcement that no other tool provides:

- [ ] `@avroCompatibility` validator: diff the current model against a
      published baseline and reject shape changes that violate the declared
      compatibility mode (BACKWARD, FORWARD, FULL, …). The trait exists; the
      enforcement doesn't yet.
- [ ] The same for `@kafkaJson`: JSON payloads evolve too. Define what
      BACKWARD/FORWARD mean for JSON Schema (add-optional-only, etc.) and
      validate against a baseline.
- [ ] `@kafkaAvro`: validate that multi-event channels use a
      `subjectNamingStrategy` that permits multiple schemas per topic
      (RECORD_NAME or TOPIC_RECORD_NAME).

## Protocol surface

- [ ] AMQP protocol (`@amqpJson`?). This is where request-reply returns:
      AMQP has native `reply_to` / `correlation_id`, so `@reply` — currently
      reserved vocabulary — gets wired into a protocol with real broker
      support instead of a Kafka convention.
- [ ] Command discrimination. Multiple `@command` types on one channel
      currently draw a validator warning because the wire contract doesn't
      say how command types are told apart. Either extend
      `eventDiscrimination` to commands or keep one-command-per-topic as the
      rule.
- [ ] `@kafkaProtobuf`: still a stub; needs the Alloy proto-trait
      integration exercised end to end.

## Channels

- [ ] Parameterized addresses (`smartylighting.streetlights.{streetlightId}.
      lighting.measured`): an address-template syntax plus a `@channelParam`
      member trait, mapped to AsyncAPI channel parameters.

## Publishing

- [x] Maven Central release pipeline (`io.github.thomaslaich.bote`
      namespace, tag-triggered workflow, Sonatype Central Portal).
- [ ] First tagged release, once an NSmithy preview consumer exists to
      validate the wire rules.
