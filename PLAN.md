# legierung — Smithy Protocol Extensions for Kafka

## Vision

A language-agnostic Smithy trait library for Kafka, similar in scope to
[Disney's Alloy](https://github.com/disneystreaming/alloy). This repo owns
the *contract* — trait definitions, protocol specs, and validators — not the
codegen. Codegen consumers (smithy4s, smithy-typescript, etc.) depend on this
library to implement the protocol.

---

## Deliverables

- `.smithy` files defining Kafka-specific traits and protocol definitions
- Smithy validators enforcing structural rules
- A published JAR that codegen tools can depend on

No codegen lives here. This repo is the source of truth for the protocol
specification.

---

## Protocol Design

### Serialization formats

Kafka is transport-agnostic — it moves bytes. Serialization is a separate
concern. We model this as **one protocol trait per encoding**, following the
Smithy precedent of `restJson1` / `restXml`:

| Protocol trait   | Encoding       | Notes                                      |
|------------------|----------------|--------------------------------------------|
| `@kafkaJson`     | JSON           | Start here — simplest to specify precisely |
| `@kafkaAvro`     | Avro           | Requires schema registry integration       |
| `@kafkaProtobuf` | Protocol Buffers | Smithy shapes map cleanly to proto messages |

Start with `@kafkaJson`. Design the trait surface so `@kafkaAvro` and
`@kafkaProtobuf` can be added later without breaking changes.

### Why not a single `@kafka(encoding: avro)` trait?

Avro and Protobuf each carry significant additional protocol surface:
schema registry URLs, schema IDs in message headers, and compatibility modes.
Bundling these as options on a single trait would make the spec harder to
validate and harder for codegen consumers to implement correctly. Separate
protocol traits keep each spec self-contained.

### Schema evolution (future — @kafkaAvro / @kafkaProtobuf)

Avro and Protobuf have explicit compatibility contracts. A key future feature
is encoding these as Smithy validators — e.g., annotating a service with
`@avroCompatibility("BACKWARD")` and having the validator enforce that all
shape changes respect that contract. No existing tool does this well.

---

## Trait Surface

Applied at **service** level:

```smithy
@kafkaJson          // wire protocol for this service
```

Applied at **operation** level:

```smithy
@kafkaProducer                          // operation publishes to a topic
@kafkaConsumer(group: "order-processor") // operation consumes from a topic
```

Applied at **shape/member** level:

```smithy
@kafkaTopic(name: "orders")   // binds an operation to a topic
@kafkaKey                     // marks the message key member
@kafkaHeader("x-trace-id")   // maps a member to a Kafka header
```

Optional infrastructure traits:

```smithy
@kafkaCompacted   // topic uses log compaction
```

### Example

```smithy
$version: "2"

namespace com.example

use legierung#kafkaJson
use legierung#kafkaTopic
use legierung#kafkaProducer
use legierung#kafkaConsumer
use legierung#kafkaKey

@kafkaJson
service OrderService {
    operations: [PublishOrder, ConsumeOrders]
}

@kafkaProducer
@kafkaTopic(name: "orders")
operation PublishOrder {
    input: OrderEvent
}

@kafkaConsumer(group: "order-processor")
@kafkaTopic(name: "orders")
operation ConsumeOrders {
    output: OrderEvent
}

structure OrderEvent {
    @kafkaKey
    orderId: String

    customerId: String
    totalCents: Integer
}
```

---

## Validators

Rules to enforce at model-build time:

- Every `@kafkaProducer` or `@kafkaConsumer` operation must have a `@kafkaTopic`
- Exactly one member per input/output structure may be annotated `@kafkaKey`
- `@kafkaKey` member must be a simple type (String, Integer, etc.)
- `@kafkaConsumer` must declare a `group`
- A service annotated with a Kafka protocol trait must only bind Kafka operations

---

## Phased Roadmap

### Phase 1 — Foundation
- [ ] Repo structure: build system choice (sbt / Gradle / Mill), module layout
- [ ] Smithy trait definitions for `@kafkaJson` protocol
- [ ] Core traits: `@kafkaTopic`, `@kafkaKey`, `@kafkaHeader`, `@kafkaProducer`, `@kafkaConsumer`
- [ ] Smithy validators for structural rules above
- [ ] Publish JAR to Maven Central (or GitHub Packages initially)

### Phase 2 — Avro protocol
- [ ] `@kafkaAvro` protocol trait
- [ ] Schema registry binding traits
- [ ] Avro compatibility mode validators (`@avroCompatibility`)

### Phase 3 — Protobuf protocol
- [ ] `@kafkaProtobuf` protocol trait
- [ ] Proto field number traits (for stable wire encoding)

---

## Decisions

- **Build system**: Gradle with `software.amazon.smithy.gradle.smithy-jar` plugin. AWS builds Smithy itself with Gradle; the official plugin is Gradle-first. sbt is only warranted for Scala codegen projects (Alloy's case); Mill has insufficient Smithy ecosystem support.
- **Dev environment**: devenv (Nix) — toolchain declared in `devenv.nix`, currently Java 21 + Gradle.
- **Namespace**: `legierung#`

## Decisions (continued)

- **Schema registry**: out of scope for Phase 1. Only relevant for `@kafkaAvro` / `@kafkaProtobuf`; `@kafkaJson` has no need for it.
- **Partition key**: left to runtime config. Smithy models message shape, not routing/infrastructure decisions. Can be revisited in Phase 2 if there is demand.
