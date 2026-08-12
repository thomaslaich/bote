# List available recipes
default:
    @just --list

# Build and validate Smithy models
build:
    gradle build

# Run unit tests
test:
    gradle test

# Format all .smithy files
fmt:
    treefmt .
    gradle smithyFormat

# Check formatting without rewriting anything (smithyFormat has no check mode,
# so it reformats and the diff has to come out empty).
check-format:
    treefmt --ci
    gradle smithyFormat
    git diff --exit-code -- '*.smithy'

# Render a generated AsyncAPI doc in AsyncAPI Studio (live-reloads on rebuild).
# Pass an example module, a service, and optionally a perspective, e.g.
# `just studio kafka OrderService` or `just studio redis ChatRoom client`.
studio example="kafka" service="StreetlightDevice" perspective="owner": build
    npx -y @asyncapi/cli start studio examples/{{ example }}/build/smithyprojections/{{ example }}/{{ if perspective == "owner" { "source" } else { "client" } }}/asyncapi/{{ service }}.asyncapi.json

# Regenerate the golden AsyncAPI documents CI diffs against
golden: build
    cp examples/kafka/build/smithyprojections/kafka/source/asyncapi/*.asyncapi.json examples/kafka/expected/
    cp examples/redis/build/smithyprojections/redis/source/asyncapi/*.asyncapi.json examples/redis/expected/

# Verify the generated AsyncAPI documents match the golden files (what CI runs)
verify-golden: build
    diff -ru examples/kafka/expected examples/kafka/build/smithyprojections/kafka/source/asyncapi
    diff -ru examples/redis/expected examples/redis/build/smithyprojections/redis/source/asyncapi

# Everything CI and the release workflow run before publishing
ci: check-format build verify-golden

# Publish bote + smithy-asyncapi to the local Maven repo (~/.m2) for local consumers
publish-local:
    gradle publishToMavenLocal

# Publish bote + smithy-asyncapi to Maven Central via the Sonatype Central Portal.
# Used by the release workflow; expects MAVEN_CENTRAL_USERNAME / MAVEN_CENTRAL_PASSWORD
# and ORG_GRADLE_PROJECT_signingInMemoryKey / ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
# to be set in the environment.
publish VERSION:
    gradle -Pversion={{ VERSION }} :publishAndReleaseToMavenCentral :codegen:smithy-asyncapi:publishAndReleaseToMavenCentral

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
