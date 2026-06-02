# List available recipes
default:
    @just --list

# Build and validate Smithy models
build:
    gradle build

# Format all .smithy files
fmt:
    treefmt .
    gradle smithyFormat

# Render a generated AsyncAPI doc in AsyncAPI Studio (live-reloads on rebuild).
# Pass an example module and a service, e.g. `just studio kafka-orders OrderService`
# or `just studio redis ChatProducer`.
studio example="kafka-streetlights" service="StreetlightsBackend": build
    npx -y @asyncapi/cli start studio examples/{{ example }}/build/smithyprojections/{{ example }}/source/asyncapi-codegen/{{ service }}.asyncapi.json

# Publish bote + smithy-asyncapi-codegen to the local Maven repo (~/.m2) for local consumers
publish-local:
    gradle publishToMavenLocal

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
