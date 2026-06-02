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

# Render a generated AsyncAPI doc in AsyncAPI Studio (live-reloads on rebuild).
# Pass an example module and a service, e.g. `just studio kafka OrderService`
# or `just studio redis ChatRoom`.
studio example="kafka" service="StreetlightDevice": build
    npx -y @asyncapi/cli start studio examples/{{ example }}/build/smithyprojections/{{ example }}/source/asyncapi/{{ service }}.asyncapi.json

# Publish bote + smithy-asyncapi to the local Maven repo (~/.m2) for local consumers
publish-local:
    gradle publishToMavenLocal

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
