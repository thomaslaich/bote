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
# The example emits one doc per service; pass another, e.g. `just studio StreetlightDevice`.
studio service="StreetlightsBackend": build
    npx -y @asyncapi/cli start studio example/build/smithyprojections/example/source/asyncapi/{{service}}.asyncapi.json

# Publish bote + bote-asyncapi to the local Maven repo (~/.m2) for local consumers
publish-local:
    gradle publishToMavenLocal

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
