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

# Render the generated AsyncAPI doc in AsyncAPI Studio (live-reloads on rebuild)
studio: build
    npx -y @asyncapi/cli start studio example/build/smithyprojections/example/source/asyncapi/StreetlightsKafka.asyncapi.json

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
