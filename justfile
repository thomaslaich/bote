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

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
