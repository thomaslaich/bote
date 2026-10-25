# List available recipes
default:
    @just --list

# Build and validate Smithy models
build:
    gradle build

# Run unit tests
test:
    gradle test

# Stage the trait library and the C# generator in build/maven-repo, where the .NET tests resolve them
stage:
    gradle publishAllPublicationsToStagingRepository

# Build and test the C# runtime and the code the C# generator emits. The Redis integration tests run
# when BOTE_TEST_REDIS points at a Redis; `devenv test` starts one and sets it.
dotnet-test: stage
    # NSmithy caches resolved plugins by coordinate; drop it so the freshly staged snapshot is used.
    rm -rf dotnet/tests/Bote.Generated.Tests/obj
    BOTE_MAVEN_REPO={{ justfile_directory() }}/build/maven-repo dotnet test dotnet/Bote.slnx

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
ci: check-format build verify-golden dotnet-test

# Publish bote, smithy-asyncapi and smithy-csharp-codegen to the local Maven repo (~/.m2) for local consumers
publish-local:
    gradle publishToMavenLocal

# Pack the C# runtime packages to dotnet/artifacts/packages
pack VERSION="0.0.0-SNAPSHOT":
    for project in Bote Bote.Kafka Bote.Redis; do dotnet pack dotnet/src/$project -c Release -p:Version={{ VERSION }}; done

# Publish the Java artifacts to Maven Central via the Sonatype Central Portal and the C# runtime to
# NuGet. Used by the release workflow; expects MAVEN_CENTRAL_USERNAME / MAVEN_CENTRAL_PASSWORD,
# ORG_GRADLE_PROJECT_signingInMemoryKey / ORG_GRADLE_PROJECT_signingInMemoryKeyPassword and
# NUGET_API_KEY to be set in the environment.
publish VERSION: (pack VERSION)
    gradle -Pversion={{ VERSION }} :publishAndReleaseToMavenCentral :codegen:smithy-asyncapi:publishAndReleaseToMavenCentral :codegen:smithy-csharp:publishAndReleaseToMavenCentral
    dotnet nuget push "dotnet/artifacts/packages/*.{{ VERSION }}.nupkg" --api-key "$NUGET_API_KEY" --source https://api.nuget.org/v3/index.json

# Clean build outputs
clean:
    gradle clean

# Clean and rebuild
rebuild: clean build
