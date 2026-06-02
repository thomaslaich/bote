plugins {
    java
    id("software.amazon.smithy.gradle.smithy-jar") version "1.4.0"
}

repositories {
    mavenCentral()
}

dependencies {
    // Pins the Smithy CLI version used to run the build.
    add("smithyCli", "software.amazon.smithy:smithy-cli:1.69.0")

    // The bote trait library supplies the protocol/channel/key traits used below.
    // On the runtime classpath so the produced model jar validates standalone.
    implementation(project(":"))

    // The AsyncAPI codegen plugin — placing it on the build classpath makes
    // the "asyncapi" smithy-build plugin discoverable.
    add("smithyBuild", project(":codegen:smithy-asyncapi"))
}
