plugins {
    `java-library`
    `maven-publish`
}

repositories {
    mavenCentral()
}

val smithyVersion = "1.69.0"

dependencies {
    // The bote trait library — needed to read the protocol/topic/key traits.
    api(project(":"))

    api("software.amazon.smithy:smithy-model:$smithyVersion")
    api("software.amazon.smithy:smithy-build:$smithyVersion")
    api("software.amazon.smithy:smithy-jsonschema:$smithyVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

base {
    archivesName = "smithy-asyncapi"
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile> {
    options.release.set(17)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

group = "io.bote"
version = "0.1.0-SNAPSHOT"

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "smithy-asyncapi"
            from(components["java"])
        }
    }
}
