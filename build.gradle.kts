plugins {
    `java-library`
    `maven-publish`
    id("software.amazon.smithy.gradle.smithy-jar") version "1.4.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.disneystreaming.alloy:alloy-core:0.3.39")
    compileOnly("software.amazon.smithy:smithy-model:1.69.0")
    testImplementation("software.amazon.smithy:smithy-model:1.69.0")
    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

group = "io.bote"
version = "0.1.0-SNAPSHOT"

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}
