plugins {
    `java-library`
    `maven-publish`
    id("software.amazon.smithy.gradle.smithy-jar") version "1.4.0"
    id("net.ltgt.errorprone") version "5.1.0"
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

    errorprone("com.google.errorprone:error_prone_core:2.50.0")
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
            from(components["java"])
        }
    }
}
