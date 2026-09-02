import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    `java-library`
    id("software.amazon.smithy.gradle.smithy-jar") version "1.4.0"
    id("net.ltgt.errorprone") version "5.1.0"
    id("com.vanniktech.maven.publish") version "0.30.0"
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
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.withType<JavaCompile> {
    options.release.set(21)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// The smithy-jar plugin stages the model files into the jar resources; the
// sources jar added later by the publish plugin picks them up and needs the
// ordering declared.
tasks.matching { it.name == "sourcesJar" }.configureEach {
    dependsOn(tasks.named("smithyJarStaging"))
}

mavenPublishing {
    // Targets the Sonatype Central Portal (https://central.sonatype.com).
    // Requires MAVEN_CENTRAL_USERNAME / MAVEN_CENTRAL_PASSWORD secrets (user token).
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    // Sign publications when a PGP key is supplied via env vars
    // ORG_GRADLE_PROJECT_signingInMemoryKey / signingInMemoryKeyPassword.
    // Skipped for local `publishToMavenLocal` runs that don't carry secrets.
    if (providers.environmentVariable("ORG_GRADLE_PROJECT_signingInMemoryKey").isPresent) {
        signAllPublications()
    }

    configure(JavaLibrary(javadocJar = JavadocJar.Empty(), sourcesJar = true))

    coordinates(group.toString(), "bote", version.toString())

    pom {
        name.set("bote")
        description.set(
            "Smithy trait library for messaging contracts (Kafka, Redis): trait definitions, protocol specs, and validators."
        )
        url.set("https://github.com/thomaslaich/bote")
        inceptionYear.set("2026")
        licenses {
            license {
                name.set("Apache-2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("thomaslaich")
                name.set("Thomas Laich")
                url.set("https://github.com/thomaslaich")
            }
        }
        scm {
            url.set("https://github.com/thomaslaich/bote")
            connection.set("scm:git:git://github.com/thomaslaich/bote.git")
            developerConnection.set("scm:git:ssh://git@github.com/thomaslaich/bote.git")
        }
    }
}
