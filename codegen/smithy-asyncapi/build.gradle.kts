import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    `java-library`
    id("net.ltgt.errorprone")
    id("com.vanniktech.maven.publish")
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

    errorprone("com.google.errorprone:error_prone_core:2.50.0")
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

mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    if (providers.environmentVariable("ORG_GRADLE_PROJECT_signingInMemoryKey").isPresent) {
        signAllPublications()
    }

    configure(JavaLibrary(javadocJar = JavadocJar.Empty(), sourcesJar = true))

    coordinates(group.toString(), "smithy-asyncapi", version.toString())

    pom {
        name.set("smithy-asyncapi")
        description.set(
            "Smithy build plugin that generates AsyncAPI 3.1 documents from bote messaging contracts."
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
