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

val nsmithyVersion = "0.14.0"
val smithyVersion = "1.73.0"

dependencies {
    // The bote trait library and BoteIndex.
    api(project(":"))

    // NSmithy's C# generator loads this integration; it is on the classpath at run time, so it is
    // not a published dependency.
    compileOnly("io.github.thomaslaich.nsmithy:smithy-csharp-codegen:$nsmithyVersion")
    testImplementation("io.github.thomaslaich.nsmithy:smithy-csharp-codegen:$nsmithyVersion")
    testImplementation("software.amazon.smithy:smithy-build:$smithyVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    errorprone("com.google.errorprone:error_prone_core:2.50.0")
}

base {
    archivesName = "smithy-csharp-codegen"
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

mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    if (providers.environmentVariable("ORG_GRADLE_PROJECT_signingInMemoryKey").isPresent) {
        signAllPublications()
    }

    configure(JavaLibrary(javadocJar = JavadocJar.Empty(), sourcesJar = true))

    coordinates(group.toString(), "smithy-csharp-codegen", version.toString())

    pom {
        name.set("smithy-csharp-codegen")
        description.set(
            "NSmithy integration that generates C# messaging code for bote contracts."
        )
        url.set("https://github.com/toml-ai/bote")
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
            url.set("https://github.com/toml-ai/bote")
            connection.set("scm:git:git://github.com/toml-ai/bote.git")
            developerConnection.set("scm:git:ssh://git@github.com/toml-ai/bote.git")
        }
    }
}
