import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.aorm"
version = "1.1.17"

plugins {
    kotlin("jvm") version "1.8.21" apply true
    id("tanvd.kosogor") version "1.0.18"
}

val artifactoryUploadEnabled = System.getenv("artifactory_url") != null

repositories {
    mavenCentral()
    if (artifactoryUploadEnabled)
        maven(System.getenv("artifactory_url")!!)
}

dependencies {
    api(kotlin("stdlib"))
    api("com.clickhouse", "clickhouse-jdbc", "0.4.6")
    api("joda-time", "joda-time", "2.12.2")
    api("org.slf4j", "slf4j-api", "1.7.36")

    testImplementation("org.junit.jupiter", "junit-jupiter-api", "5.8.2")
    testImplementation("org.junit.jupiter", "junit-jupiter-engine", "5.8.2")

    testImplementation("org.testcontainers", "clickhouse", "1.18.1")
    testImplementation("org.lz4", "lz4-java", "1.8.0")
}

tasks.withType<JavaCompile> {
    targetCompatibility = "11"
    sourceCompatibility = "11"
}

tasks.withType<KotlinJvmCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "11"
        apiVersion = "1.8"
        languageVersion = "1.8"
        freeCompilerArgs += "-Xuse-ir"
        freeCompilerArgs += "-Xbackend-threads=3"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
    }
}

publishJar {
    publication {
        artifactId = "aorm"
    }

    if (artifactoryUploadEnabled) {
        artifactory {
            serverUrl = System.getenv("artifactory_url")
            repository = System.getenv("artifactory_repo")
            username = System.getenv("artifactory_username")
            secretKey = System.getenv("artifactory_api_key") ?: ""
        }
    }
}
