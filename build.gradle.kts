import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.aorm"
version = "1.1.9"

plugins {
    kotlin("jvm") version "1.5.31" apply true
    id("tanvd.kosogor") version "1.0.10"
}

val bintrayUploadEnabled = System.getenv("bintray_key") != null
val artifactoryUploadEnabled = System.getenv("artifactory_url") != null

repositories {
    mavenCentral()
    if (bintrayUploadEnabled)
        jcenter()
    if (artifactoryUploadEnabled)
        maven(System.getenv("artifactory_url")!!)
}

dependencies {
    api(kotlin("stdlib"))
    api("ru.yandex.clickhouse", "clickhouse-jdbc", "0.3.1-patch")
    api("joda-time", "joda-time", "2.10.8")
    api("org.slf4j", "slf4j-api", "1.7.30")

    testImplementation("org.junit.jupiter", "junit-jupiter-api", "5.6.2")
    testImplementation("org.junit.jupiter", "junit-jupiter-engine", "5.6.2")

    testImplementation("org.testcontainers", "testcontainers", "1.15.2")
    testImplementation("org.testcontainers", "junit-jupiter", "1.15.2")
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        languageVersion = "1.5"
        apiVersion = "1.5"
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

    if (bintrayUploadEnabled) {
        bintray {
            username = "tanvd"
            repository = "aorm"
            info {
                githubRepo = "tanvd/aorm"
                vcsUrl = "https://github.com/tanvd/aorm"
                labels.addAll(listOf("kotlin", "clickhouse"))
                license = "MIT"
                description = "Kotlin SQL Framework for Clickhouse"
            }
        }
    }

    if (artifactoryUploadEnabled) {
        artifactory {
            secretKey = System.getenv("artifactory_api_key")
        }
    }
}
