import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.aorm"
version = "1.1.6-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.3.61" apply true
    id("tanvd.kosogor") version "1.0.7"
}

repositories {
    jcenter()
}

dependencies {
    api(kotlin("stdlib"))
    api("ru.yandex.clickhouse", "clickhouse-jdbc", "0.2.3")
    api("joda-time", "joda-time", "2.10.5")
    api("org.slf4j", "slf4j-api", "1.7.25")

    testCompile("org.junit.jupiter", "junit-jupiter-api", "5.6.0")
    testRuntime("org.junit.jupiter", "junit-jupiter-engine", "5.6.0")

    testCompile("org.testcontainers", "testcontainers", "1.12.5")
    testCompile("org.testcontainers", "junit-jupiter", "1.12.5")
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        languageVersion = "1.3"
        apiVersion = "1.3"
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
