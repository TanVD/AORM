import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import tanvd.kosogor.proxy.publishJar

group = "tanvd.aorm"
version = "1.1.18-SNAPSHOT"

plugins {
    kotlin("jvm") version "2.1.20" apply true
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
    api("com.clickhouse", "clickhouse-jdbc", "0.8.5")
    api("joda-time", "joda-time", "2.14.0")
    api("org.slf4j", "slf4j-api", "2.0.17")

    testImplementation("org.junit.jupiter", "junit-jupiter-api", "5.8.2")
    testImplementation("org.junit.jupiter", "junit-jupiter-engine", "5.8.2")

    testImplementation("org.testcontainers", "clickhouse", "1.21.0")
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        apiVersion.set(KotlinVersion.KOTLIN_2_1)
        languageVersion.set(KotlinVersion.KOTLIN_2_1)
        // https://jakewharton.com/kotlins-jdk-release-compatibility-flag/
        // https://youtrack.jetbrains.com/issue/KT-49746/Support-Xjdk-release-in-gradle-toolchain#focus=Comments-27-8935065.0-0
        freeCompilerArgs.addAll("-Xjdk-release=17")
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of("17"))
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
