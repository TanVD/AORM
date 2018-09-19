import groovy.lang.GroovyObject
import org.jfrog.gradle.plugin.artifactory.dsl.PublisherConfig
import org.jfrog.gradle.plugin.artifactory.dsl.ResolverConfig

group = "tanvd.aorm"
version = "1.1-SNAPSHOT"

val kotlin_version = "1.2.70"

buildscript {
    repositories {
        mavenCentral()
        jcenter()
    }
    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.2.70")
        classpath("org.jfrog.buildinfo:build-info-extractor-gradle:4.7.5")
    }
}

plugins {
    kotlin("jvm") version "1.2.70" apply true
    `maven-publish` apply true
    id("com.jfrog.artifactory") version "4.7.5" apply true

}

repositories {
    mavenCentral()
    jcenter()

}

kotlin.sourceSets {
    this["main"].kotlin.also {
        it.srcDir("src/main")
    }
    this["test"].kotlin.also {
        it.srcDir("src/test")
    }
}

dependencies {
    compile("org.jetbrains.kotlin", "kotlin-stdlib", kotlin_version)
    compile("org.jetbrains.kotlin", "kotlin-reflect", kotlin_version)
    compile("ru.yandex.clickhouse", "clickhouse-jdbc", "0.1.41")
    compile("org.slf4j", "slf4j-api", "1.7.25")

    testCompile("org.testng", "testng", "6.11")
    testCompile("org.mockito", "mockito-all", "1.10.19")
    testCompile("org.powermock", "powermock-mockito-release-full", "1.6.4")
}

(tasks["test"] as Test).apply {
    if (project.hasProperty("clickhouseUrl")) {
        systemProperty("clickhouseUrl", project.properties["clickhouseUrl"]!!)
    }
    if (project.hasProperty("clickhouseUser")) {
        systemProperty("clickhouseUser", project.properties["clickhouseUser"]!!)
    }
    if (project.hasProperty("clickhousePassword")) {
        systemProperty("clickhousePassword", project.properties["clickhousePassword"]!!)
    }

    useTestNG()
}

task<Wrapper>("wrapper") {
    gradleVersion = "4.9"
}

val sourceJar = task<Jar>("sourceJar") {
    classifier = "sources"
    from(kotlin.sourceSets["main"]!!.kotlin.sourceDirectories)
}

publishing {
    publications.create("maven", MavenPublication::class.java) {
        from(components.getByName("java"))
        artifact(sourceJar)
    }
}

artifactory {
    setContextUrl("https://bintray.com/tanvd")

    publish(delegateClosureOf<PublisherConfig> {
        repository(delegateClosureOf<GroovyObject> {
            setProperty("repoKey", "aorm")
            setProperty("username", "tanvd")
            setProperty("password", project.findProperty("bintray_api_key"))
            setProperty("maven", true)
        })
        defaults(delegateClosureOf<GroovyObject> {
            setProperty("publishArtifacts", true)
//            setProperty("publishBuildInfo", true)
            setProperty("publishPom", true)
            invokeMethod("publications", "maven")
        })
    })
}

