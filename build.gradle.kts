import groovy.lang.GroovyObject

group = "tanvd.aorm"
version = "1.1-SNAPSHOT"

val kotlin_version = "1.2.70"

plugins {
    kotlin("jvm") version "1.2.70" apply true
    `maven-publish` apply true
    id("com.jfrog.bintray") version "1.8.4" apply true

}

repositories {
    mavenCentral()
    maven { setUrl("https://dl.bintray.com/jfrog/jfrog-jars") }
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



val sourceJar = task<Jar>("sourceJar") {
    classifier = "sources"
    from(kotlin.sourceSets["main"]!!.kotlin.sourceDirectories)
}


publishing {
    publications.create("maven", MavenPublication::class.java) {
        from(components.getByName("java"))
        artifact(sourceJar)
    }
    repositories {
        maven {
            setUrl("https://bintray.com/tanvd/aorm")
        }
    }
}

bintray {
    user = "tanvd"
    key = project.findProperty("bintray_api_key") as String
    setPublications("maven")
}

task<Wrapper>("wrapper") {
    gradleVersion = "4.9"
}

