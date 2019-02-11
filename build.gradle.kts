import com.jfrog.bintray.gradle.BintrayExtension
import groovy.lang.GroovyObject
import org.jfrog.gradle.plugin.artifactory.dsl.PublisherConfig

group = "tanvd.aorm"
version = "1.1.3-SNAPSHOT"

plugins {
    idea apply true
    kotlin("jvm") version "1.3.21" apply true
    `maven-publish` apply true
    id("com.jfrog.bintray") version "1.8.4" apply true
    id("com.jfrog.artifactory") version "4.7.5" apply true
}

idea {
    module {
        excludeDirs = files(".gradle", ".idea", "gradle", "gradlew", "gradlew.bat", "build", "out", "classes").toSet()
    }
}

repositories {
    jcenter()
    maven { setUrl("https://dl.bintray.com/jfrog/jfrog-jars") }
}

dependencies {
    compile("org.jetbrains.kotlin", "kotlin-stdlib", "1.3.21")
    compile("org.jetbrains.kotlin", "kotlin-reflect", "1.3.21")
    compile("ru.yandex.clickhouse", "clickhouse-jdbc", "0.1.50")
    compile("joda-time", "joda-time", "2.9.9")
    compile("org.slf4j", "slf4j-api", "1.7.25")

    testCompile("org.testng", "testng", "6.11")
    testCompile("org.mockito", "mockito-all", "1.10.19")
    testCompile("org.powermock", "powermock-mockito-release-full", "1.6.4")
}

(tasks["test"] as Test).apply {
    systemProperty("clickhouseUrl", System.getenv("clickhouseUrl"))
    systemProperty("clickhouseUser", System.getenv("clickhouseUser"))
    systemProperty("clickhousePassword", System.getenv("clickhousePassword"))

    useTestNG()
}


val sourceJar = task<Jar>("sourceJar") {
    classifier = "sources"
    from(kotlin.sourceSets["main"]!!.kotlin.sourceDirectories)
}


publishing {
    publications.create<MavenPublication>("aorm_package") {
        artifactId = "aorm"

        from(components.getByName("java"))
        artifact(sourceJar)
    }
}

artifactory {
    setContextUrl("https://oss.jfrog.org/artifactory")

    publish(delegateClosureOf<PublisherConfig> {
        repository(delegateClosureOf<GroovyObject> {
            setProperty("repoKey", "oss-snapshot-local")
            setProperty("username", "tanvd")
            setProperty("password", System.getenv("artifactory_api_key"))
            setProperty("maven", true)
        })

        defaults(delegateClosureOf<GroovyObject> {
            setProperty("publishArtifacts", true)
            setProperty("publishPom", true)
            invokeMethod("publications", "aorm_package")
        })
    })
}

bintray {
    user = "tanvd"
    key = System.getenv("bintray_api_key")
    publish = true
    setPublications("aorm_package")
    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "aorm"
        name = "aorm"
        githubRepo = "tanvd/aorm"
        vcsUrl = "https://github.com/tanvd/aorm"
        setLabels("kotlin", "clickhouse")
        setLicenses("MIT")
        desc = "Kotlin SQL Framework for Clickhouse"
    })
}

tasks.withType(Wrapper::class.java) {
    gradleVersion = "5.1.1"
}
