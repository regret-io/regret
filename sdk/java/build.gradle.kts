import com.google.protobuf.gradle.id

plugins {
    `java-library`
    `maven-publish`
    id("com.google.protobuf") version "0.9.4"
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}

val grpcVersion = "1.72.0"
val protobufVersion = "4.31.1"
val otelVersion = "1.44.1"

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    // OpenTelemetry metrics + Prometheus exporter (pilot scrapes /metrics)
    implementation("io.opentelemetry:opentelemetry-api:$otelVersion")
    implementation("io.opentelemetry:opentelemetry-sdk:$otelVersion")
    implementation("io.opentelemetry:opentelemetry-sdk-metrics:$otelVersion")
    implementation("io.opentelemetry:opentelemetry-exporter-prometheus:$otelVersion-alpha")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            groupId = "io.regret"
            artifactId = "regret-adapter-sdk-java"
            from(components["java"])
        }
    }
    repositories {
        mavenLocal()
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/regret-io/regret")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String? ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.key") as String? ?: ""
            }
        }
    }
}
