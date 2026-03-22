FROM gradle:8.12-jdk21 AS builder
WORKDIR /build
COPY settings.gradle.kts build.gradle.kts ./
COPY sdk/java/ sdk/java/
COPY adapters/oxia-java/ adapters/oxia-java/
RUN gradle :adapters:oxia-java:shadowJar --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /build/adapters/oxia-java/build/libs/*-all.jar /app/adapter.jar
EXPOSE 9090
CMD ["java", "-jar", "/app/adapter.jar"]
