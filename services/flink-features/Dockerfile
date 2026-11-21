FROM maven:3.9.9-eclipse-temurin-17@sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc AS build
WORKDIR /build
COPY services/flink-features/pom.xml ./
COPY services/flink-features/src ./src
RUN --mount=type=cache,id=quantos-maven,target=/root/.m2 mvn -B package
FROM flink:2.2.1-scala_2.12-java17@sha256:15e8901a97a3703ad351f8212166798e36dd411c100b869e30c7c5e6b64351b0
COPY --from=build /build/target/site/jacoco /opt/coverage
COPY --from=build /build/target/market-features-0.1.0.jar /opt/flink/usrlib/market-features.jar
# S3 filesystem plugin is bundled by Flink; checkpoints use the dedicated MinIO prefix.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
