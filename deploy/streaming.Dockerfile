FROM eclipse-temurin:17.0.20.1_1-jdk-noble AS jdk
FROM maven:3.10.0-eclipse-temurin-17-noble AS verify
COPY --from=jdk /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
WORKDIR /src
COPY streaming/pom.xml ./
COPY streaming/src ./src
RUN java -version 2>&1 | grep '17.0.20.1' && mvn -B verify
FROM flink:2.2.1-scala_2.12-java17
COPY --from=jdk /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=verify /src/target/authorization.jar /opt/flink/usrlib/authorization.jar
RUN mkdir -p /state/checkpoints /state/savepoints && chown -R flink:flink /state
USER flink
