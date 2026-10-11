FROM eclipse-temurin:17.0.20.1_1-jdk-noble AS jdk
FROM python:3.12.15-slim-bookworm

COPY --from=jdk /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=4.1.3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm -f /tmp/spark.tgz

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark4.1-bundle_2.13/1.2.1/hudi-spark4.1-bundle_2.13-1.2.1.jar -o /opt/spark/jars/hudi-spark4.1-bundle_2.13-1.2.1.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar -o /opt/spark/jars/hadoop-aws-3.4.2.jar \
    && curl -fsSL https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar -o /opt/spark/jars/bundle-2.29.52.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.3.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar -o /opt/spark/jars/kafka-clients-3.9.1.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar -o /opt/spark/jars/commons-pool2-2.12.1.jar

COPY batch/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY deploy/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN useradd -u 1000 -m -s /bin/bash spark \
    && mkdir -p /tmp/spark-spill /opt/spark/conf \
    && chown -R spark:spark /opt/spark /app /tmp/spark-spill

WORKDIR /app
USER spark
