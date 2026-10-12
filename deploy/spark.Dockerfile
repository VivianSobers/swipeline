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
ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN printf '%s\n' 'pyspark==4.1.3 --hash=sha256:b600238e771e45618a58beaf9d63cfd9aa06f61ce193244cedf890a04a4409f7' > /tmp/requirements-pyspark.txt \
    && pip install --no-cache-dir --no-deps --require-hashes -r /tmp/requirements-pyspark.txt \
    && rm -f /tmp/requirements-pyspark.txt

RUN curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark4.1-bundle_2.13/1.2.1/hudi-spark4.1-bundle_2.13-1.2.1.jar -o "${SPARK_HOME}/jars/hudi-spark4.1-bundle_2.13-1.2.1.jar" \
    && echo "90d03303ca994bdae82f91136d8c149e8df26b68  ${SPARK_HOME}/jars/hudi-spark4.1-bundle_2.13-1.2.1.jar" | sha1sum -c \
    && curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar -o "${SPARK_HOME}/jars/hadoop-aws-3.4.2.jar" \
    && echo "16f9de6da5c7862241d55a13d34843404d0cbb88  ${SPARK_HOME}/jars/hadoop-aws-3.4.2.jar" | sha1sum -c \
    && curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar -o "${SPARK_HOME}/jars/bundle-2.29.52.jar" \
    && echo "b63eb928c2ac13bde07746bcfe7fb38bbdb4b5c5  ${SPARK_HOME}/jars/bundle-2.29.52.jar" | sha1sum -c \
    && curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar -o "${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.13-4.1.3.jar" \
    && echo "fa6f5869ac9f8f183e1941e0a8faae6257fd1ca2  ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.13-4.1.3.jar" | sha1sum -c \
    && curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar -o "${SPARK_HOME}/jars/kafka-clients-3.9.1.jar" \
    && echo "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9  ${SPARK_HOME}/jars/kafka-clients-3.9.1.jar" | sha256sum -c \
    && curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar -o "${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.13-4.1.3.jar" \
    && echo "3903171bdc843d9560916a91ada4ecf9d10db1e7  ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.13-4.1.3.jar" | sha1sum -c \
    && curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar -o "${SPARK_HOME}/jars/commons-pool2-2.12.1.jar" \
    && echo "e5c7b6c9f5a4b59be2bba213fbec70b413376fc1  ${SPARK_HOME}/jars/commons-pool2-2.12.1.jar" | sha1sum -c

COPY batch/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY batch /app/batch

RUN mkdir -p "${SPARK_HOME}/conf"
COPY deploy/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

RUN useradd -u 1000 -m -s /bin/bash spark \
    && mkdir -p /tmp/spark-spill /opt/spark \
    && ln -s ${SPARK_HOME}/jars /opt/spark/jars \
    && ln -s ${SPARK_HOME}/conf /opt/spark/conf \
    && chown -R spark:spark ${SPARK_HOME} /app /tmp/spark-spill /opt/spark

WORKDIR /app
USER spark

CMD ["python", "-m", "batch.worker", "--poll", "60s"]
