# spark/Dockerfile
# =============================================================================
# Spark 3.5.7 — pre-loaded JARs:
#   • spark-sql-kafka-0-10        (Kafka source/sink connector)
#   • spark-token-provider-kafka  (Kafka token provider)
#   • kafka-clients               (ByteArraySerializer etc.)
#   • commons-pool2               (REQUIRED by kafka-clients — was missing!)
#   • hadoop-aws                  (S3A file system for MinIO)
#   • aws-java-sdk-bundle         (S3A runtime dependency)
#   • postgresql                  (JDBC driver for PostgreSQL — M3 analytics)
# =============================================================================

FROM apache/spark:3.5.7

USER root

RUN mkdir -p /opt/spark/jars

# ---------------------------------------------------------------------------
# Kafka connector JARs
# ---------------------------------------------------------------------------
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar

RUN curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar

# ---------------------------------------------------------------------------
# kafka-clients — provides ByteArraySerializer and other Kafka client classes
# ---------------------------------------------------------------------------
RUN curl -L -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

# ---------------------------------------------------------------------------
# commons-pool2 — REQUIRED by kafka-clients (KafkaDataConsumer uses it)
# This was the missing JAR causing NoClassDefFoundError
# ---------------------------------------------------------------------------
RUN curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ---------------------------------------------------------------------------
# S3A file-system + AWS SDK (required to talk to MinIO)
# ---------------------------------------------------------------------------
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ---------------------------------------------------------------------------
# PostgreSQL JDBC driver — used by analytics_job.py to write insight tables
# ---------------------------------------------------------------------------
RUN curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# ---------------------------------------------------------------------------
# Drop back to the non-root Spark user (uid 185)
# ---------------------------------------------------------------------------
USER 185
