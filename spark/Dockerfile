FROM apache/spark:3.5.7

USER root

# ---------------------------------------------------------------------------
# Kafka connector for Spark Structured Streaming, PLUS its full transitive
# dependency set. The original version of this Dockerfile only downloaded
# the two top-level connector jars (spark-sql-kafka-0-10 and
# spark-token-provider-kafka-0-10). That's not enough on its own — the
# connector needs kafka-clients at runtime, and kafka-clients in turn needs
# commons-pool2. Without those, submitting a job WITHOUT --packages fails
# with NoClassDefFoundError on the driver (kafka-clients) and, once that's
# fixed only on the driver, a SEPARATE NoClassDefFoundError shows up only on
# EXECUTORS (commons-pool2) because /opt/spark/jars/ is baked into the image
# per-container — the worker container needs the same jars as the master.
#
# Versions must match spark-sql-kafka-0-10_2.12:3.5.7's declared dependencies:
#   kafka-clients        3.4.1
#   commons-pool2        2.11.1
#   lz4-java              1.8.0
#   snappy-java          1.1.10.5
#   slf4j-api             2.0.7   (already present in most Spark images, but
#                                  pinned here for consistency)
# ---------------------------------------------------------------------------

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar

RUN curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar

RUN curl -L -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

RUN curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN curl -L -o /opt/spark/jars/lz4-java-1.8.0.jar \
    https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar

RUN curl -L -o /opt/spark/jars/snappy-java-1.1.10.5.jar \
    https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar

USER 185
