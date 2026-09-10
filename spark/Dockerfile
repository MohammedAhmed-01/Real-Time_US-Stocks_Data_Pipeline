FROM apache/spark:3.5.7

USER root

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar

RUN curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar

USER 185