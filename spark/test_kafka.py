"""
test_kafka.py — Quick connectivity test: Kafka → Spark console sink
===================================================================
Prints raw Kafka messages to the console so you can confirm the
Spark ↔ Kafka wiring works before running the full streaming job.

Run inside the Spark container:
    spark-submit \\
      --master spark://stocks-spark:7077 \\
      --conf spark.jars.ivy=/tmp/.ivy2 \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,\\
                 org.apache.hadoop:hadoop-aws:3.3.4,\\
                 com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
      /opt/spark/work-dir/test_kafka.py
"""

import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

spark = (
    SparkSession.builder
    .appName("TestKafkaConnection")
    # S3A / MinIO config (needed so the JARs initialise cleanly even in tests)
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key",  MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",  MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",
            "kafka-1:29092,kafka-2:29093,kafka-3:29094")
    .option("subscribe",        "us-stocks-raw")
    .option("startingOffsets",  "latest")
    .load()
)

(
    df.selectExpr(
        "CAST(key   AS STRING) AS key",
        "CAST(value AS STRING) AS value",
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-kafka-test-checkpoint")
    .start()
    .awaitTermination()
)
