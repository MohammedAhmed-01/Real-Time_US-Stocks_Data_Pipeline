"""
streaming_job.py — US Stocks Spark Structured Streaming
========================================================
M2 · Spark Streaming  |  Real-Time US Stocks Data Pipeline

Storage backend: MinIO (S3-compatible object store) via the S3A connector.

All HDFS paths have been replaced with s3a:// URIs.  MinIO credentials and
endpoint are injected through SparkSession Hadoop configuration so no AWS
SDK credentials file is needed inside the container.

Paths:
    s3a://stocks/clean/          — validated events, partitioned by date
    s3a://stocks/aggregated/     — 1-minute windowed aggregations
    s3a://stocks/checkpoints/    — streaming state / fault-recovery

Usage (inside the Spark container):
    spark-submit \\
      --master spark://stocks-spark:7077 \\
      --conf spark.jars.ivy=/tmp/.ivy2 \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,\\
                 org.apache.hadoop:hadoop-aws:3.3.4,\\
                 com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
      /opt/spark/work-dir/streaming_job.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    struct,
    sum,
    to_date,
    to_json,
    window,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================
# 1. Configuration
# ============================================================

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
KAFKA_TOPIC             = "us-stocks-raw"

# --- MinIO / S3A ---
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "stocks")

S3_BASE              = f"s3a://{MINIO_BUCKET}"
CLEAN_PATH           = f"{S3_BASE}/clean"
AGGREGATED_PATH      = f"{S3_BASE}/aggregated"
CLEAN_CHECKPOINT     = f"{S3_BASE}/checkpoints/clean"
AGGREGATED_CHECKPOINT= f"{S3_BASE}/checkpoints/aggregated"
DEAD_LETTER_CHECKPOINT = f"{S3_BASE}/checkpoints/dead-letter"

# ============================================================
# 2. Spark Session — with S3A / MinIO Hadoop configuration
# ============================================================

spark = (
    SparkSession.builder
    .appName("USStocksStreaming")
    # S3A implementation
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # MinIO endpoint (path-style required for MinIO)
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # Credentials
    .config("spark.hadoop.fs.s3a.access.key",  MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",  MINIO_SECRET_KEY)
    # Performance / reliability tweaks for MinIO
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   "false")
    .config("spark.hadoop.fs.s3a.fast.upload",              "true")
    .config("spark.hadoop.fs.s3a.multipart.size",           "104857600")  # 100 MB
    .config("spark.hadoop.fs.s3a.block.size",               "33554432")   # 32 MB
    .config("spark.hadoop.fs.s3a.committer.name",           "directory")
    .config("spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
    .config("spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
    # Parquet compression
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 3. Canonical Event Schema
# ============================================================

stock_schema = StructType([
    StructField("event_id",      LongType(),      True),
    StructField("ticker",        StringType(),    True),
    StructField("date",          DateType(),      True),
    StructField("open",          DoubleType(),    True),
    StructField("high",          DoubleType(),    True),
    StructField("low",           DoubleType(),    True),
    StructField("close",         DoubleType(),    True),
    StructField("volume",        DoubleType(),    True),
    StructField("dividends",     DoubleType(),    True),
    StructField("stock_splits",  DoubleType(),    True),
    StructField("stochk_14_3_3", DoubleType(),    True),
    StructField("stochd_14_3_3", DoubleType(),    True),
    StructField("source_file",   StringType(),    True),
    StructField("produced_at",   TimestampType(), True),
])

# ============================================================
# 4. Read from Kafka
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .load()
)

# ============================================================
# 5. Parse JSON payload → typed columns
# ============================================================

parsed_df = (
    kafka_df
    .select(
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("data", from_json(col("json_value"), stock_schema))
)

stocks_df = parsed_df.select("data.*", "kafka_timestamp")

# ============================================================
# 6. Data Validation
# ============================================================

valid_condition = (
    col("event_id").isNotNull()
    & col("ticker").isNotNull()
    & col("date").isNotNull()
    & col("open").isNotNull()
    & col("high").isNotNull()
    & col("low").isNotNull()
    & col("close").isNotNull()
    & col("volume").isNotNull()
    & (col("open")   >= 0)
    & (col("high")   >= 0)
    & (col("low")    >= 0)
    & (col("close")  >= 0)
    & (col("volume") >= 0)
)

validated_df = stocks_df.withColumn("is_valid", valid_condition)
valid_df     = validated_df.filter(col("is_valid"))
invalid_df   = validated_df.filter(~col("is_valid"))

# ============================================================
# 7. 1-Minute Windowed Aggregation
# ============================================================

windowed_df = (
    valid_df
    .withWatermark("kafka_timestamp", "2 minutes")
    .groupBy(
        window(col("kafka_timestamp"), "1 minute"),
        col("ticker"),
    )
    .agg(
        avg("close").alias("avg_close"),
        sum("volume").alias("total_volume"),
        count("*").alias("event_count"),
    )
)

# ============================================================
# 8. Dead-Letter Stream → Kafka topic
# ============================================================

dead_letter_query = (
    invalid_df
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic",                   "us-stocks-dead-letter")
    .option("checkpointLocation",      DEAD_LETTER_CHECKPOINT)
    .outputMode("append")
    .start()
)

# ============================================================
# 9. Clean Events → MinIO (Parquet, partitioned by date)
# ============================================================

valid_output_df = valid_df.drop("is_valid", "kafka_timestamp")

valid_query = (
    valid_output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path",              CLEAN_PATH)
    .option("checkpointLocation", CLEAN_CHECKPOINT)
    .partitionBy("date")
    .start()
)

# ============================================================
# 10. Windowed Aggregation → MinIO (Parquet, partitioned by window_date)
# ============================================================

def write_aggregation_batch(batch_df, batch_id: int) -> None:
    """Write one micro-batch of aggregated data to MinIO as Parquet."""
    if batch_df.isEmpty():
        return

    (
        batch_df
        .withColumn("window_date", to_date(col("window.start")))
        .write
        .mode("append")
        .partitionBy("window_date")
        .parquet(AGGREGATED_PATH)
    )


aggregation_query = (
    windowed_df.writeStream
    .foreachBatch(write_aggregation_batch)
    .option("checkpointLocation", AGGREGATED_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

# ============================================================
# 11. Keep the job alive
# ============================================================

aggregation_query.awaitTermination()
