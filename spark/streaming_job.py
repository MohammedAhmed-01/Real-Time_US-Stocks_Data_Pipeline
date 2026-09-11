"""
streaming_job.py — US Stocks Spark Structured Streaming
========================================================
M2 · Spark Streaming  |  Real-Time US Stocks Data Pipeline

BUG FIX (2026-09-11)
---------------------
Removed merged.count() call that appeared AFTER .write.mode("overwrite").
The fix: count new_rows BEFORE the write instead.

Root cause: merged is a lazy DataFrame that still references the OLD
parquet file in its query plan.  After .write.mode("overwrite") deletes
that file and replaces it, calling merged.count() re-executes the plan
against the now-deleted file, producing:

    SparkFileNotFoundException: No such file or directory:
      s3a://stocks/clean/AA/part-00000-<uuid>.snappy.parquet

Storage backend: MinIO (S3-compatible object store) via the S3A connector.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
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

log = logging.getLogger(__name__)

# ============================================================
# 1. Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
KAFKA_TOPIC             = "us-stocks-raw"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "stocks")

S3_BASE                = f"s3a://{MINIO_BUCKET}"
CLEAN_PATH             = f"{S3_BASE}/clean"
AGGREGATED_PATH        = f"{S3_BASE}/aggregated"
CLEAN_CHECKPOINT       = f"{S3_BASE}/checkpoints/clean"
AGGREGATED_CHECKPOINT  = f"{S3_BASE}/checkpoints/aggregated"
DEAD_LETTER_CHECKPOINT = f"{S3_BASE}/checkpoints/dead-letter"

# ============================================================
# 2. Spark Session  — add these configs
# ============================================================

spark = (
    SparkSession.builder
    .appName("USStocksStreaming")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key",  MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",  MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   "false")
    .config("spark.hadoop.fs.s3a.fast.upload",              "true")
    .config("spark.hadoop.fs.s3a.multipart.size",           "104857600")
    .config("spark.hadoop.fs.s3a.block.size",               "33554432")
    .config("spark.sql.parquet.compression.codec", "snappy")
    # ── FIXES ──────────────────────────────────────────────────────────────
    .config("spark.sql.shuffle.partitions",     "6")   # match partition count; default 200 is lethal
    .config("spark.network.timeout",            "300s")  # was 120s default — executor had 60s to respond
    .config("spark.executor.heartbeatInterval", "20s")   # more frequent than the 60s timeout window
    .config("spark.sql.streaming.stateStore.compression.codec", "lz4")
    .getOrCreate()
)
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
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    .load()
)

# ============================================================
# 5. Parse JSON payload
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
valid_df     = validated_df.filter( col("is_valid"))
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
# 9. Clean Events → MinIO  — rewrite foreachBatch to one job per batch
# ============================================================

def write_clean_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write ALL tickers in a single batch with one Spark job, not one per ticker.

    OLD approach: loop over tickers → collect → filter → read existing →
      union → dedup → write  (N Spark jobs for N tickers per batch — very slow)

    NEW approach:
      1. Persist the incoming batch once.
      2. Read ALL existing clean data that overlaps with tickers in this batch.
      3. Union, dedup, sort — one Spark job.
      4. Write partitioned by ticker — one write, one Spark job.
    """
    if batch_df.isEmpty():
        return

    batch_clean = batch_df.select(_CLEAN_COLS).persist()

    tickers = [
        row.ticker
        for row in batch_clean.select("ticker").distinct().collect()
        if row.ticker is not None
    ]

    if not tickers:
        batch_clean.unpersist()
        return

    log.info(
        "batch_id=%d | tickers in batch: %d | %s",
        batch_id, len(tickers),
        ", ".join(tickers[:10]) + ("…" if len(tickers) > 10 else ""),
    )

    # Read only the affected ticker paths (avoids scanning all of /clean/)
    existing_parts = []
    for ticker in tickers:
        try:
            existing_parts.append(spark.read.parquet(f"{CLEAN_PATH}/{ticker}"))
        except Exception:
            pass  # ticker has no existing data yet

    if existing_parts:
        from functools import reduce
        existing = reduce(DataFrame.union, existing_parts)
        merged = (
            existing
            .union(batch_clean)
            .dropDuplicates(["event_id"])
            .orderBy("date", "event_id")
        )
    else:
        merged = batch_clean.orderBy("date", "event_id")

    # Write all tickers in ONE Spark job, partitioned by ticker
    (
        merged
        .repartition(len(tickers), col("ticker"))
        .write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(CLEAN_PATH)
    )

    batch_clean.unpersist()
    log.info("batch_id=%d | wrote %d tickers to %s", batch_id, len(tickers), CLEAN_PATH)

# ============================================================
# 10. Windowed Aggregation → MinIO
# ============================================================

def write_aggregation_batch(batch_df: DataFrame, batch_id: int) -> None:
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

spark.streams.awaitAnyTermination()
