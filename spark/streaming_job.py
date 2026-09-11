"""
streaming_job.py — US Stocks Spark Structured Streaming
========================================================
M2 · Spark Streaming  |  Real-Time US Stocks Data Pipeline

Storage backend: MinIO (S3-compatible object store) via the S3A connector.

CHANGE vs previous version
---------------------------
Clean events are now written as ONE Parquet file per ticker symbol instead
of many scattered row-level files.

Layout (MinIO bucket "stocks"):
    s3a://stocks/clean/<TICKER>/data.parquet   ← one merged file per stock
    s3a://stocks/aggregated/                   ← 1-minute windowed aggs
    s3a://stocks/checkpoints/                  ← streaming state

How it works (write_clean_batch):
    For every micro-batch that arrives from Kafka:
      1. Find all distinct tickers present in the batch.
      2. For each ticker:
           a. Read the existing Parquet file for that ticker (if any).
           b. Union existing data with the new batch rows.
           c. Write back as a single coalesced file (overwrite).
    Result: every ticker always has exactly one up-to-date Parquet file
    that contains ALL its historical rows received so far.

Usage (inside the Spark container):
    spark-submit \\
      --master spark://stocks-spark:7077 \\
      --conf spark.jars.ivy=/tmp/.ivy2 \\
      /opt/spark/work-dir/streaming_job.py
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

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
KAFKA_TOPIC             = "us-stocks-raw"

# --- MinIO / S3A ---
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "stocks")

S3_BASE                = f"s3a://{MINIO_BUCKET}"
CLEAN_PATH             = f"{S3_BASE}/clean"          # one sub-dir per ticker
AGGREGATED_PATH        = f"{S3_BASE}/aggregated"
CLEAN_CHECKPOINT       = f"{S3_BASE}/checkpoints/clean"
AGGREGATED_CHECKPOINT  = f"{S3_BASE}/checkpoints/aggregated"
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
    .option("startingOffsets",         "earliest")   # read ALL existing data
    .option("failOnDataLoss",          "false")
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
# 8. Dead-Letter Stream → Kafka topic  (unchanged)
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
# 9. Clean Events → MinIO  (ONE Parquet file per ticker)
# ============================================================
#
# MinIO layout after this runs:
#
#   stocks/
#   └── clean/
#       ├── AAPL/
#       │   └── part-00000-<uuid>.snappy.parquet   ← ALL AAPL rows
#       ├── MSFT/
#       │   └── part-00000-<uuid>.snappy.parquet   ← ALL MSFT rows
#       ├── TSLA/
#       │   └── part-00000-<uuid>.snappy.parquet
#       └── ...
#
# Strategy (read-union-overwrite per ticker):
#   For each micro-batch we find every distinct ticker in the batch,
#   read its existing Parquet file from MinIO (if one exists), union
#   the new rows with the old ones, sort by date for readability, then
#   write it back as a single coalesced file.  The "overwrite" mode
#   atomically replaces the old file, so readers always see a complete
#   consistent snapshot.
#
# Trade-off: read-modify-write per ticker per batch means one Spark
#   read + one Spark write for every ticker that appears in the batch.
#   This is fine for a dataset of ~6 000 tickers where each batch
#   usually touches a small subset.  For very high-throughput scenarios
#   consider a periodic compaction job instead.

# Columns to keep in the clean file (drop streaming-internal columns)
_CLEAN_COLS = [
    "event_id", "ticker", "date",
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits",
    "stochk_14_3_3", "stochd_14_3_3",
    "source_file", "produced_at",
]


def write_clean_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Merge incoming batch rows into one Parquet file per ticker in MinIO.

    Called automatically by Spark's foreachBatch sink for every
    micro-batch.  If the batch is empty (e.g. during idle periods)
    the function returns immediately without touching MinIO.
    """
    if batch_df.isEmpty():
        return

    # Keep only the canonical columns; drop is_valid / kafka_timestamp
    batch_clean = batch_df.select(_CLEAN_COLS)

    # Collect the distinct tickers present in this batch.
    # collect() is safe here because it is just a list of ticker strings,
    # not the full data — at most a few thousand short strings.
    tickers: list[str] = [
        row.ticker
        for row in batch_clean.select("ticker").distinct().collect()
        if row.ticker is not None
    ]

    log.info(
        "batch_id=%d | tickers in batch: %d | %s",
        batch_id, len(tickers),
        ", ".join(tickers[:10]) + ("…" if len(tickers) > 10 else ""),
    )

    for ticker in tickers:
        ticker_path = f"{CLEAN_PATH}/{ticker}"

        # Rows for this ticker from the current micro-batch
        new_rows = batch_clean.filter(col("ticker") == ticker)

        # Read existing file for this ticker (if it already exists in MinIO)
        try:
            existing = spark.read.parquet(ticker_path)
            # Union old + new, deduplicate on event_id to be safe
            merged = (
                existing
                .union(new_rows)
                .dropDuplicates(["event_id"])
                .orderBy("date", "event_id")
            )
        except Exception:
            # No existing file yet — first time we see this ticker
            merged = new_rows.orderBy("date", "event_id")

        # Write as a SINGLE Parquet file (coalesce → 1 output partition)
        # mode("overwrite") atomically replaces the previous file
        (
            merged
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(ticker_path)
        )

        log.info(
            "batch_id=%d | ticker=%-6s | rows=%d | path=%s",
            batch_id, ticker, merged.count(), ticker_path,
        )


# Wire up the foreachBatch sink
valid_query = (
    valid_df
    .writeStream
    .foreachBatch(write_clean_batch)
    .option("checkpointLocation", CLEAN_CHECKPOINT)
    .trigger(processingTime="30 seconds")   # tune to taste
    .start()
)

# ============================================================
# 10. Windowed Aggregation → MinIO  (unchanged)
# ============================================================

def write_aggregation_batch(batch_df: DataFrame, batch_id: int) -> None:
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
# 11. Keep the job alive — wait for ALL queries
# ============================================================

# Block until the first query fails or is stopped manually.
# This keeps the driver alive so all three queries keep running.
spark.streams.awaitAnyTermination()
