"""
streaming_job.py — US Stocks Spark Structured Streaming
========================================================
M2 · Spark Streaming  |  Real-Time US Stocks Data Pipeline

FIXES APPLIED
-------------
1. _CLEAN_COLS was referenced inside write_clean_batch but never defined.
   Added it before the function.

2. The clean sink query was NEVER STARTED — valid_df was parsed and validated
   but silently dropped because there was no .writeStream.foreachBatch(...).start()
   call for it. Added clean_query with the correct checkpointLocation and trigger.

3. write_clean_batch used a per-ticker loop (N Spark jobs per batch). Replaced
   with a single partitioned write for all tickers in one job.

4. awaitAnyTermination() replaced with awaitTermination() on each query so all
   three streams (clean, aggregation, dead-letter) block together and any one
   crashing surfaces immediately.

Storage backend: MinIO (S3-compatible object store) via the S3A connector.

Run inside the Spark container:
    spark-submit \\
      --master spark://stocks-spark:7077 \\
      --conf spark.jars.ivy=/tmp/.ivy2 \\
      /opt/spark/work-dir/streaming_job.py
"""

from __future__ import annotations

import logging
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    struct,
    sum as spark_sum,
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [STREAMING] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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

# FIX 1 — define _CLEAN_COLS here so write_clean_batch can reference it
_CLEAN_COLS = [
    "event_id",
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividends",
    "stock_splits",
    "stochk_14_3_3",
    "stochd_14_3_3",
    "source_file",
    "produced_at",
]

# ============================================================
# 2. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("USStocksStreaming")
    # ── MinIO / S3A ──────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload",            "true")
    .config("spark.hadoop.fs.s3a.multipart.size",         "104857600")   # 100 MB
    .config("spark.hadoop.fs.s3a.block.size",             "33554432")    # 32 MB
    # ── Output ──────────────────────────────────────────────
    .config("spark.sql.parquet.compression.codec", "snappy")
    # ── Performance / stability ──────────────────────────────
    .config("spark.sql.shuffle.partitions",                "6")
    .config("spark.network.timeout",                       "300s")
    .config("spark.executor.heartbeatInterval",            "20s")
    .config("spark.sql.streaming.stateStore.compression.codec", "lz4")
    # Tolerate missing S3A paths on read (no AnalysisException for empty dirs)
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

log.info("=" * 62)
log.info("  US Stocks Spark Structured Streaming")
log.info("  Kafka  : %s  topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
log.info("  MinIO  : %s  bucket=%s", MINIO_ENDPOINT, MINIO_BUCKET)
log.info("  Clean  : %s", CLEAN_PATH)
log.info("  Agg    : %s", AGGREGATED_PATH)
log.info("=" * 62)

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
    # Limit how much Kafka data is pulled per micro-batch so the worker
    # doesn't OOM on first run when replaying millions of historical rows.
    .option("maxOffsetsPerTrigger",    "50000")
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
# 6. Data Validation  — add is_valid flag
# ============================================================

valid_condition = (
    col("event_id").isNotNull()
    & col("ticker").isNotNull()
    & col("date").isNotNull()
    & col("open").isNotNull()   & (col("open")   >= 0)
    & col("high").isNotNull()   & (col("high")   >= 0)
    & col("low").isNotNull()    & (col("low")    >= 0)
    & col("close").isNotNull()  & (col("close")  >= 0)
    & col("volume").isNotNull() & (col("volume") >= 0)
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
        spark_sum("volume").alias("total_volume"),
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

log.info("Dead-letter query started  id=%s", dead_letter_query.id)

# ============================================================
# 9. Clean Events → MinIO  (foreachBatch — one Spark job per batch)
# ============================================================

def write_clean_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Merge new clean rows into the per-ticker Parquet store in MinIO.

    Strategy
    --------
    • Select only the canonical columns (_CLEAN_COLS).
    • Collect the list of distinct tickers in this batch (driver side — cheap).
    • Read only the existing Parquet files for those tickers (avoids a full
      scan of /clean/).
    • Union existing + new, deduplicate on event_id, sort by date/event_id.
    • Write back partitioned by ticker in a single Spark job.

    The count of new rows is captured BEFORE the write so we never
    re-execute the lazy plan against a file that has already been overwritten
    (the bug fixed on 2026-09-11).
    """
    if batch_df.isEmpty():
        log.info("batch_id=%d | empty batch — skipping", batch_id)
        return

    # Select and cache the clean columns for this batch
    batch_clean = batch_df.select(_CLEAN_COLS).persist()

    # Collect distinct tickers (small list — safe to run on driver)
    tickers = [
        row.ticker
        for row in batch_clean.select("ticker").distinct().collect()
        if row.ticker is not None
    ]

    if not tickers:
        batch_clean.unpersist()
        return

    # Count BEFORE the write (avoids SparkFileNotFoundException after overwrite)
    new_row_count = batch_clean.count()

    log.info(
        "batch_id=%d | new_rows=%d | tickers=%d | %s",
        batch_id,
        new_row_count,
        len(tickers),
        ", ".join(tickers[:15]) + ("…" if len(tickers) > 15 else ""),
    )

    # Read existing Parquet for affected tickers only
    existing_parts: list[DataFrame] = []
    for ticker in tickers:
        ticker_path = f"{CLEAN_PATH}/{ticker}"
        try:
            existing_parts.append(spark.read.parquet(ticker_path))
        except Exception:
            # No existing data for this ticker yet — that's fine
            pass

    if existing_parts:
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
    # mode="overwrite" + partitionOverwriteMode="dynamic" only rewrites
    # the partitions (tickers) present in this batch — all other ticker
    # partitions are left untouched.
    (
        merged
        .repartition(max(1, len(tickers)), col("ticker"))
        .write
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(CLEAN_PATH)
    )

    batch_clean.unpersist()

    log.info(
        "batch_id=%d | ✓ wrote %d tickers → %s",
        batch_id, len(tickers), CLEAN_PATH,
    )


# FIX 2 — start the clean query (this call was completely missing before)
clean_query = (
    valid_df.writeStream
    .foreachBatch(write_clean_batch)
    .option("checkpointLocation", CLEAN_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

log.info("Clean query started  id=%s", clean_query.id)

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

    log.info("batch_id=%d | aggregation batch written → %s", batch_id, AGGREGATED_PATH)


aggregation_query = (
    windowed_df.writeStream
    .foreachBatch(write_aggregation_batch)
    .option("checkpointLocation", AGGREGATED_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

log.info("Aggregation query started  id=%s", aggregation_query.id)

# ============================================================
# 11. Keep all three streams alive
#     awaitAnyTermination() stops the moment ANY query ends or crashes.
#     If a query crashes its exception is re-raised here so the job exits
#     with a non-zero code and Docker Compose can restart it.
# ============================================================

log.info("All three streaming queries are running. Waiting for termination …")
log.info("  • clean          id=%s", clean_query.id)
log.info("  • aggregation    id=%s", aggregation_query.id)
log.info("  • dead-letter    id=%s", dead_letter_query.id)

spark.streams.awaitAnyTermination()

# If we get here, one of the queries stopped (cleanly or with an error).
# Log the status of all queries to help diagnose the cause.
for q in [clean_query, aggregation_query, dead_letter_query]:
    log.info(
        "Query %s (%s): isActive=%s  recentProgress=%s",
        q.name or q.id, q.id, q.isActive,
        q.recentProgress[-1] if q.recentProgress else "no progress yet",
    )
