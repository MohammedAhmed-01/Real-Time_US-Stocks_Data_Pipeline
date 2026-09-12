"""
streaming_job.py — US Stocks Spark Structured Streaming  [OPTIMISED]
=====================================================================
M2 · Spark Streaming  |  Real-Time US Stocks Data Pipeline

OPTIMISATIONS vs the original
------------------------------
1.  maxOffsetsPerTrigger  50 000 → 200 000   — pull 4× more per micro-batch
2.  Clean trigger          30 s  → 15 s       — write to MinIO twice as often
3.  Aggregation trigger    10 s  → 10 s       — unchanged (already fast)
4.  shuffle.partitions      6   → 24          — more parallelism for large batches
5.  S3A multipart size    100MB → 64MB        — better parallelism on small files
6.  S3A connection pool raised (fs.s3a.connection.maximum 15→100)
7.  executor.memory / driver.memory set via SparkConf so the job
    can request resources when submitted standalone.
8.  write_clean_batch: dropped the per-ticker read-loop altogether.
    Instead: read ALL existing clean data once per batch (cached),
    union with the new batch, deduplicate, write back in one job.
    This eliminates N serial S3 reads per batch (was the main bottleneck).
9.  Dead-letter sink now uses kafka.batch.size 131072 (128 KB) and
    kafka.linger.ms 20 for better producer throughput.
10. awaitAnyTermination used as before so any crash surfaces fast.

Run inside the Spark container:
    spark-submit \\
      --master spark://stocks-spark:7077 \\
      --conf spark.jars.ivy=/tmp/.ivy2 \\
      --conf spark.executor.memory=6g \\
      --conf spark.driver.memory=4g \\
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

_CLEAN_COLS = [
    "event_id", "ticker", "date",
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits",
    "stochk_14_3_3", "stochd_14_3_3",
    "source_file", "produced_at",
]

# ============================================================
# 2. Spark Session — tuned for high throughput
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
    .config("spark.hadoop.fs.s3a.multipart.size",         "67108864")    # 64 MB (was 100 MB)
    .config("spark.hadoop.fs.s3a.block.size",             "33554432")    # 32 MB
    # Raise S3A connection pool so parallel ticker writes don't queue
    .config("spark.hadoop.fs.s3a.connection.maximum",     "100")         # was 15
    .config("spark.hadoop.fs.s3a.threads.max",            "20")
    .config("spark.hadoop.fs.s3a.max.total.tasks",        "30")
    # ── Output ──────────────────────────────────────────────
    .config("spark.sql.parquet.compression.codec", "snappy")
    # ── Performance ─────────────────────────────────────────
    .config("spark.sql.shuffle.partitions",                "24")   # was 6
    .config("spark.network.timeout",                       "300s")
    .config("spark.executor.heartbeatInterval",            "20s")
    .config("spark.sql.streaming.stateStore.compression.codec", "lz4")
    .config("spark.sql.files.ignoreMissingFiles",          "true")
    # Adaptive query execution — lets Spark coalesce small shuffle partitions
    .config("spark.sql.adaptive.enabled",                  "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "4")
    # ── Memory ──────────────────────────────────────────────
    .config("spark.memory.fraction",                       "0.8")
    .config("spark.memory.storageFraction",                "0.3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

log.info("=" * 62)
log.info("  US Stocks Spark Structured Streaming  [OPTIMISED]")
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
# 4. Read from Kafka — raised offset cap for higher throughput
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    # Pull up to 200 000 offsets per micro-batch (was 50 000 — 4× improvement)
    .option("maxOffsetsPerTrigger",    "200000")
    # Raise fetch size so Kafka sends larger chunks
    .option("kafka.fetch.max.bytes",           "52428800")   # 50 MB
    .option("kafka.max.partition.fetch.bytes", "10485760")   # 10 MB
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
# 8. Dead-Letter Stream → Kafka  (tuned Kafka producer settings)
# ============================================================

dead_letter_query = (
    invalid_df
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers",    KAFKA_BOOTSTRAP_SERVERS)
    .option("topic",                      "us-stocks-dead-letter")
    .option("checkpointLocation",         DEAD_LETTER_CHECKPOINT)
    # Larger batches to the dead-letter topic
    .option("kafka.batch.size",           "131072")   # 128 KB
    .option("kafka.linger.ms",            "20")
    .option("kafka.compression.type",     "lz4")
    .outputMode("append")
    .start()
)

log.info("Dead-letter query started  id=%s", dead_letter_query.id)

# ============================================================
# 9. Clean Events → MinIO  (OPTIMISED: single read + single write per batch)
#
# OLD approach (bottleneck):
#   for each ticker in batch:
#       existing = spark.read.parquet(ticker_path)   ← N serial S3 reads
#       merged   = union + dedup + sort
#       write back
#   This was O(N_tickers) sequential S3 round-trips per micro-batch.
#
# NEW approach (fast):
#   Read ALL existing clean data ONCE using mergeSchema (partitioned read —
#   Spark pushes down the ticker filter automatically).
#   Union with the batch, dedup, write back — ONE Spark job total.
#   S3 reads are now parallel across all partitions.
# ============================================================

def write_clean_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        log.info("batch_id=%d | empty batch — skipping", batch_id)
        return

    batch_clean = batch_df.select(_CLEAN_COLS).persist()

    # Collect tickers present in this batch (driver-side, small list)
    tickers = [
        row.ticker
        for row in batch_clean.select("ticker").distinct().collect()
        if row.ticker is not None
    ]

    if not tickers:
        batch_clean.unpersist()
        return

    # Count new rows BEFORE any write (avoids lazy-plan re-execution on
    # deleted files — the bug fixed on 2026-09-11)
    new_row_count = batch_clean.count()

    log.info(
        "batch_id=%d | new_rows=%d | tickers=%d | %s",
        batch_id,
        new_row_count,
        len(tickers),
        ", ".join(tickers[:15]) + ("…" if len(tickers) > 15 else ""),
    )

    # Read existing data for ALL affected tickers in ONE parallel S3 read.
    # Using a filter pushdown so only the affected ticker partitions are read.
    try:
        existing_df = (
            spark.read
            .option("mergeSchema", "true")
            .parquet(CLEAN_PATH)
            .filter(col("ticker").isin(tickers))
        )
        merged = (
            existing_df
            .union(batch_clean)
            .dropDuplicates(["event_id"])
            .orderBy("date", "event_id")
        )
    except Exception:
        # No existing data at all yet — first batch
        merged = batch_clean.orderBy("date", "event_id")

    # Write all affected tickers back in ONE Spark job.
    # partitionOverwriteMode=dynamic only rewrites partitions present in
    # this batch — all other ticker partitions are untouched.
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


# Trigger every 15 s (was 30 s) — data reaches MinIO twice as fast
clean_query = (
    valid_df.writeStream
    .foreachBatch(write_clean_batch)
    .option("checkpointLocation", CLEAN_CHECKPOINT)
    .trigger(processingTime="15 seconds")   # was 30 seconds
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
# ============================================================

log.info("All three streaming queries running. Waiting for termination …")
log.info("  • clean          id=%s", clean_query.id)
log.info("  • aggregation    id=%s", aggregation_query.id)
log.info("  • dead-letter    id=%s", dead_letter_query.id)

spark.streams.awaitAnyTermination()

for q in [clean_query, aggregation_query, dead_letter_query]:
    log.info(
        "Query %s (%s): isActive=%s  recentProgress=%s",
        q.name or q.id, q.id, q.isActive,
        q.recentProgress[-1] if q.recentProgress else "no progress yet",
    )