"""
streaming_job.py — Spark Structured Streaming: Kafka -> Validate -> HDFS
=========================================================================
M2 · Spark  |  Real-Time US Stocks Data Pipeline

FIXES applied vs the original version:
  1. `date` is now parsed as STRING first, then converted with to_date()
     using an explicit format. The original schema declared `date` as
     DateType directly inside from_json, which requires Spark's from_json
     to auto-parse the date — this silently fails (-> null) for many
     date string formats, which then fails the `date IS NOT NULL` check
     in valid_condition and makes rows fall into the dead-letter path
     even though they are perfectly valid events. This was almost
     certainly why the HDFS clean layer was coming up empty/short.
  2. All three streaming queries (dead-letter, clean parquet, aggregation)
     are now tracked and awaited together via spark.streams.awaitAnyTermination(),
     with a graceful shutdown that stops all queries if any one of them
     fails or the process receives SIGINT/SIGTERM. Previously only
     aggregation_query.awaitTermination() was called, so a crash in
     valid_query or dead_letter_query could go unnoticed while aggregation
     kept running (or vice versa).
  3. Added defensive .option("failOnDataLoss", "false") on the Kafka read
     (recommended by Spark docs for dev/replay scenarios) and startup
     logging so it's obvious in `docker logs` whether HDFS paths were
     reachable at job start.
"""

import signal
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import (
    from_json,
    col,
    to_json,
    to_date,
    struct,
    window,
    avg,
    sum as _sum,
    count,
)

# ==============================
# 1. Create Spark Session
# ==============================

spark = (
    SparkSession.builder
    .appName("USStocksStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==============================
# 2. Kafka Configuration
# ==============================

KAFKA_BOOTSTRAP_SERVERS = (
    "kafka-1:29092,"
    "kafka-2:29093,"
    "kafka-3:29094"
)

KAFKA_TOPIC = "us-stocks-raw"
DEAD_LETTER_TOPIC = "us-stocks-dead-letter"


# ==============================
# 3. HDFS Configuration
# ==============================

HDFS_BASE_PATH = "hdfs://namenode:9000/stocks"

CLEAN_PATH = f"{HDFS_BASE_PATH}/clean"
AGGREGATED_PATH = f"{HDFS_BASE_PATH}/aggregated"

CLEAN_CHECKPOINT = f"{HDFS_BASE_PATH}/checkpoints/clean"
AGGREGATED_CHECKPOINT = f"{HDFS_BASE_PATH}/checkpoints/aggregated"
DEAD_LETTER_CHECKPOINT = f"{HDFS_BASE_PATH}/checkpoints/dead-letter"

print("=" * 70)
print("  USStocksStreaming starting")
print(f"  Kafka bootstrap : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"  Source topic    : {KAFKA_TOPIC}")
print(f"  Clean path      : {CLEAN_PATH}")
print(f"  Aggregated path : {AGGREGATED_PATH}")
print("=" * 70)

# ---------------------------------------------------------------------
# Sanity check: can we reach HDFS at all before starting the streams?
# This fails fast with a clear error instead of a query silently
# erroring out 30s later inside a background thread.
# ---------------------------------------------------------------------
try:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    # IMPORTANT: this Spark image has no core-site.xml with fs.defaultFS set,
    # so FileSystem.get(hadoop_conf) alone silently resolves to the LOCAL
    # filesystem (file:///) instead of HDFS, and any hdfs:// path throws
    # "Wrong FS: hdfs://..., expected: file:///". Passing the URI explicitly
    # forces it to resolve against the HDFS namenode instead.
    hdfs_uri = spark._jvm.java.net.URI(HDFS_BASE_PATH)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdfs_uri, hadoop_conf)
    base_path_hadoop = spark._jvm.org.apache.hadoop.fs.Path(HDFS_BASE_PATH)
    if not fs.exists(base_path_hadoop):
        fs.mkdirs(base_path_hadoop)
        print(f"  Created missing base HDFS path: {HDFS_BASE_PATH}")
    else:
        print(f"  Confirmed HDFS base path exists: {HDFS_BASE_PATH}")
except Exception as exc:
    print(f"  ERROR: could not reach HDFS at {HDFS_BASE_PATH}: {exc}")
    print("  Check that the 'namenode' service is healthy and reachable "
          "from the Spark container on the 'stocks-net' Docker network.")
    sys.exit(1)


# ==============================
# 4. Canonical Schema
#    NOTE: date is STRING here (fix #1) — converted to a real DateType
#    column explicitly after parsing, with format control.
# ==============================

stock_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("ticker", StringType(), True),
    StructField("date", StringType(), True),          # <-- was DateType
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("dividends", DoubleType(), True),
    StructField("stock_splits", DoubleType(), True),
    StructField("stochk_14_3_3", DoubleType(), True),
    StructField("stochd_14_3_3", DoubleType(), True),
    StructField("source_file", StringType(), True),
    StructField("produced_at", TimestampType(), True),
])


# ==============================
# 5. Read from Kafka
# ==============================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# ==============================
# 6. Convert Kafka Value to JSON
# ==============================

parsed_df = (
    kafka_df
    .select(
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("data", from_json(col("json_value"), stock_schema))
)


# ==============================
# 7. Select Canonical Columns + parse date properly (fix #1)
# ==============================

stocks_df = (
    parsed_df
    .select("data.*", "kafka_timestamp")
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
)


# ==============================
# 8. Data Validation
# ==============================

valid_condition = (
    col("event_id").isNotNull()
    & col("ticker").isNotNull()
    & col("date").isNotNull()
    & col("open").isNotNull()
    & col("high").isNotNull()
    & col("low").isNotNull()
    & col("close").isNotNull()
    & col("volume").isNotNull()
    & (col("open") >= 0)
    & (col("high") >= 0)
    & (col("low") >= 0)
    & (col("close") >= 0)
    & (col("volume") >= 0)
)

validated_df = stocks_df.withColumn("is_valid", valid_condition)

valid_df = validated_df.filter(col("is_valid") == True)     # noqa: E712
invalid_df = validated_df.filter(col("is_valid") == False)  # noqa: E712


# ==============================
# 9. Window Aggregation
# ==============================

windowed_df = (
    valid_df
    .withWatermark("kafka_timestamp", "2 minutes")
    .groupBy(
        window(col("kafka_timestamp"), "1 minute"),
        col("ticker"),
    )
    .agg(
        avg("close").alias("avg_close"),
        _sum("volume").alias("total_volume"),
        count("*").alias("event_count"),
    )
)


# ==============================
# 10. Prepare + Write Invalid Events -> Dead Letter Topic
# ==============================

dead_letter_df = invalid_df.select(to_json(struct("*")).alias("value"))

dead_letter_query = (
    dead_letter_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", DEAD_LETTER_TOPIC)
    .option("checkpointLocation", DEAD_LETTER_CHECKPOINT)
    .outputMode("append")
    .queryName("dead_letter_writer")
    .start()
)


# ==============================
# 11. Write Valid Events -> HDFS Parquet (clean layer)
# ==============================

valid_output_df = valid_df.drop("is_valid", "kafka_timestamp")

valid_query = (
    valid_output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", CLEAN_PATH)
    .option("checkpointLocation", CLEAN_CHECKPOINT)
    .partitionBy("date")
    .queryName("clean_parquet_writer")
    .start()
)


# ==============================
# 12. Write Windowed Aggregation -> HDFS Parquet (aggregated layer)
# ==============================

def write_aggregation_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    out_df = batch_df.withColumn("window_date", to_date(col("window.start")))
    (
        out_df.write
        .mode("append")
        .partitionBy("window_date")
        .parquet(AGGREGATED_PATH)
    )
    print(f"  [batch {batch_id}] wrote {out_df.count()} aggregated rows to {AGGREGATED_PATH}")


aggregation_query = (
    windowed_df.writeStream
    .foreachBatch(write_aggregation_batch)
    .option("checkpointLocation", AGGREGATED_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .queryName("aggregation_writer")
    .start()
)


# ==============================
# 13. Track all queries + graceful shutdown (fix #2)
# ==============================

all_queries = [dead_letter_query, valid_query, aggregation_query]


def _shutdown(signum=None, _frame=None):
    print(f"\nReceived shutdown signal ({signum}) — stopping all streaming queries …")
    for q in all_queries:
        try:
            if q.isActive:
                q.stop()
        except Exception as exc:
            print(f"  Error stopping query {q.name}: {exc}")
    print("All queries stopped. Exiting.")
    sys.exit(0)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

print("All three streaming queries started:")
for q in all_queries:
    print(f"  - {q.name}  (id={q.id})")
print("Waiting on any query to terminate (Ctrl+C to stop all) …")

try:
    # awaitAnyTermination blocks until ONE query stops (success or failure).
    # If that happens, we stop the rest too instead of leaving orphans.
    spark.streams.awaitAnyTermination()
finally:
    for q in all_queries:
        try:
            if q.isActive:
                print(f"Stopping remaining active query: {q.name}")
                q.stop()
        except Exception as exc:
            print(f"  Error stopping query {q.name}: {exc}")
