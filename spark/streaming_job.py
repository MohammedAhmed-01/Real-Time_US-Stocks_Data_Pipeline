from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    DateType,
    TimestampType
)
from pyspark.sql.functions import (
    from_json,
    col,
    to_json,
    struct,
    window,
    avg,
    sum,
    count,
    to_date
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


# ==============================
# 3. HDFS Configuration
# ==============================

HDFS_BASE_PATH = "hdfs://namenode:9000/stocks"

CLEAN_PATH = f"{HDFS_BASE_PATH}/clean"
AGGREGATED_PATH = f"{HDFS_BASE_PATH}/aggregated"

CLEAN_CHECKPOINT = f"{HDFS_BASE_PATH}/checkpoints/clean"
AGGREGATED_CHECKPOINT = f"{HDFS_BASE_PATH}/checkpoints/aggregated"
DEAD_LETTER_CHECKPOINT = f"{HDFS_BASE_PATH}/checkpoints/dead-letter"


# ==============================
# 4. Canonical Schema
# ==============================

stock_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("ticker", StringType(), True),
    StructField("date", DateType(), True),
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
    StructField("produced_at", TimestampType(), True)
])


# ==============================
# 5. Read from Kafka
# ==============================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# ==============================
# 6. Convert Kafka Value to JSON
# ==============================

parsed_df = (
    kafka_df
    .select(
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp")
    )
    .withColumn(
        "data",
        from_json(col("json_value"), stock_schema)
    )
)


# ==============================
# 7. Select Canonical Columns
# ==============================

stocks_df = parsed_df.select(
    "data.*",
    "kafka_timestamp"
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

validated_df = stocks_df.withColumn(
    "is_valid",
    valid_condition
)

valid_df = validated_df.filter(
    col("is_valid") == True
)

invalid_df = validated_df.filter(
    col("is_valid") == False
)


# ==============================
# 9. Window Aggregation
# ==============================

windowed_df = (
    valid_df
    .withWatermark("kafka_timestamp", "2 minutes")
    .groupBy(
        window(
            col("kafka_timestamp"),
            "1 minute"
        ),
        col("ticker")
    )
    .agg(
        avg("close").alias("avg_close"),
        sum("volume").alias("total_volume"),
        count("*").alias("event_count")
    )
)


# ==============================
# 10. Prepare Invalid Events
# ==============================

dead_letter_df = invalid_df.select(
    to_json(
        struct("*")
    ).alias("value")
)


# ==============================
# 11. Write Invalid Events
#    to Dead Letter Kafka Topic
# ==============================

dead_letter_query = (
    dead_letter_df.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "topic",
        "us-stocks-dead-letter"
    )
    .option(
        "checkpointLocation",
        DEAD_LETTER_CHECKPOINT
    )
    .outputMode("append")
    .start()
)


# ==============================
# 12. Write Valid Events
#     to HDFS as Parquet
# ==============================

valid_output_df = valid_df.drop(
    "is_valid",
    "kafka_timestamp"
)

valid_query = (
    valid_output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        CLEAN_PATH
    )
    .option(
        "checkpointLocation",
        CLEAN_CHECKPOINT
    )
    .partitionBy("date")
    .start()
)


# ==============================
# 13. Prepare Aggregation Output
# ==============================

windowed_output_df = windowed_df.withColumn(
    "window_date",
    to_date(col("window.start"))
)


# ==============================
# 14. Write Window Aggregation
#     to HDFS as Parquet
# ==============================

def write_aggregation_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_df = batch_df.withColumn(
        "window_date",
        to_date(col("window.start"))
    )

    (
        batch_df.write
        .mode("append")
        .partitionBy("window_date")
        .parquet(AGGREGATED_PATH)
    )


aggregation_query = (
    windowed_df.writeStream
    .foreachBatch(write_aggregation_batch)
    .option(
        "checkpointLocation",
        AGGREGATED_CHECKPOINT
    )
    .trigger(processingTime="10 seconds")
    .start()
)


# ==============================
# 15. Keep Streaming Job Running
# ==============================

aggregation_query.awaitTermination()