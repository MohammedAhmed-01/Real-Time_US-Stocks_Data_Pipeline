from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestKafkaConnection") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29093,kafka-3:29094") \
    .option("subscribe", "us-stocks-raw") \
    .option("startingOffsets", "latest") \
    .load()

df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value"
).writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-kafka-test-checkpoint") \
    .start() \
    .awaitTermination()