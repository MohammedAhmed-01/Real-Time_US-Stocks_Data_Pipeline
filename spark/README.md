# ⚡ Real-Time US Stocks Data Pipeline — Spark Streaming (MinIO Edition)

> **Apache Spark 3.5.7 · Apache Kafka · MinIO (S3-compatible) · Parquet · Docker**

This module implements the **real-time Spark Structured Streaming layer** of the US Stocks Data Pipeline. It reads stock market events from Kafka, validates them against a canonical schema, routes invalid records to a Dead Letter Topic, merges valid records into per-ticker Parquet files in **MinIO**, and performs **1-minute windowed aggregations** — all fully containerised.

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Prerequisites](#-prerequisites)
- [Environment Variables](#-environment-variables)
- [MinIO Object Store](#-minio-object-store)
- [Spark Configuration & JARs](#-spark-configuration--jars)
- [Kafka Input](#-kafka-input)
- [Streaming Job Logic (step-by-step)](#-streaming-job-logic-step-by-step)
- [Data Validation Rules](#-data-validation-rules)
- [MinIO Storage Layout](#-minio-storage-layout)
- [Checkpointing & Fault Recovery](#-checkpointing--fault-recovery)
- [Running the Full Pipeline](#-running-the-full-pipeline)
- [Submitting the Spark Job](#-submitting-the-spark-job)
- [Web UIs & Localhost Ports](#-web-uis--localhost-ports)
- [Reading & Verifying Output](#-reading--verifying-output)
- [Useful MinIO Commands](#-useful-minio-commands)
- [Known Bug Fix (2026-09-11)](#-known-bug-fix-2026-09-11)
- [Handover to M3](#-handover-to-m3)
- [Definition of Done](#-definition-of-done)

---

## 🔎 Overview

```
Kafka (us-stocks-raw)
    │
    ▼
Spark Structured Streaming
    │
    ├── Valid events   ──► MinIO  s3a://stocks/clean          (one Parquet per ticker)
    │                  ──► MinIO  s3a://stocks/aggregated     (1-min windowed stats)
    │
    └── Invalid events ──► Kafka  us-stocks-dead-letter
```

Every message on `us-stocks-raw` is a JSON event with 14 fields (OHLCV, stochastics, metadata). The Spark job parses, validates, deduplicates, and persists them with no HDFS dependency — MinIO replaces HDFS entirely.

---

## 🏗️ Architecture

```
                    ┌──────────────────────┐
                    │   Kafka Producer     │
                    │  (us-stocks-raw)     │
                    └──────────┬───────────┘
                               │  JSON events, keyed by ticker
                               ▼
              ┌─────────────────────────────────────┐
              │     Spark Structured Streaming       │
              │                                     │
              │  Step 4 – readStream from Kafka      │
              │  Step 5 – from_json → StructType     │
              │  Step 6 – validate (is_valid flag)   │
              │  Step 7 – 1-min window aggregation   │
              └───────────────┬─────────────────────┘
                              │
           ┌──────────────────┼──────────────────┐
           │                  │                  │
           ▼                  ▼                  ▼
  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐
  │  Dead-Letter │  │  Clean Parquet   │  │  Windowed Parquet    │
  │  Kafka topic │  │  (per-ticker)    │  │  (1-min tumbling)    │
  └──────────────┘  └────────┬─────────┘  └──────────┬───────────┘
                             │                        │
                             ▼                        ▼
                   ┌──────────────────────────────────────┐
                   │   MinIO  (s3a://stocks/...)           │
                   │   • /clean/<TICKER>/part-*.parquet    │
                   │   • /aggregated/window_date=.../      │
                   │   • /checkpoints/clean/               │
                   │   • /checkpoints/aggregated/          │
                   │   • /checkpoints/dead-letter/         │
                   └──────────────────────────────────────┘
```

---

## 🧰 Technology Stack

| Technology | Version | Role |
|---|---|---|
| Apache Spark | 3.5.7 | Stream processing engine |
| PySpark | 3.5.7 | Streaming job implementation |
| Apache Kafka | 7.6.1 (Confluent) | Event ingestion source |
| MinIO | latest | S3-compatible object store (replaces HDFS) |
| Hadoop S3A | 3.3.4 | Spark ↔ MinIO file-system connector |
| AWS Java SDK | 1.12.262 | S3A runtime dependency |
| Parquet (Snappy) | — | Columnar storage format |
| Docker / Compose | v24+ | Container orchestration |

---

## 🧰 Prerequisites

| Requirement | Detail |
|---|---|
| Docker Desktop | v24+ — allocate **at least 8 GB RAM** in Settings → Resources |
| Docker Compose | Bundled with Docker Desktop |
| Kaggle account + API token | Needed by the M1 producer; get one at https://www.kaggle.com/settings |
| Free host ports | See the full list in [Web UIs & Localhost Ports](#-web-uis--localhost-ports) |

No local Python or Java installation is required — everything runs inside containers.

---

## 🔧 Environment Variables

Copy `spark/env.example` to `.env` in the **repo root** (the same directory as `docker-compose.yml`) and fill in your values:

```bash
cp spark/env.example .env   # or copy env.example .env from the repo root
```

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAGGLE_USERNAME` | **Yes** | — | Your Kaggle username |
| `KAGGLE_KEY` | **Yes** | — | Your Kaggle API key |
| `KAFKA_BOOTSTRAP` | No | `localhost:9092,...` | Broker addresses |
| `KAFKA_TOPIC` | No | `us-stocks-raw` | Primary ingest topic |
| `KAFKA_DLT_TOPIC` | No | `us-stocks-dead-letter` | Dead-letter topic |
| `KAFKA_GROUP_ID` | No | `m1-validation-group` | M1 consumer group |
| `MINIO_ACCESS_KEY` | No | `minioadmin` | MinIO root user |
| `MINIO_SECRET_KEY` | No | `minioadmin` | MinIO root password |
| `MINIO_BUCKET` | No | `stocks` | Bucket for all Parquet and checkpoints |

> **Never commit `.env`** — it contains secrets. Only `env.example` should be tracked in Git.

---

## 🪣 MinIO Object Store

MinIO is the S3-compatible object store that completely replaces HDFS in this project. Spark writes Parquet files and checkpoints directly to MinIO via the S3A connector.

### Container Details

| Setting | Value |
|---|---|
| Container name | `stocks-minio` |
| Image | `minio/minio:latest` |
| S3 API (used by Spark) | `http://minio:9000` (internal Docker) / `http://localhost:9000` (host) |
| Web console | `http://localhost:9001` |
| Default credentials | user `minioadmin` / password `minioadmin` |
| Persistent volume | `minio-data` (named Docker volume — data survives container restarts) |
| Bucket | `stocks` (created automatically by `minio-init` on first start) |

### How `minio-init` works

A one-shot service (`stocks-minio-init`) runs the `mc` CLI after MinIO becomes healthy:

```bash
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb --ignore-existing local/stocks
```

This creates the `stocks` bucket if it does not already exist, then exits. You never need to create the bucket manually.

---

## ⚙️ Spark Configuration & JARs

### Spark Session (from `streaming_job.py`)

```python
spark = (
    SparkSession.builder
    .appName("USStocksStreaming")
    # S3A / MinIO
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload",            "true")
    .config("spark.hadoop.fs.s3a.multipart.size",         "104857600")  # 100 MB
    .config("spark.hadoop.fs.s3a.block.size",             "33554432")   # 32 MB
    # Output
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)
```

### Pre-baked JARs (inside `spark/Dockerfile`)

All required JARs are downloaded into the image at build time — no `--packages` flag is needed at `spark-submit`:

| JAR | Version | Purpose |
|---|---|---|
| `spark-sql-kafka-0-10_2.12` | 3.5.7 | Kafka source/sink connector |
| `spark-token-provider-kafka-0-10_2.12` | 3.5.7 | Kafka token provider |
| `kafka-clients` | 3.4.1 | Low-level Kafka client classes |
| `commons-pool2` | 2.11.1 | Required by `kafka-clients` (was missing, now fixed) |
| `hadoop-aws` | 3.3.4 | S3A file-system implementation |
| `aws-java-sdk-bundle` | 1.12.262 | AWS/S3 SDK runtime dependency |

---

## 📨 Kafka Input

| Setting | Value |
|---|---|
| Topic | `us-stocks-raw` |
| Brokers (inside Docker) | `kafka-1:29092,kafka-2:29093,kafka-3:29094` |
| Brokers (host machine) | `localhost:9092,localhost:9093,localhost:9094` |
| Partitions | 6 |
| Replication factor | 3 |
| Starting offset | `earliest` (re-reads full history on a clean start) |
| `failOnDataLoss` | `false` (tolerates offset gaps without crashing) |

---

## 🔄 Streaming Job Logic (step-by-step)

The job is implemented in `spark/streaming_job.py`. Here is a detailed walkthrough of each step.

### Step 1 — Configuration constants

```python
KAFKA_BOOTSTRAP_SERVERS = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
KAFKA_TOPIC             = "us-stocks-raw"

S3_BASE                = "s3a://stocks"
CLEAN_PATH             = "s3a://stocks/clean"
AGGREGATED_PATH        = "s3a://stocks/aggregated"
CLEAN_CHECKPOINT       = "s3a://stocks/checkpoints/clean"
AGGREGATED_CHECKPOINT  = "s3a://stocks/checkpoints/aggregated"
DEAD_LETTER_CHECKPOINT = "s3a://stocks/checkpoints/dead-letter"
```

All S3A paths resolve to the `stocks` bucket in MinIO. Checkpoints are stored inside the same bucket — no external state store is required.

---

### Step 2 — Build the SparkSession

The session configures the S3A connector so every read/write call transparently uses MinIO. `path.style.access = true` is required because MinIO uses path-style URLs, not virtual-hosted-style.

---

### Step 3 — Define the canonical `StructType`

```python
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
```

`stochk_14_3_3` and `stochd_14_3_3` are nullable because the stochastic indicator requires a ~14-row warm-up window; the first rows per ticker will have `null` values for these fields.

---

### Step 4 — Read the Kafka stream

```python
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    .load()
)
```

Raw Kafka columns: `key` (binary), `value` (binary), `topic`, `partition`, `offset`, `timestamp` (Kafka broker ingest time), `timestampType`.

---

### Step 5 — Parse JSON payload

```python
parsed_df = (
    kafka_df
    .select(
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp"),   # keep Kafka ingest time
    )
    .withColumn("data", from_json(col("json_value"), stock_schema))
)

stocks_df = parsed_df.select("data.*", "kafka_timestamp")
```

`kafka_timestamp` is the broker-side ingest time and is used as the event-time column for windowed aggregation. `produced_at` (inside the JSON) is the producer wall-clock time.

---

### Step 6 — Data validation (add `is_valid` flag)

```python
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
```

---

### Step 7 — 1-minute windowed aggregation

```python
windowed_df = (
    valid_df
    .withWatermark("kafka_timestamp", "2 minutes")   # tolerate 2-min late arrivals
    .groupBy(
        window(col("kafka_timestamp"), "1 minute"),   # tumbling window
        col("ticker"),
    )
    .agg(
        avg("close").alias("avg_close"),
        sum("volume").alias("total_volume"),
        count("*").alias("event_count"),
    )
)
```

The 2-minute watermark lets Spark drop state for windows older than 2 minutes, keeping memory bounded.

---

### Step 8 — Route invalid events to the Dead-Letter Kafka topic

```python
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
```

Invalid events are serialised back to JSON and published to `us-stocks-dead-letter` for investigation. The checkpoint at `s3a://stocks/checkpoints/dead-letter` ensures exactly-once delivery tracking.

---

### Step 9 — Write clean events to MinIO (`foreachBatch`, one file per ticker)

This is the most important sink. Instead of partitioning by date (which scatters small files across many directories), the job maintains **one Parquet file per ticker** and merges new batches into it:

```python
def write_clean_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    batch_clean = batch_df.select(_CLEAN_COLS)
    tickers = [row.ticker for row in batch_clean.select("ticker").distinct().collect()
               if row.ticker is not None]

    for ticker in tickers:
        ticker_path = f"{CLEAN_PATH}/{ticker}"        # e.g. s3a://stocks/clean/AAPL
        new_rows    = batch_clean.filter(col("ticker") == ticker)

        # Count BEFORE the write (see Bug Fix section)
        new_row_count = new_rows.count()

        try:
            existing = spark.read.parquet(ticker_path)
            merged = (
                existing
                .union(new_rows)
                .dropDuplicates(["event_id"])          # idempotent merges
                .orderBy("date", "event_id")
            )
        except Exception:
            merged = new_rows.orderBy("date", "event_id")

        merged.coalesce(1).write.mode("overwrite").parquet(ticker_path)
```

Key properties of this approach:

| Property | Detail |
|---|---|
| **One file per ticker** | `s3a://stocks/clean/AAPL/part-00000-<uuid>.snappy.parquet` |
| **Deduplication** | `dropDuplicates(["event_id"])` — safe to re-run or replay |
| **Sort order** | Rows sorted by `date` then `event_id` within each file |
| **Trigger** | Every 30 seconds (`processingTime="30 seconds"`) |
| **Coalesce** | `coalesce(1)` — single output file per ticker (avoids small-file explosion) |

---

### Step 10 — Write windowed aggregations to MinIO

```python
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
```

Aggregated results are partitioned by `window_date` (the calendar day of the window start), making date-range queries efficient. Trigger: every 10 seconds.

---

## ✅ Data Validation Rules

| Field | Rule |
|---|---|
| `event_id` | Must be non-null |
| `ticker` | Must be non-null |
| `date` | Must be non-null |
| `open`, `high`, `low`, `close` | Must be non-null and `>= 0` |
| `volume` | Must be non-null and `>= 0` |
| `dividends`, `stock_splits` | Nullable — `0.0` on non-event days |
| `stochk_14_3_3`, `stochd_14_3_3` | Nullable — `null` during indicator warm-up (~14 rows) |

Events that fail any check get `is_valid = false` and are routed to the dead-letter topic.

---

## 🗂️ MinIO Storage Layout

```
stocks/                                     ← top-level bucket
│
├── clean/
│   ├── AAPL/
│   │   └── part-00000-<uuid>.snappy.parquet   ← ALL AAPL rows, merged & deduped
│   ├── MSFT/
│   │   └── part-00000-<uuid>.snappy.parquet
│   └── <TICKER>/
│       └── part-00000-<uuid>.snappy.parquet
│
├── aggregated/
│   ├── window_date=2024-01-15/
│   │   └── part-00000-<uuid>.snappy.parquet
│   └── window_date=2024-01-16/
│       └── part-00000-<uuid>.snappy.parquet
│
└── checkpoints/
    ├── clean/          ← Spark streaming checkpoint for the clean sink
    ├── aggregated/     ← Spark streaming checkpoint for the aggregation sink
    └── dead-letter/    ← Spark streaming checkpoint for the dead-letter sink
```

### Path Reference

| Purpose | S3A Path | Host URL |
|---|---|---|
| Clean data (all tickers) | `s3a://stocks/clean` | `http://localhost:9000/stocks/clean` |
| One ticker | `s3a://stocks/clean/AAPL` | `http://localhost:9000/stocks/clean/AAPL` |
| Aggregated data | `s3a://stocks/aggregated` | `http://localhost:9000/stocks/aggregated` |
| Clean checkpoint | `s3a://stocks/checkpoints/clean` | — |
| Aggregation checkpoint | `s3a://stocks/checkpoints/aggregated` | — |
| Dead-letter checkpoint | `s3a://stocks/checkpoints/dead-letter` | — |
| MinIO console | — | `http://localhost:9001` |
| MinIO S3 API | — | `http://localhost:9000` |

---

## 💾 Checkpointing & Fault Recovery

All three streaming queries write their checkpoints to MinIO. This means:

- Checkpoints survive container restarts (stored in the persistent `minio-data` volume).
- On restart, Spark reads the checkpoint, determines the last committed Kafka offset, and resumes from exactly that point — no data loss, no duplicates (because of `dropDuplicates` in the clean sink).

**To do a full reset** (re-read everything from Kafka offset 0):

```bash
# Delete all checkpoints inside the bucket
docker run --rm --network stocks-net minio/mc \
  alias set local http://minio:9000 minioadmin minioadmin

docker run --rm --network stocks-net minio/mc \
  rm --recursive --force local/stocks/checkpoints/

# Then restart the Spark job
```

---

## ▶️ Running the Full Pipeline

### Step 1 — Clone and configure

```bash
git clone <your-repo-url>
cd <repo-directory>
cp spark/env.example .env    # or: cp env.example .env
```

Edit `.env` — set your own Kaggle credentials:

```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
# MinIO defaults are fine for local development:
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=stocks
```

### Step 2 — Build images (first time only)

```bash
docker compose build
```

This builds two custom images:

- `Dockerfile.python` — for the producer and consumer (Python + confluent-kafka)
- `spark/Dockerfile` — Spark 3.5.7 with all required JARs pre-baked

### Step 3 — Start the full stack

```bash
docker compose up -d
```

Docker Compose starts services in dependency order:

| Order | Service | What it does |
|---|---|---|
| 1 | `kafka-1`, `kafka-2`, `kafka-3` | Start the 3-broker KRaft cluster |
| 2 | `kafka-init` | Waits 30 s, creates `us-stocks-raw` and `us-stocks-dead-letter` topics, then exits |
| 3 | `minio` | Starts the MinIO object store |
| 4 | `minio-init` | Creates the `stocks` bucket, then exits |
| 5 | `producer` | Downloads Kaggle CSVs and streams events to Kafka at 500 rows/s |
| 5 | `consumer` | M1 validation consumer (logs stats, enforces schema) |
| 5 | `kafka-ui` | Kafka monitoring dashboard |
| 5 | `spark` | Spark Master node |
| 5 | `spark-worker` | Spark Worker (4 cores, 4 GB RAM) |

### Step 4 — Verify all services are healthy

```bash
docker compose ps
```

Expected output:

```
NAME                   STATUS          PORTS
kafka-1                healthy         0.0.0.0:9092->9092/tcp
kafka-2                healthy         0.0.0.0:9093->9093/tcp
kafka-3                healthy         0.0.0.0:9094->9094/tcp
kafka-init             exited (0)
stocks-minio           healthy         0.0.0.0:9000-9001->9000-9001/tcp
stocks-minio-init      exited (0)
stocks-producer        running
stocks-consumer        running
kafka-ui               running         0.0.0.0:8080->8080/tcp
stocks-spark           running         0.0.0.0:7077->7077/tcp, 0.0.0.0:8081->8080/tcp
stocks-spark-worker    running
```

Both `kafka-init` and `stocks-minio-init` must show `exited (0)` (success). Any other exit code means something went wrong — check logs with `docker compose logs kafka-init` or `docker compose logs stocks-minio-init`.

### Step 5 — Check events are flowing into Kafka

```bash
docker compose logs --tail=30 producer
```

You should see lines like:

```
[PRODUCER]  START  file=AAPL.csv       rows=9876  files=3/6600 done  total=0
[PRODUCER]    AAPL.csv  sent=  50/9876  ( 0.5%)  total=50  elapsed=00:00:01
```

Also confirm in Kafka UI at `http://localhost:8080` → Topics → `us-stocks-raw` → Messages.

---

## 🚀 Submitting the Spark Job

### Submit from inside the Spark container (recommended)

```bash
docker exec -it stocks-spark \
  /opt/spark/bin/spark-submit \
    --master spark://stocks-spark:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/spark/work-dir/streaming_job.py
```

Because all JARs are baked into `spark/Dockerfile`, there is no need for `--packages`. The job starts immediately.

### Run the Kafka connectivity test first (optional sanity check)

```bash
docker exec -it stocks-spark \
  /opt/spark/bin/spark-submit \
    --master spark://stocks-spark:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/spark/work-dir/test_kafka.py
```

This prints raw Kafka messages to the console and exits when you press Ctrl+C. Use it to confirm Spark ↔ Kafka wiring before running the full streaming job.

### Start only what you need (recommended for Spark-only development)

If Kafka is already running and you just want to restart the Spark job:

```bash
# Bring up only Kafka brokers + MinIO (if not already running)
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init minio minio-init

# Wait for init services to complete
docker compose logs kafka-init     # last line must be "Topics ready. Done."
docker compose logs minio-init     # last line must be 'Bucket "stocks" is ready.'

# Start Spark and the producer
docker compose up -d spark spark-worker producer kafka-ui

# Submit the job
docker exec -it stocks-spark \
  /opt/spark/bin/spark-submit \
    --master spark://stocks-spark:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/spark/work-dir/streaming_job.py
```

### Stop everything

```bash
# Stop containers, keep all data volumes (Kafka offsets + MinIO Parquet survive)
docker compose down

# Full reset — delete all data volumes (Kafka logs + MinIO bucket contents)
docker compose down -v
```

---

## 🖥 Web UIs & Localhost Ports

| Service | URL | Credentials |
|---|---|---|
| **Kafka UI** | http://localhost:8080 | None required |
| **Spark Master UI** | http://localhost:8081 | None required |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **MinIO S3 API** | http://localhost:9000 | — |

### Host ports that must be free

| Port | Service |
|---|---|
| `8080` | Kafka UI |
| `8081` | Spark Master UI (mapped from container port 8080) |
| `9000` | MinIO S3 API |
| `9001` | MinIO web console |
| `9092` | Kafka broker 1 |
| `9093` | Kafka broker 2 |
| `9094` | Kafka broker 3 |
| `7077` | Spark Master (submit endpoint) |

If any port is already in use, stop the conflicting service or edit the host-side port mapping in `docker-compose.yml`.

---

## 🧪 Reading & Verifying Output

### From inside the Spark container (PySpark shell)

```bash
docker exec -it stocks-spark /opt/spark/bin/pyspark \
  --master spark://stocks-spark:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
```

Then in the PySpark shell:

```python
# Read ALL clean data
clean = spark.read.parquet("s3a://stocks/clean")
clean.printSchema()
clean.show(10, truncate=False)
print("Total clean rows:", clean.count())

# Read one ticker
aapl = spark.read.parquet("s3a://stocks/clean/AAPL")
aapl.orderBy("date").show(20, truncate=False)
print("AAPL rows:", aapl.count())

# Read aggregated data
agg = spark.read.parquet("s3a://stocks/aggregated")
agg.orderBy("window").show(20, truncate=False)
print("Aggregated windows:", agg.count())

# Check which tickers have data
from pyspark.sql.functions import countDistinct
clean.agg(countDistinct("ticker").alias("distinct_tickers")).show()
```

### Watch streaming progress in real time

```bash
docker compose logs -f spark          # Spark Master logs
docker compose logs -f spark-worker   # Worker logs (actual job output)
```

Look for lines like:

```
batch_id=12 | tickers in batch: 3 | AAPL, MSFT, GOOGL
batch_id=12 | ticker=AAPL   | new_rows=247 | path=s3a://stocks/clean/AAPL
```

---

## 🧺 Useful MinIO Commands

All commands use the `mc` CLI. The quickest way is to `exec` into any running container that has `mc` installed, or spin up a temporary `minio/mc` container.

### Set up the `mc` alias (run once per terminal session)

```bash
docker run --rm --network stocks-net minio/mc \
  alias set local http://minio:9000 minioadmin minioadmin
```

### Browse all objects in the bucket

```bash
docker run --rm --network stocks-net minio/mc \
  ls --recursive local/stocks
```

### List clean Parquet files

```bash
docker run --rm --network stocks-net minio/mc \
  ls local/stocks/clean/
```

### List files for a specific ticker

```bash
docker run --rm --network stocks-net minio/mc \
  ls local/stocks/clean/AAPL/
```

### List aggregated output

```bash
docker run --rm --network stocks-net minio/mc \
  ls local/stocks/aggregated/
```

### List checkpoints

```bash
docker run --rm --network stocks-net minio/mc \
  ls --recursive local/stocks/checkpoints/
```

### Delete all checkpoints (force full re-read from Kafka)

```bash
docker run --rm --network stocks-net minio/mc \
  rm --recursive --force local/stocks/checkpoints/
```

### Delete all clean data (full reset)

```bash
docker run --rm --network stocks-net minio/mc \
  rm --recursive --force local/stocks/clean/

docker run --rm --network stocks-net minio/mc \
  rm --recursive --force local/stocks/aggregated/
```

### Access MinIO via the web console

Open `http://localhost:9001`, log in with `minioadmin` / `minioadmin`, then navigate to **Buckets → stocks** to browse, download, or delete objects through the UI.

---

## 🐛 Known Bug Fix (2026-09-11)

**Symptom:** the streaming job crashed with:

```
SparkFileNotFoundException: No such file or directory:
  s3a://stocks/clean/AA/part-00000-<uuid>.snappy.parquet
```

**Root cause:** `merged` is a lazy Spark DataFrame whose query plan references the old Parquet file path. The old code called `merged.count()` *after* `.write.mode("overwrite")`. The overwrite deleted the old file, then `.count()` re-executed the lazy plan against the now-deleted path.

**Fix:** moved `new_row_count = new_rows.count()` to *before* the write, so it counts only the incoming rows (which are in memory, not on disk). `merged.count()` is no longer called at all. See `write_clean_batch` in `streaming_job.py`.

---

## 🤝 Handover to M3

M3 does not need to rebuild the Kafka or Spark layers — just read from MinIO.

### Clean event data (one file per ticker, all history)

```
s3a://stocks/clean/<TICKER>/part-*.snappy.parquet
```

Schema: `event_id`, `ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `dividends`, `stock_splits`, `stochk_14_3_3`, `stochd_14_3_3`, `source_file`, `produced_at`

### 1-minute windowed aggregations

```
s3a://stocks/aggregated/window_date=YYYY-MM-DD/part-*.snappy.parquet
```

Schema: `window.start`, `window.end`, `ticker`, `avg_close`, `total_volume`, `event_count`, `window_date`

### Connecting any S3-compatible client to MinIO

```
Endpoint   : http://localhost:9000       (or http://minio:9000 inside Docker)
Access key : minioadmin
Secret key : minioadmin
Bucket     : stocks
Region     : us-east-1  (any value works — MinIO ignores it)
Path-style : true
```

---

## ✅ Definition of Done

| Task | Status |
|---|---|
| Spark cluster configured (master + worker) | ✅ |
| Kafka source configured (`us-stocks-raw`) | ✅ |
| Canonical `StructType` schema enforced | ✅ |
| JSON parsing with `from_json` | ✅ |
| Data validation (`is_valid` flag) | ✅ |
| Invalid events routed to dead-letter Kafka topic | ✅ |
| 1-minute tumbling window aggregation | ✅ |
| 2-minute watermark for late-arrival tolerance | ✅ |
| MinIO deployed with persistent `minio-data` volume | ✅ |
| `stocks` bucket auto-created by `minio-init` | ✅ |
| S3A connector bundled in `spark/Dockerfile` | ✅ |
| Clean Parquet written to MinIO (one file per ticker) | ✅ |
| `dropDuplicates` on `event_id` for idempotent merges | ✅ |
| Aggregated Parquet partitioned by `window_date` | ✅ |
| All checkpoints stored in MinIO | ✅ |
| Fault recovery via MinIO checkpoint | ✅ |
| Bug fix: `merged.count()` moved before overwrite | ✅ |
| README updated with full commands and port reference | ✅ |