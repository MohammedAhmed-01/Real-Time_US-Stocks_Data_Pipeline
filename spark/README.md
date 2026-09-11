# ⚡ Real-Time US Stocks Data Pipeline — Spark Streaming (MinIO Edition)

> **Spark Structured Streaming • Apache Kafka • MinIO (S3) • Parquet • Docker**

This module implements the **real-time Spark Streaming layer** of the US Stocks Data Pipeline.

It consumes stock market events from Apache Kafka, applies a canonical schema and data-quality validation, routes invalid records to a Dead Letter Topic, stores valid events as partitioned Parquet files in **MinIO** (an S3-compatible object store), and performs **1-minute windowed aggregations** — all containerised with Docker.

> **What changed from the HDFS version?**
> `namenode` and `datanode` containers have been removed. All data and checkpoints that were previously stored under `hdfs://namenode:9000/stocks/…` are now stored under `s3a://stocks/…` inside a MinIO container. The Spark job uses the S3A file-system connector (bundled in `spark/Dockerfile`) to write directly to MinIO.

---

## 📌 Table of Contents

* [Overview](#-overview)
* [Architecture](#-architecture)
* [Technology Stack](#-technology-stack)
* [MinIO Setup](#-minio-setup)
* [Spark Configuration](#-spark-configuration)
* [Kafka Input](#-kafka-input)
* [Streaming Workflow](#-streaming-workflow)
* [MinIO Layout](#-minio-layout)
* [Checkpointing & Fault Recovery](#-checkpointing--fault-recovery)
* [Running the Pipeline](#-running-the-pipeline)
* [Reading the Output](#-reading-the-output)
* [Useful MinIO Commands](#-useful-minio-commands)
* [Handover to M3](#-handover-to-m3)
* [Definition of Done](#-definition-of-done)

---

## 🔎 Overview

```text
Kafka → Spark → Validation → Clean Parquet  → MinIO  s3a://stocks/clean
                           → 1-min Agg      → MinIO  s3a://stocks/aggregated
                           → Invalid events → Kafka  us-stocks-dead-letter
```

---

## 🏗️ Architecture

```text
                    ┌──────────────────────┐
                    │   Stock Data Source  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Apache Kafka      │
                    │   us-stocks-raw      │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌─────────────────────────────────┐
              │     Spark Structured Streaming  │
              │                                 │
              │  • Read Kafka                   │
              │  • Parse JSON                   │
              │  • Apply Schema                 │
              │  • Validate Data                │
              │  • Window Aggregation           │
              └───────────────┬─────────────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
                 ▼                         ▼
       ┌──────────────────┐      ┌─────────────────────┐
       │   Valid Events   │      │   Invalid Events    │
       └────────┬─────────┘      └──────────┬──────────┘
                │                           │
                ▼                           ▼
    ┌──────────────────────┐   ┌───────────────────────┐
    │  MinIO               │   │  Dead Letter Kafka    │
    │  s3a://stocks/clean  │   │  us-stocks-dead-letter│
    └────────┬─────────────┘   └───────────────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │  1-Minute Aggregation    │
    └──────────┬───────────────┘
               │
               ▼
    ┌────────────────────────────────┐
    │  MinIO                         │
    │  s3a://stocks/aggregated       │
    └────────────────────────────────┘
```

---

## 🧰 Technology Stack

| Technology                     | Role                                 |
| ------------------------------ | ------------------------------------ |
| **Apache Spark 3.5.7**         | Stream processing & aggregation      |
| **Spark Structured Streaming** | Continuous real-time processing      |
| **Apache Kafka**               | Real-time event ingestion            |
| **MinIO**                      | S3-compatible distributed object store (replaces HDFS) |
| **Hadoop S3A**                 | Spark ↔ MinIO file-system connector  |
| **Parquet**                    | Columnar storage format              |
| **Docker**                     | Containerised execution environment  |
| **PySpark**                    | Streaming application implementation |

---

## 🪣 MinIO Setup

MinIO is deployed as a single Docker container with a persistent named volume (`minio-data`).

| Setting | Value |
|---|---|
| Container | `stocks-minio` |
| S3 API port | `9000` (used by Spark) |
| Web console | `http://localhost:9001` |
| Default user | `minioadmin` |
| Default password | `minioadmin` |
| Bucket | `stocks` |

The `minio-init` service runs once at startup to create the `stocks` bucket via the `mc` CLI. Spark then writes all Parquet files and checkpoints directly into that bucket.

Override the defaults in your `.env` file:

```env
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_BUCKET=stocks
```

---

## ⚙️ Spark Configuration

| Configuration    | Value                       |
| ---------------- | --------------------------- |
| Spark Version    | `3.5.7`                     |
| Spark Service    | `spark`                     |
| Spark Container  | `stocks-spark`              |
| Spark Master     | `spark://stocks-spark:7077` |
| Application Name | `USStocksStreaming`          |
| Streaming Script | `spark/streaming_job.py`    |
| S3A endpoint     | `http://minio:9000`         |

### Extra JARs bundled in `spark/Dockerfile`

| JAR | Purpose |
|---|---|
| `spark-sql-kafka-0-10_2.12-3.5.7.jar` | Kafka source connector |
| `spark-token-provider-kafka-0-10_2.12-3.5.7.jar` | Kafka token provider |
| `hadoop-aws-3.3.4.jar` | S3A file-system implementation |
| `aws-java-sdk-bundle-1.12.262.jar` | AWS/S3 SDK (S3A runtime dependency) |

---

## 📨 Kafka Input

| Setting | Value |
|---|---|
| Topic | `us-stocks-raw` |
| Brokers (internal) | `kafka-1:29092,kafka-2:29093,kafka-3:29094` |
| Partitions | 6 |
| Replication Factor | 3 |

---

## 🔄 Streaming Workflow

The workflow is identical to the HDFS version except all `hdfs://namenode:9000/…` paths are replaced with `s3a://stocks/…`.

1. **Kafka Ingestion** — `readStream` from `us-stocks-raw`
2. **JSON Parsing** — `from_json` with the canonical `StructType`
3. **Validation** — required fields + numeric sanity checks → `is_valid` flag
4. **Dead-Letter Routing** — invalid events → `us-stocks-dead-letter`
5. **Clean Parquet** — valid events → `s3a://stocks/clean`, partitioned by `date`
6. **Windowed Aggregation** — 1-minute tumbling window, 2-minute watermark
7. **Aggregated Parquet** → `s3a://stocks/aggregated`, partitioned by `window_date`

---

## 🗂️ MinIO Layout

```text
stocks/                          ← bucket
│
├── clean/
│   └── date=YYYY-MM-DD/
│       └── *.snappy.parquet
│
├── aggregated/
│   └── window_date=YYYY-MM-DD/
│       └── *.snappy.parquet
│
└── checkpoints/
    ├── clean/
    ├── aggregated/
    └── dead-letter/
```

### Path Reference

| Purpose                | S3A Path                                                  |
| ---------------------- | --------------------------------------------------------- |
| Clean Data             | `s3a://stocks/clean`                                      |
| Aggregated Data        | `s3a://stocks/aggregated`                                 |
| Clean Checkpoint       | `s3a://stocks/checkpoints/clean`                          |
| Aggregation Checkpoint | `s3a://stocks/checkpoints/aggregated`                     |
| Dead-Letter Checkpoint | `s3a://stocks/checkpoints/dead-letter`                    |
| MinIO API              | `http://minio:9000` (internal) / `http://localhost:9000`  |
| MinIO Console          | `http://localhost:9001`                                   |

---

## 💾 Checkpointing & Fault Recovery

Checkpoints are stored in MinIO under `s3a://stocks/checkpoints/`. The S3A committer (`directory` mode) ensures atomic writes so partial checkpoint files do not corrupt state.

Recovery procedure is unchanged from the HDFS version:

1. Stop the Spark application.
2. Restart it with the same `--checkpointLocation` paths.
3. Spark reads the existing checkpoint and resumes from where it left off.
4. New Parquet files appear in `s3a://stocks/clean` and `s3a://stocks/aggregated`.

---

## ▶️ Running the Pipeline

### 1 — Start the full stack

```bash
# Copy env template and set your credentials
cp env.example .env
# Edit .env: set KAGGLE_USERNAME, KAGGLE_KEY
# MinIO credentials default to minioadmin/minioadmin — change for production

docker compose up -d
```

Services started: `kafka-1/2/3` → `kafka-init` → `minio` → `minio-init` → `producer` + `consumer` + `kafka-ui` + `spark` + `spark-worker`.

### 2 — Submit the streaming job

```bash
docker exec -it stocks-spark \
  /opt/spark/bin/spark-submit \
    --master spark://stocks-spark:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/spark/work-dir/streaming_job.py
```

Because all JARs are pre-baked into the image (`spark/Dockerfile`), there is no need to pass `--packages` at submit time. The `--packages` flag is only needed if you are running `spark-submit` outside the container and want Maven to download JARs on the fly.

### 3 — Test Kafka connectivity only

```bash
docker exec -it stocks-spark \
  /opt/spark/bin/spark-submit \
    --master spark://stocks-spark:7077 \
    /opt/spark/work-dir/test_kafka.py
```

---

## 🧪 Useful MinIO Commands

All commands use the `mc` CLI inside the `minio-init` (or any `minio/mc`) container. You can also use the web console at `http://localhost:9001`.

### Browse bucket contents

```bash
docker run --rm --network stocks-net minio/mc \
  alias set local http://minio:9000 minioadmin minioadmin

# List all objects
docker run --rm --network stocks-net minio/mc \
  ls --recursive local/stocks
```

### Check clean output

```bash
docker run --rm --network stocks-net minio/mc \
  ls local/stocks/clean/
```

### Check aggregated output

```bash
docker run --rm --network stocks-net minio/mc \
  ls local/stocks/aggregated/
```

### Check checkpoints

```bash
docker run --rm --network stocks-net minio/mc \
  ls --recursive local/stocks/checkpoints/
```

### Read Parquet with Spark (verification)

```python
# Run inside a PySpark shell pointed at the same MinIO instance
df = spark.read.parquet("s3a://stocks/clean")
df.printSchema()
df.show(10, truncate=False)
print(df.count())

agg = spark.read.parquet("s3a://stocks/aggregated")
agg.show(10, truncate=False)
print(agg.count())
```

---

## 🤝 Handover to M3

The Spark Streaming and MinIO output layer is complete. M3 does **not** need to rebuild the Kafka ingestion or Spark streaming pipeline.

### For detailed valid events

```text
s3a://stocks/clean
```

### For real-time aggregated data

```text
s3a://stocks/aggregated
```

Fields: `window.start`, `window.end`, `ticker`, `avg_close`, `total_volume`, `event_count`, `window_date`.

### Connecting M3 to MinIO

Any client that speaks the S3 protocol works. Configure it with:

```
Endpoint : http://localhost:9000   (or http://minio:9000 inside Docker)
Access key: minioadmin             (or your custom value from .env)
Secret key: minioadmin
Bucket    : stocks
```

---

## ✅ Definition of Done

| Task                                       | Status |
| ------------------------------------------ | :----: |
| Spark cluster configured                   |    ✅   |
| Kafka source configured                    |    ✅   |
| Canonical schema enforced                  |    ✅   |
| JSON parsing implemented                   |    ✅   |
| Data validation implemented                |    ✅   |
| Invalid events routed to dead-letter topic |    ✅   |
| Windowed aggregation implemented           |    ✅   |
| MinIO deployed & bucket initialised        |    ✅   |
| S3A connector bundled in Spark image       |    ✅   |
| Clean Parquet written to MinIO             |    ✅   |
| Aggregated Parquet written to MinIO        |    ✅   |
| MinIO partitioning implemented             |    ✅   |
| Checkpointing configured in MinIO          |    ✅   |
| Fault recovery via checkpoint              |    ✅   |
| Handover documentation updated             |    ✅   |
