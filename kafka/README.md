<p align="center">
  <h1 align="center">📈 Real-Time US Stocks Data Pipeline</h1>
  <p align="center">
    An end-to-end streaming data engineering project — from raw Kaggle data to an AI-powered RAG assistant.
  </p>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square"/>
  <img src="https://img.shields.io/badge/Milestone-M1%20Complete-blue?style=flat-square"/>
  <img src="https://img.shields.io/badge/License-MIT-yellow?style=flat-square"/>
</p>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Kafka Cluster Configuration](#-kafka-cluster-configuration)
- [Topic Design](#-topic-design)
- [Event Schema](#-event-schema)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Running the Pipeline](#-running-the-pipeline)
- [Web UIs & Monitoring](#-web-uis--monitoring)
- [Producer Details](#-producer-details)
- [Consumer / Validator Details](#-consumer--validator-details)
- [Consumer Groups](#-consumer-groups)
- [Environment Variables](#-environment-variables)
- [Validation Rules](#-validation-rules)
- [M1 → M2 Handover Reference](#-m1--m2-handover-reference)
- [M2 Spark Structured Streaming — Start Here](#-m2-spark-structured-streaming--start-here)
- [Python Dependencies](#-python-dependencies)
- [Troubleshooting](#-troubleshooting)

---

## 🔍 Overview

This project implements a **real-time US stock market data pipeline** using industry-standard big data tools. A Kaggle dataset (`footballjoe789/us-stock-dataset`) is replayed as a live stream through **Apache Kafka**, processed by **Apache Spark**, stored in **HDFS** and **PostgreSQL**, visualised in **Power BI**, enriched with an **MLlib** prediction model, and finally surfaced through an AI-powered **RAG chat assistant** — all containerised with Docker and orchestrated end-to-end.

The pipeline is structured across milestones. This repository covers **M1 — Data Ingestion & Kafka**, which delivers:

| Component | Description |
|---|---|
| **Kafka Producer** | Parallel downloader + streamer — ingests Kaggle CSVs as live JSON events |
| **Kafka Consumer** | Validating micro-batch consumer — enforces the canonical event schema |
| **KRaft Cluster** | 3-broker Kafka cluster in KRaft mode (no ZooKeeper) running inside Docker |
| **Kafka UI** | Real-time monitoring dashboard at `http://localhost:8080` |

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  DOCKER NETWORK: stocks-net                                              │
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐               │
│  │   kafka-1    │    │   kafka-2    │    │   kafka-3    │               │
│  │  (broker +   │◄──►│  (broker +   │◄──►│  (broker +   │               │
│  │  controller) │    │  controller) │    │  controller) │               │
│  │  port 9092   │    │  port 9093   │    │  port 9094   │               │
│  └──────┬───────┘    └──────────────┘    └──────────────┘               │
│         │                                                                │
│  ┌──────▼──────────────────────────────────────────────────────────┐    │
│  │                         TOPICS                                   │    │
│  │   us-stocks-raw         (6 partitions, RF=3, 7-day retention)   │    │
│  │   us-stocks-dead-letter (3 partitions, RF=3, 30-day retention)  │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│         ▲                               │                                │
│  ┌──────┴──────┐                ┌───────▼──────┐                        │
│  │  producer   │                │   consumer   │                        │
│  │             │                │              │                        │
│  │  Kaggle     │                │  Validate +  │                        │
│  │  Download   │                │  Log Stats   │                        │
│  │  Thread     │                │              │                        │
│  │     ↓       │                └──────────────┘                        │
│  │  Queue      │                                                         │
│  │     ↓       │                                                         │
│  │  Produce    │                                                         │
│  │  Thread     │                                                         │
│  └─────────────┘                                                         │
│                                                                          │
│  ┌──────────────┐                                                        │
│  │  kafka-ui    │  →  http://localhost:8080                             │
│  └──────────────┘                                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Kaggle Dataset
    │
    │  kaggle CLI / REST API
    ▼
Download Thread  ──[queue]──►  Main Thread (Producer)
                                    │
                                    │  JSON events, keyed by ticker
                                    ▼
                              us-stocks-raw  (Kafka Topic)
                                    │
                          ┌─────────┴──────────┐
                          │                    │
                          ▼                    ▼
                   M1 Consumer          M2 Spark Job
                   (validate)           (transform)
                          │
                          ▼
                  us-stocks-dead-letter
                  (invalid events only)
```

---

## 📁 Project Structure

```
.
├── docker-compose.yml        # 3-broker KRaft cluster + producer + consumer + UI
├── Dockerfile.python         # Shared image for producer and consumer
├── requirements.txt          # Python dependencies
├── .env                      # Your secrets — NEVER commit this file
├── env.example               # Template — copy to .env and fill in your values
└── kafka/
    ├── producer.py           # Kaggle → Kafka streaming producer
    ├── consumer.py           # Validating micro-batch consumer
    └── topic_config.py       # Single source of truth: topic specs & event schema
```

---

## ⚙️ Kafka Cluster Configuration

The cluster runs in **KRaft mode** — no ZooKeeper required. All three brokers act simultaneously as both broker and controller, providing a lean, self-contained cluster.

| Setting | Value |
|---|---|
| Image | `confluentinc/cp-kafka:7.6.1` |
| Mode | KRaft (Raft-based metadata, no ZooKeeper) |
| Brokers | 3 (`kafka-1`, `kafka-2`, `kafka-3`) |
| Cluster ID | `MkU3OEVBNTcwNTJENDM2Qk` |
| Default replication factor | `3` |
| Min in-sync replicas | `2` |
| Default partitions | `6` |
| Log retention | 7 days (168 hours) |
| Log segment size | 1 GB |
| Compression | `lz4` |
| Auto topic creation | **Disabled** (topics created explicitly by `kafka-init`) |
| Inter-broker protocol | `PLAINTEXT` (internal Docker network only) |

### Broker Port Mapping

| Broker | Internal (Docker network) | External (Host machine) |
|---|---|---|
| `kafka-1` | `kafka-1:29092` | `localhost:9092` |
| `kafka-2` | `kafka-2:29093` | `localhost:9093` |
| `kafka-3` | `kafka-3:29094` | `localhost:9094` |

> **Why three brokers?** A replication factor of 3 with `min.insync.replicas=2` means the cluster tolerates one broker failure without data loss or downtime.

---

## 📨 Topic Design

### `us-stocks-raw` — Primary Ingest Topic

| Property | Value |
|---|---|
| Partitions | `6` |
| Replication factor | `3` |
| Min in-sync replicas | `2` |
| Retention | 7 days (`604800000 ms`) |
| Compression | `lz4` |
| Cleanup policy | `delete` |

**Purpose:** One JSON event per CSV row, keyed by ticker symbol. Partitioned by ticker so all rows for the same stock land on the same partition, preserving per-ticker time order. M2 (Spark Structured Streaming) reads from this topic using its own independent consumer group.

---

### `us-stocks-dead-letter` — Failed Events

| Property | Value |
|---|---|
| Partitions | `3` |
| Replication factor | `3` |
| Retention | 30 days (`2592000000 ms`) |
| Compression | `lz4` |

**Purpose:** Receives events that fail schema validation. Retained for 30 days to allow investigation and reprocessing.

> Topics are created once by the `kafka-init` service on first startup. Auto-creation is disabled to enforce exact partition and replication settings.

---

## 🗂 Event Schema

Every message on `us-stocks-raw` is a UTF-8 encoded JSON object. The Kafka message **key** is the `ticker` string, ensuring partition affinity per stock.

```json
{
  "event_id":       1042,
  "ticker":         "AAPL",
  "date":           "2023-11-14",
  "open":           184.25,
  "high":           186.10,
  "low":            183.97,
  "close":          185.32,
  "volume":         52341200.0,
  "dividends":      0.0,
  "stock_splits":   0.0,
  "stochk_14_3_3":  72.41,
  "stochd_14_3_3":  68.15,
  "source_file":    "data/stockhistory/AAPL.csv",
  "produced_at":    "2026-09-09T08:38:51Z"
}
```

### Field Reference

| Field | Spark Type | Nullable | Notes |
|---|---|---|---|
| `event_id` | `LongType` | No | Monotonically increasing across the entire run |
| `ticker` | `StringType` | No | Derived from the CSV filename (e.g. `AAPL.csv` → `"AAPL"`) |
| `date` | `DateType` | No | Trading date in `YYYY-MM-DD` format |
| `open` | `DoubleType` | No | Opening price — must be > 0 |
| `high` | `DoubleType` | No | Session high — must be > 0 and ≥ `low` |
| `low` | `DoubleType` | No | Session low — must be > 0 |
| `close` | `DoubleType` | No | Closing price — must be > 0 |
| `volume` | `DoubleType` | No | Shares traded — must be ≥ 0 |
| `dividends` | `DoubleType` | No | `0.0` on non-dividend days |
| `stock_splits` | `DoubleType` | No | `0.0` on non-split days |
| `stochk_14_3_3` | `DoubleType` | **Yes** | Stochastic %K — null during first ~14 rows per ticker (indicator warm-up) |
| `stochd_14_3_3` | `DoubleType` | **Yes** | Stochastic %D — null during first ~14 rows per ticker (indicator warm-up) |
| `source_file` | `StringType` | No | Original Kaggle path, e.g. `data/stockhistory/AAPL.csv` |
| `produced_at` | `TimestampType` | No | UTC wall-clock at produce time (ISO-8601 with `Z` suffix) |

---

## 🧰 Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker Desktop (or Engine + Compose plugin) | v24+ | At least **8 GB RAM** allocated to Docker |
| Python | 3.10 / 3.11 / 3.12 | Only needed for running outside Docker |
| Kaggle account + API token | — | [Create one here](https://www.kaggle.com/settings) under API → Create New Token |

**Host ports that must be free:**

| Port | Service |
|---|---|
| `8080` | Kafka UI |
| `9092` | kafka-1 (external) |
| `9093` | kafka-2 (external) |
| `9094` | kafka-3 (external) |

---

## 🚀 Quick Start

### 1 — Clone & configure

```bash
git clone <your-repo-url>
cd <repo-directory>
cp env.example .env
```

Open `.env` and fill in your Kaggle credentials:

```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

### 2 — Start the full stack

```bash
docker compose up -d
```

Docker Compose will:
1. Start `kafka-1`, `kafka-2`, `kafka-3` and wait for all three health checks to pass
2. Run `kafka-init` — waits 30 s for KRaft leader election, then creates both topics
3. Start the **producer** — downloads Kaggle CSVs and streams them into Kafka
4. Start the **consumer** — reads events, validates them, and logs batch statistics
5. Start **kafka-ui** at `http://localhost:8080`

### 2a — Start only what you need (recommended for M2 Spark work)

If you are working on the M2 Spark job and only need the Kafka cluster + data flowing in — you don't need to run the M1 validation consumer inside Docker. Start just the three services Spark depends on:

```bash
# Bring up brokers + topic init first (one-time, blocking until topics exist)
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init

# Wait until kafka-init finishes (check with: docker compose logs kafka-init)
# Then start the producer and the UI
docker compose up -d producer kafka-ui
```

Or as a single command once the brokers are already healthy:

```bash
docker compose up producer consumer kafka-ui
```

> **Note:** `producer` and `consumer` without `-d` run in the foreground so you see their logs directly. Add `-d` to detach.

Check everything is running:

```bash
docker compose ps
```

Expected output — all three brokers `healthy`, producer and consumer `running`:

```
NAME              STATUS          PORTS
kafka-1           healthy         0.0.0.0:9092->9092/tcp
kafka-2           healthy         0.0.0.0:9093->9093/tcp
kafka-3           healthy         0.0.0.0:9094->9094/tcp
kafka-init        exited (0)
stocks-producer   running
stocks-consumer   running
kafka-ui          running         0.0.0.0:8080->8080/tcp
```

### 3 — Watch the logs

```bash
# All services
docker compose logs -f

# Producer only
docker compose logs -f producer

# Consumer only
docker compose logs -f consumer

# Topic init
docker compose logs kafka-init
```

### 4 — Open Kafka UI

Navigate to **[http://localhost:8080](http://localhost:8080)** to browse topics, inspect messages, and monitor consumer lag in real time.

### 5 — Stop everything

```bash
# Stop containers, keep volumes (Kafka data persists)
docker compose down

# Stop and delete all Kafka data volumes (full reset)
docker compose down -v
```

---

## 🏃 Running the Pipeline

### Producer — local (outside Docker)

```bash
pip install -r requirements.txt

# Stream all stocks at 5× speed, 50-row micro-batches
python kafka/producer.py --speed 5 --batch-size 50

# Stream only one ticker symbol
python kafka/producer.py --ticker AAPL --speed 1

# Override the target topic
python kafka/producer.py --topic my-topic --speed 10
```

### Consumer — local (outside Docker)

```bash
# Default settings (batch=100 messages, max-wait=2 s)
python kafka/consumer.py

# Print every valid message as it arrives
python kafka/consumer.py --live

# Larger batches, faster flush interval
python kafka/consumer.py --batch-size 500 --max-wait-ms 1000
```

### Producer CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--speed` | `1.0` | Replay speed multiplier (`5` = 5× faster than real-time) |
| `--batch-size` | `50` | Rows per Kafka micro-batch |
| `--ticker` | *(all tickers)* | Stream only this ticker symbol |
| `--topic` | `us-stocks-raw` | Override the target Kafka topic |

### Consumer CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--batch-size` | `100` | Messages per processing batch (size trigger) |
| `--max-wait-ms` | `2000` | Max milliseconds before a time-triggered flush |
| `--timeout` | `0.5` | Kafka poll timeout in seconds |
| `--group` | `m1-validation-group` | Override the consumer group ID |
| `--topic` | `us-stocks-raw` | Override the source topic |
| `--live` | off | Print every valid message individually as it arrives |

---

## 🖥 Web UIs & Monitoring

### Kafka UI — `http://localhost:8080`

Powered by [Provectus Kafka UI](https://github.com/provectus/kafka-ui). Accessible from the host machine only.

| Task | Where to find it |
|---|---|
| Browse topics and messages | **Topics** → select a topic → **Messages** |
| Check partition offsets and lag | **Topics** → select a topic → **Overview** |
| Monitor consumer group lag | **Consumer Groups** → `m1-validation-group` |
| View broker health and metrics | **Brokers** |
| Inspect cluster metadata | **Dashboard** |

---

## 📤 Producer Details

The producer runs in **parallel real-time mode** using two coordinated threads:

| Thread | Role |
|---|---|
| **Download thread** | Downloads CSV files from Kaggle one at a time; pushes `(filename, DataFrame)` pairs to an in-memory queue (max 3 files buffered) |
| **Main thread** | Pops files from the queue, converts each row to a JSON event, and produces micro-batches to Kafka |

This design keeps the network download from blocking Kafka production, and keeps memory usage bounded.

### Kafka Producer Settings

| Setting | Value | Reason |
|---|---|---|
| `acks` | `all` | Strongest durability — leader + all ISR replicas confirm write |
| `retries` | `5` | Automatic retry on transient broker errors |
| `retry.backoff.ms` | `500` | Pause between retries |
| `linger.ms` | `10` | Accumulate small messages into larger batches |
| `batch.size` | `65536` | 64 KB produce batch |
| `compression.type` | `lz4` | Fast compression, matches topic-level config |
| `enable.idempotence` | `true` | Exactly-once semantics on the producer side |
| `max.in.flight.requests.per.connection` | `5` | Safe with idempotence enabled |

### Dataset Source

| Detail | Value |
|---|---|
| Kaggle dataset | `footballjoe789/us-stock-dataset` |
| File pattern matched | `data/stockhistory/*.csv` |
| Required columns | `date`, `open`, `high`, `low`, `close`, `volume` |
| Ticker derivation | From the CSV filename — `AAPL.csv` → `ticker = "AAPL"` |

---

## 📥 Consumer / Validator Details

The consumer uses **dual-triggered micro-batch flushing** — whichever condition fires first triggers a flush:

| Trigger | Condition |
|---|---|
| **Size** | Buffer accumulates `--batch-size` messages |
| **Time** | `--max-wait-ms` milliseconds elapse since the last flush |

After each batch, offsets are **committed manually** (`enable.auto.commit = false`) — no event is marked processed before it has been validated.

### Kafka Consumer Settings

| Setting | Value |
|---|---|
| `auto.offset.reset` | `earliest` — replay from the beginning on first run |
| `enable.auto.commit` | `false` — manual commit after each validated batch |
| `max.poll.interval.ms` | `300000` (5 minutes) |
| `session.timeout.ms` | `30000` (30 seconds) |
| `fetch.min.bytes` | `1` |
| `fetch.wait.max.ms` | `500` |

---

## 👥 Consumer Groups

| Group ID | Used by | Purpose |
|---|---|---|
| `m1-validation-group` | `consumer.py` (M1) | Schema validation and statistics logging |
| `spark-streaming-group` | M2 Spark job | Structured Streaming reads with independent offset tracking |

Each group maintains its own committed offsets, so M2 Spark can re-read from the beginning without being affected by M1's position — and vice versa.

---

## 🔧 Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAGGLE_USERNAME` | **Yes** | — | Your Kaggle account username |
| `KAGGLE_KEY` | **Yes** | — | Your Kaggle API key |
| `KAFKA_BOOTSTRAP` | No | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_TOPIC` | No | `us-stocks-raw` | Primary ingest topic name |
| `KAFKA_DLT_TOPIC` | No | `us-stocks-dead-letter` | Dead-letter topic name |
| `KAFKA_GROUP_ID` | No | `m1-validation-group` | Consumer group for M1 validation |

> **Inside Docker** — the `docker-compose.yml` environment block automatically injects `kafka-1:29092,kafka-2:29093,kafka-3:29094` as `KAFKA_BOOTSTRAP`. You do not need to change this.
>
> **Outside Docker** — the default `localhost:9092,...` resolves to the exposed host ports.

---

## ✅ Validation Rules

The consumer enforces the following checks on every incoming event:

| Check | Rule |
|---|---|
| Required fields | All 14 schema fields must be present in the JSON object |
| Core numeric fields | `open`, `high`, `low`, `close`, `volume` must be non-null and parseable as `float` |
| Nullable numeric fields | `dividends`, `stock_splits`, `stochk_14_3_3`, `stochd_14_3_3` may be `null`; if present, must be parseable as `float` |
| Date format | `date` must be parseable as `YYYY-MM-DD` |
| Price sanity | `high` ≥ `low`; `open`, `high`, `low`, `close` all > 0 |
| Volume sanity | `volume` ≥ 0 |

Events that fail any check are logged as `INVALID` with a full error breakdown and counted in the global invalid stat. Future milestones will route them to `us-stocks-dead-letter` for reprocessing.

---

## 🔗 M1 → M2 Handover Reference

All constants needed by the M2 Spark job are exported from `kafka/topic_config.py`. Run it directly for a formatted summary:

```bash
python kafka/topic_config.py
```

### Key Values for Spark Structured Streaming

| Setting | Value |
|---|---|
| Bootstrap (internal Docker) | `kafka-1:29092,kafka-2:29093,kafka-3:29094` |
| Bootstrap (host machine) | `localhost:9092,localhost:9093,localhost:9094` |
| Source topic | `us-stocks-raw` |
| Spark consumer group | `spark-streaming-group` |
| Starting offset | `earliest` |
| Message key | `ticker` (UTF-8 bytes) |
| Message value | JSON (UTF-8) — use the field reference table above to build the Spark `StructType` |

---

## ⚡ M2 Spark Structured Streaming — Start Here

Everything below is what you need to connect your Spark job to the running Kafka cluster and start consuming and processing the stock events.

### Step 1 — Ensure M1 is running

```bash
# Brokers must be healthy and producer must be streaming
docker compose ps

# Confirm events are flowing — you should see messages in the topic
docker compose logs --tail=20 producer
```

Open **[http://localhost:8080](http://localhost:8080)** → Topics → `us-stocks-raw` → Messages to visually confirm events are arriving.

---

### Step 2 — Spark Dependencies (Maven packages)

Spark needs the Kafka connector JAR. Pass it at submit time or add it to your `SparkSession`:

```bash
# spark-submit
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  your_spark_job.py
```

Or pin it in your `SparkSession` builder:

```python
spark = SparkSession.builder \
    .appName("us-stocks-streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
```

> Match the Scala version (`2.12`) and Spark version (`3.5.0`) to your local Spark installation. Check with `spark-submit --version`.

---

### Step 3 — Connect to Kafka and Read the Stream

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, TimestampType
)

# ── Spark Session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("us-stocks-m2-streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Kafka connection — use localhost ports when Spark runs on the host machine
KAFKA_BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
# If Spark runs INSIDE the same Docker network (stocks-net), use:
# KAFKA_BOOTSTRAP = "kafka-1:29092,kafka-2:29093,kafka-3:29094"

KAFKA_TOPIC     = "us-stocks-raw"
CONSUMER_GROUP  = "spark-streaming-group"   # independent from M1

# ── Read raw stream from Kafka ───────────────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \          # replay from the start
    .option("kafka.group.id", CONSUMER_GROUP) \
    .option("failOnDataLoss", "false") \
    .load()

# raw_stream columns: key (binary), value (binary), topic, partition, offset,
#                     timestamp (Kafka ingest time), timestampType
```

---

### Step 4 — Define the Schema and Parse JSON

Copy this schema exactly — it mirrors the canonical event shape produced by `producer.py`:

```python
STOCK_SCHEMA = StructType([
    StructField("event_id",       LongType(),      nullable=False),
    StructField("ticker",         StringType(),    nullable=False),
    StructField("date",           StringType(),    nullable=False),  # parse below
    StructField("open",           DoubleType(),    nullable=False),
    StructField("high",           DoubleType(),    nullable=False),
    StructField("low",            DoubleType(),    nullable=False),
    StructField("close",          DoubleType(),    nullable=False),
    StructField("volume",         DoubleType(),    nullable=False),
    StructField("dividends",      DoubleType(),    nullable=True),
    StructField("stock_splits",   DoubleType(),    nullable=True),
    StructField("stochk_14_3_3",  DoubleType(),    nullable=True),   # null during warm-up
    StructField("stochd_14_3_3",  DoubleType(),    nullable=True),   # null during warm-up
    StructField("source_file",    StringType(),    nullable=True),
    StructField("produced_at",    StringType(),    nullable=True),   # parse below
])

# ── Deserialise and flatten ──────────────────────────────────────────────────
parsed_stream = (
    raw_stream
    .select(
        col("key").cast("string").alias("kafka_key"),       # = ticker
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_ingest_time"),        # Kafka broker timestamp
        from_json(col("value").cast("string"), STOCK_SCHEMA).alias("data")
    )
    .select(
        col("kafka_key"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_ingest_time"),
        col("data.*")                                       # explode all event fields
    )
    # Cast date strings to proper types
    .withColumn("date",        col("date").cast("date"))
    .withColumn("produced_at", to_timestamp(col("produced_at")))
)

# parsed_stream is now a clean streaming DataFrame ready for transformations
```

---

### Step 5 — Apply Transformations

```python
from pyspark.sql.functions import (
    window, avg, max as spark_max, min as spark_min,
    round as spark_round, count
)

# ── Example 1: filter a single ticker ───────────────────────────────────────
aapl_stream = parsed_stream.filter(col("ticker") == "AAPL")

# ── Example 2: daily OHLCV summary (tumbling window on trading date) ─────────
daily_summary = (
    parsed_stream
    .groupBy("ticker", "date")
    .agg(
        spark_round(avg("close"),  4).alias("avg_close"),
        spark_round(spark_max("high"),   4).alias("day_high"),
        spark_round(spark_min("low"),    4).alias("day_low"),
        spark_round(avg("volume"), 0).alias("avg_volume"),
        count("*").alias("row_count"),
    )
)

# ── Example 3: 1-minute rolling window on produced_at ────────────────────────
rolling_1m = (
    parsed_stream
    .withWatermark("produced_at", "2 minutes")
    .groupBy(
        window(col("produced_at"), "1 minute"),
        col("ticker")
    )
    .agg(
        spark_round(avg("close"), 4).alias("avg_close"),
        spark_round(avg("volume"), 0).alias("avg_volume"),
    )
)
```

---

### Step 6 — Write Output (choose a sink)

```python
# ── Sink A: console (development / debugging) ────────────────────────────────
query_console = (
    parsed_stream.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="10 seconds")
    .start()
)

# ── Sink B: Parquet on HDFS (or local path for testing) ──────────────────────
query_hdfs = (
    parsed_stream.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "hdfs://namenode:9000/data/stocks/raw/")   # or a local path
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/stocks-raw/")
    .partitionBy("ticker", "date")
    .trigger(processingTime="30 seconds")
    .start()
)

# ── Sink C: PostgreSQL via foreachBatch ──────────────────────────────────────
def write_to_postgres(batch_df, batch_id):
    (
        batch_df.write
        .format("jdbc")
        .option("url",      "jdbc:postgresql://localhost:5432/stocks")
        .option("dbtable",  "stock_events")
        .option("user",     "your_pg_user")
        .option("password", "your_pg_password")
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    )

query_pg = (
    parsed_stream.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/checkpoints/stocks-pg/")
    .trigger(processingTime="30 seconds")
    .start()
)

# ── Wait for all queries ─────────────────────────────────────────────────────
spark.streams.awaitAnyTermination()
```

---

### Spark ↔ Kafka Connection Reference

| Setting | Value |
|---|---|
| **Bootstrap (Spark on host)** | `localhost:9092,localhost:9093,localhost:9094` |
| **Bootstrap (Spark in Docker on `stocks-net`)** | `kafka-1:29092,kafka-2:29093,kafka-3:29094` |
| **Topic** | `us-stocks-raw` |
| **Consumer group** | `spark-streaming-group` |
| **Starting offset** | `earliest` (re-reads all history) or `latest` (live only) |
| **Message key** | `ticker` — use `.cast("string")` to decode |
| **Message value** | JSON string — parse with `from_json` + `STOCK_SCHEMA` |
| **Kafka ingest timestamp** | `timestamp` column on the raw stream (Kafka broker time) |
| **Event timestamp** | `produced_at` field inside the JSON (producer wall-clock, UTC) |
| **Maven package** | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` |

> **Checkpoint locations** are mandatory for fault-tolerant streaming. Always set `checkpointLocation` before running in production. Without it, Spark cannot recover from a restart.

---

| Package | Version | Purpose |
|---|---|---|
| `confluent-kafka` | `2.4.0` | Kafka producer and consumer client (librdkafka bindings) |
| `pandas` | `>=2.1.0` | CSV loading and DataFrame manipulation |
| `numpy` | `>=1.26.0` | Numeric support for pandas |
| `requests` | `2.32.3` | Streaming HTTP download from the Kaggle REST API |
| `python-dotenv` | `1.0.1` | Load `.env` file into environment variables |
| `kaggle` | `1.6.14` | Kaggle CLI and `kaggle.json` auth fallback |
| `tqdm` | `4.66.4` | Progress bars for long downloads |

```bash
pip install -r requirements.txt
```

---

## 🐛 Troubleshooting

**Kafka brokers not healthy after `docker compose up`**

> KRaft leader election can take up to 60–90 seconds on first boot. Wait and re-check with `docker compose ps`. All three brokers must show `healthy` before `kafka-init` will run.

**`kafka-init` exits before topics are created**

> The init container sleeps 30 s for KRaft election before issuing `kafka-topics` commands. If brokers are slow on your machine, increase the `sleep 30` line in `docker-compose.yml`.

**Producer fails with a Kaggle auth error**

> Verify `KAGGLE_USERNAME` and `KAGGLE_KEY` in your `.env`. Test credentials with:
> ```bash
> kaggle datasets files footballjoe789/us-stock-dataset
> ```

**Consumer shows `Topic not found` and keeps retrying**

> The consumer retries for up to 5 minutes (60 × 5 s). Ensure `kafka-init` completed successfully:
> ```bash
> docker compose logs kafka-init
> ```
> The final line should read `Topics ready. Done.`

**Port already in use on startup**

> Check for conflicting services on ports `8080`, `9092`, `9093`, `9094` and stop them, or edit the host-side port mappings in `docker-compose.yml`.

**Out of memory / brokers restarting**

> Allocate at least 8 GB RAM to Docker in Docker Desktop → Settings → Resources. Three Kafka brokers are memory-intensive.

---

<p align="center">
  Built with ❤️ as part of a real-time data engineering portfolio project.
</p>
