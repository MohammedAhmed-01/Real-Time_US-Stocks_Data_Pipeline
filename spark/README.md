# ⚡ Real-Time US Stocks Data Pipeline — Spark Streaming

> **Spark Structured Streaming • Apache Kafka • HDFS • Parquet • Docker**

This module implements the **real-time Spark Streaming layer** of the US Stocks Data Pipeline.

It consumes stock market events from Apache Kafka, applies a canonical schema and data-quality validation, routes invalid records to a Dead Letter Topic, stores valid events as partitioned Parquet files in HDFS, and performs **1-minute windowed aggregations** for downstream analytics.

---

## 📌 Table of Contents

* [Overview](#-overview)
* [Architecture](#-architecture)
* [Technology Stack](#-technology-stack)
* [Spark Configuration](#-spark-configuration)
* [Kafka Input](#-kafka-input)
* [Streaming Workflow](#-streaming-workflow)

  * [1. Kafka Ingestion](#1-kafka-ingestion)
  * [2. Schema Enforcement](#2-schema-enforcement)
  * [3. Data Validation](#3-data-validation)
  * [4. Dead Letter Handling](#4-dead-letter-handling)
  * [5. Clean Data Layer](#5-clean-data-layer)
  * [6. Windowed Aggregation](#6-windowed-aggregation)
  * [7. Aggregated Data Layer](#7-aggregated-data-layer)
* [HDFS Layout](#-hdfs-layout)
* [Checkpointing & Fault Recovery](#-checkpointing--fault-recovery)
* [Data Validation](#-data-validation)
* [Running the Pipeline](#-running-the-pipeline)
* [Reading the Output](#-reading-the-output)
* [Handover to M3](#-handover-to-m3)
* [Definition of Done](#-definition-of-done)

---

## 🔎 Overview

The Spark Streaming module is responsible for transforming the raw real-time Kafka stream into two reliable HDFS data layers:

### Clean Layer

Contains validated stock events in Parquet format.

```text
Kafka → Spark → Validation → Clean Parquet → HDFS
```

### Aggregated Layer

Contains ticker-level 1-minute streaming aggregations.

```text
Clean Events → 1-Minute Window → Aggregation → Parquet → HDFS
```

Invalid events are isolated through a Dead Letter Topic instead of being written to the clean layer.

```text
                         ┌── Valid ──→ Clean Parquet
Kafka → Spark → Validate ┤
                         └── Invalid → Dead Letter Topic
```

---

# 🏗️ Architecture

```text
                    ┌──────────────────────┐
                    │   Stock Data Source  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Apache Kafka      │
                    │   us-stocks-raw     │
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
              │  • Transform Events             │
              │  • Window Aggregation            │
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
       ┌──────────────────┐       ┌────────────────────┐
       │   Clean Parquet  │       │ Dead Letter Topic  │
       └────────┬─────────┘       └────────────────────┘
                │
                ▼
       ┌──────────────────────┐
       │ 1-Minute Aggregation │
       └──────────┬───────────┘
                  │
                  ▼
       ┌──────────────────────┐
       │ Aggregated Parquet   │
       └──────────┬───────────┘
                  │
                  ▼
             ┌─────────┐
             │   HDFS  │
             └─────────┘
```

---

# 🧰 Technology Stack

| Technology                     | Role                                 |
| ------------------------------ | ------------------------------------ |
| **Apache Spark 3.5.7**         | Stream processing & aggregation      |
| **Spark Structured Streaming** | Continuous real-time processing      |
| **Apache Kafka**               | Real-time event ingestion            |
| **Hadoop HDFS**                | Distributed data storage             |
| **Parquet**                    | Columnar storage format              |
| **Docker**                     | Containerized execution environment  |
| **PySpark**                    | Streaming application implementation |

---

# ⚙️ Spark Configuration

| Configuration    | Value                       |
| ---------------- | --------------------------- |
| Spark Version    | `3.5.7`                     |
| Spark Service    | `spark`                     |
| Spark Container  | `stocks-spark`              |
| Spark Master     | `spark://stocks-spark:7077` |
| Application Name | `USStocksStreaming`         |
| Streaming Script | `spark/streaming_job.py`    |

Inside the Spark container:

```text
/opt/spark/work-dir/streaming_job.py
```

---

# 📨 Kafka Input

The streaming application consumes events from:

```text
us-stocks-raw
```

Kafka brokers:

```text
kafka-1:29092
kafka-2:29093
kafka-3:29094
```

Topic configuration:

```text
Partitions: 6
Replication Factor: 3
```

Spark reads the Kafka message value and converts it from binary to a string before parsing the JSON payload.

---

# 🔄 Streaming Workflow

## 1. Kafka Ingestion

The pipeline continuously reads events from the Kafka topic:

```text
us-stocks-raw
```

The Kafka stream is loaded using Spark Structured Streaming.

```text
Kafka
  ↓
Spark readStream
  ↓
JSON message
```

---

## 2. Schema Enforcement

A predefined PySpark `StructType` is used to enforce a consistent schema.

### Stock Event Schema

```text
event_id
ticker
date
open
high
low
close
volume
dividends
stock_splits
stochk_14_3_3
stochd_14_3_3
source_file
produced_at
```

### Main Data Types

| Field         | Type      |
| ------------- | --------- |
| `event_id`    | Long      |
| `ticker`      | String    |
| `date`        | Date      |
| `open`        | Double    |
| `high`        | Double    |
| `low`         | Double    |
| `close`       | Double    |
| `volume`      | Double    |
| `produced_at` | Timestamp |

This guarantees that the incoming stream is processed using a predictable structure and data types.

**Status:** ✅ Verified

---

## 3. Data Validation

Each parsed event is checked before entering the clean layer.

### Required Fields

The following fields must not be `NULL`:

```text
event_id
ticker
date
open
high
low
close
volume
```

### Numerical Validation

The following values must be greater than or equal to zero:

```text
open   >= 0
high   >= 0
low    >= 0
close  >= 0
volume >= 0
```

Each record receives a validation flag:

```text
is_valid = true
```

or:

```text
is_valid = false
```

The stream is then separated into valid and invalid events.

---

## 4. Dead Letter Handling

Invalid events are redirected to a dedicated Kafka topic:

```text
us-stocks-dead-letter
```

This prevents invalid data from contaminating the clean HDFS layer while preserving the rejected events for debugging and monitoring.

### Validation Test

An intentionally invalid event was injected with:

```text
close = -50.0
```

The event was correctly rejected and appeared in:

```text
us-stocks-dead-letter
```

**Status:** ✅ Dead Letter Handling Verified

---

## 5. Clean Data Layer

Valid events are written to HDFS in **Parquet** format.

### HDFS Path

```text
hdfs://namenode:9000/stocks/clean
```

### Partitioning

The clean layer is partitioned by:

```text
date
```

### Directory Structure

```text
/stocks/clean/
├── date=2026-09-09/
│   └── *.snappy.parquet
│
└── date=2026-09-10/
    └── *.snappy.parquet
```

Partitioning by date allows downstream systems to efficiently access data for specific dates.

### Validation

The clean Parquet output was successfully loaded and queried using Spark.

Verified test count:

```text
250 records
```

**Status:** ✅ Clean Parquet Verified

---

## 6. Windowed Aggregation

The valid streaming data is also processed using a **1-minute tumbling window**.

The aggregation is grouped by:

```text
window
ticker
```

For every ticker and every 1-minute window, the pipeline calculates:

### Average Closing Price

```text
avg_close
```

### Total Trading Volume

```text
total_volume
```

### Number of Events

```text
event_count
```

A **2-minute watermark** is configured to handle late-arriving events.

### Aggregation Logic

```text
Valid Events
     │
     ▼
1-Minute Window
     │
     ▼
Group by Ticker
     │
     ├── Average Close
     ├── Total Volume
     └── Event Count
```

---

## 7. Aggregated Data Layer

The aggregated results are stored as Parquet files in HDFS.

### HDFS Path

```text
hdfs://namenode:9000/stocks/aggregated
```

### Partitioning

The aggregation output is partitioned by:

```text
window_date
```

### Directory Structure

```text
/stocks/aggregated/
├── window_date=2026-09-09/
│   └── *.snappy.parquet
│
└── window_date=2026-09-10/
    └── *.snappy.parquet
```

---

# 📊 Aggregated Schema

The aggregated dataset contains:

```text
window
 ├── start : timestamp
 └── end   : timestamp

ticker        : string
avg_close     : double
total_volume  : double
event_count   : long
window_date   : date
```

| Column         | Description                    |
| -------------- | ------------------------------ |
| `window.start` | Start of the 1-minute window   |
| `window.end`   | End of the 1-minute window     |
| `ticker`       | Stock ticker symbol            |
| `avg_close`    | Average closing price          |
| `total_volume` | Total volume in the window     |
| `event_count`  | Number of events in the window |
| `window_date`  | Partition date                 |

Verified test count:

```text
906 aggregated records
```

**Status:** ✅ Aggregated Parquet Verified

---

# 🗂️ HDFS Layout

The Spark module uses the following HDFS structure:

```text
/stocks/
│
├── clean/
│   └── date=YYYY-MM-DD/
│       └── *.parquet
│
├── aggregated/
│   └── window_date=YYYY-MM-DD/
│       └── *.parquet
│
└── checkpoints/
    │
    ├── clean/
    ├── aggregated/
    └── dead-letter/
```

### Main Paths

| Purpose                | HDFS Path                                             |
| ---------------------- | ----------------------------------------------------- |
| Clean Data             | `hdfs://namenode:9000/stocks/clean`                   |
| Aggregated Data        | `hdfs://namenode:9000/stocks/aggregated`              |
| Clean Checkpoint       | `hdfs://namenode:9000/stocks/checkpoints/clean`       |
| Aggregation Checkpoint | `hdfs://namenode:9000/stocks/checkpoints/aggregated`  |
| Dead Letter Checkpoint | `hdfs://namenode:9000/stocks/checkpoints/dead-letter` |
| NameNode               | `hdfs://namenode:9000`                                |

---

# 💾 Checkpointing & Fault Recovery

Checkpointing is implemented using HDFS.

The aggregation checkpoint is:

```text
hdfs://namenode:9000/stocks/checkpoints/aggregated
```

The checkpoint stores Spark's streaming progress and state.

The checkpoint directory contains state files including:

```text
.delta
.snapshot
```

## Fault Recovery Test

The recovery process was tested as follows:

```text
1. Start the Spark Streaming application.
2. Allow the pipeline to process data.
3. Stop the Spark application.
4. Restart the same application.
5. Keep the existing checkpoint.
6. Verify that Spark starts successfully.
7. Verify that new aggregated Parquet files are generated.
```

After restarting the application, new Parquet files appeared in HDFS.

The checkpoint also continued to contain active state files.

Therefore:

> **Checkpoint-Based Fault Recovery: ✅ VERIFIED**

The test confirms that Spark can recover its streaming state using the persisted HDFS checkpoint.

---

# 🔍 Data Validation

Both output layers were successfully read using Spark.

### Clean Layer

```scala
val clean = spark.read.parquet(
  "hdfs://namenode:9000/stocks/clean"
)

clean.printSchema()
clean.show(10, false)
clean.count()
```

Expected verified test count:

```text
250
```

### Aggregated Layer

```scala
val agg = spark.read.parquet(
  "hdfs://namenode:9000/stocks/aggregated"
)

agg.printSchema()
agg.show(10, false)
agg.count()
```

Expected verified test count:

```text
906
```

### Aggregation Date Range

```scala
import org.apache.spark.sql.functions._

agg.select("window_date")
   .agg(
      min("window_date"),
      max("window_date")
   )
   .show()
```

---

# ▶️ Running the Streaming Pipeline

Make sure the Docker services are running before starting Spark.

Run the streaming application with:

```powershell
docker exec -it stocks-spark /opt/spark/bin/spark-submit --master spark://stocks-spark:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/spark/work-dir/streaming_job.py
```

The Kafka connector is:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7
```

Its dependencies are cached under:

```text
/tmp/.ivy2
```

---

# 🧪 Useful HDFS Commands

### Check Clean Output

```powershell
docker exec stocks-namenode hdfs dfs -ls -R /stocks/clean
```

### Check Aggregated Output

```powershell
docker exec stocks-namenode hdfs dfs -ls -R /stocks/aggregated
```

### Check All Checkpoints

```powershell
docker exec stocks-namenode hdfs dfs -ls -R /stocks/checkpoints
```

### Check Aggregation Checkpoint

```powershell
docker exec stocks-namenode hdfs dfs -ls -R /stocks/checkpoints/aggregated
```

---

# 🤝 Handover to M3

The **Spark Streaming and HDFS output layer is complete**.

M3 does **not** need to rebuild the Kafka ingestion or Spark streaming pipeline.

The downstream work can start directly from the generated HDFS Parquet datasets.

### For detailed valid events

Use:

```text
hdfs://namenode:9000/stocks/clean
```

This contains validated individual stock events.

### For real-time aggregated data

Use:

```text
hdfs://namenode:9000/stocks/aggregated
```

This contains:

```text
1-minute window
+
ticker
+
average close
+
total volume
+
event count
```

### Recommended Starting Point

For downstream analytics and dashboards, start with:

```text
/stocks/aggregated
```

For detailed event-level analysis, use:

```text
/stocks/clean
```

---

# ✅ Definition of Done

| Task                             | Status |
| -------------------------------- | :----: |
| Spark cluster configured         |    ✅   |
| Kafka source configured          |    ✅   |
| Canonical schema enforced        |    ✅   |
| JSON parsing implemented         |    ✅   |
| Data validation implemented      |    ✅   |
| Invalid events handled           |    ✅   |
| Dead Letter Topic tested         |    ✅   |
| Windowed aggregation implemented |    ✅   |
| HDFS checkpointing configured    |    ✅   |
| Clean Parquet written            |    ✅   |
| Aggregated Parquet written       |    ✅   |
| HDFS partitioning implemented    |    ✅   |
| Parquet schema validated         |    ✅   |
| Parquet data queryable           |    ✅   |
| Fault recovery tested            |    ✅   |
| Handover documentation           |    ✅   |

---

# 📁 Key Project File

The complete Spark Streaming implementation is contained in:

```text
spark/
└── streaming_job.py
```

The script contains:

```text
Kafka Source
     ↓
Schema
     ↓
Validation
     ↓
Dead Letter Stream
     ↓
Clean Parquet Stream
     ↓
Windowed Aggregation
     ↓
Aggregated Parquet
```

---

## 🎯 Final Status

> **Spark Streaming Module: COMPLETE ✅**

The pipeline successfully provides:

* ⚡ Real-time Kafka ingestion
* 🧩 Schema enforcement
* 🔍 Data-quality validation
* 🚨 Dead Letter handling
* 🗄️ Clean Parquet storage
* 📊 1-minute windowed aggregations
* 💾 HDFS checkpointing
* 🔄 Checkpoint-based fault recovery
* 📦 Partitioned HDFS outputs
* 🔎 Queryable Parquet datasets

**Ready for downstream processing by M3.**
