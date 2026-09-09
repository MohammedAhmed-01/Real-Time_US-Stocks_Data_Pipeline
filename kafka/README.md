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

- [📋 Table of Contents](#-table-of-contents)
- [🆕 Changelog](#-changelog)
  - [`producer.py` — fixed a slow-download stall caused by missing tickers (2026-09-09)](#producerpy--fixed-a-slow-download-stall-caused-by-missing-tickers-2026-09-09)
- [🧭 Getting Started on a New Machine — What You MUST Check/Change](#-getting-started-on-a-new-machine--what-you-must-checkchange)
- [🔍 Overview](#-overview)
- [🏗 Architecture](#-architecture)
  - [Data Flow](#data-flow)
- [📁 Project Structure](#-project-structure)
- [⚙️ Kafka Cluster Configuration](#️-kafka-cluster-configuration)
  - [Broker Port Mapping](#broker-port-mapping)
- [📨 Topic Design](#-topic-design)
  - [`us-stocks-raw` — Primary Ingest Topic](#us-stocks-raw--primary-ingest-topic)
  - [`us-stocks-dead-letter` — Failed Events](#us-stocks-dead-letter--failed-events)
- [🗂 Event Schema](#-event-schema)
  - [Field Reference](#field-reference)
- [🧾 What is Stock\_List.csv and how is it used?](#-what-is-stock_listcsv-and-how-is-it-used)
- [🧰 Prerequisites](#-prerequisites)
- [🚀 Quick Start](#-quick-start)
  - [1 — Clone \& configure](#1--clone--configure)
  - [2 — Start the full stack](#2--start-the-full-stack)
  - [2a — Start only what you need (recommended for M2 Spark work)](#2a--start-only-what-you-need-recommended-for-m2-spark-work)
  - [3 — Watch the logs](#3--watch-the-logs)
  - [4 — Open Kafka UI](#4--open-kafka-ui)
  - [5 — Stop everything](#5--stop-everything)
- [🏃 Running the Pipeline](#-running-the-pipeline)
  - [Producer — local (outside Docker)](#producer--local-outside-docker)
  - [Consumer — local (outside Docker)](#consumer--local-outside-docker)
  - [Producer CLI Reference](#producer-cli-reference)
  - [Consumer CLI Reference](#consumer-cli-reference)
- [🖥 Web UIs \& Monitoring](#-web-uis--monitoring)
  - [Kafka UI — `http://localhost:8080`](#kafka-ui--httplocalhost8080)
- [📤 Producer Details](#-producer-details)
  - [Speed Control](#speed-control)
  - [Kafka Producer Settings](#kafka-producer-settings)
  - [Dataset Source](#dataset-source)
  - [Handling missing tickers (404s) — not a bug](#handling-missing-tickers-404s--not-a-bug)
- [📥 Consumer / Validator Details](#-consumer--validator-details)
  - [Kafka Consumer Settings](#kafka-consumer-settings)
- [👥 Consumer Groups](#-consumer-groups)
- [🔧 Environment Variables](#-environment-variables)
- [✅ Validation Rules](#-validation-rules)
- [🔗 M1 → M2 Handover Reference](#-m1--m2-handover-reference)
  - [Key Values for Spark Structured Streaming](#key-values-for-spark-structured-streaming)
- [⚡ M2 Spark Structured Streaming — Start Here](#-m2-spark-structured-streaming--start-here)
  - [Step 1 — Ensure M1 is running](#step-1--ensure-m1-is-running)
  - [Step 2 — Spark Dependencies (Maven packages)](#step-2--spark-dependencies-maven-packages)
  - [Step 3 — Connect to Kafka and Read the Stream](#step-3--connect-to-kafka-and-read-the-stream)
  - [Spark ↔ Kafka Connection Reference](#spark--kafka-connection-reference)
- [🐛 Troubleshooting](#-troubleshooting)

---

## 🆕 Changelog

### `producer.py` — fixed a slow-download stall caused by missing tickers (2026-09-09)

**Symptom:** the producer would appear to "hang" for minutes at a time on certain
tickers (e.g. `AACBR`, `AACIU`), logging repeated lines like:

```
[DL] REST attempt 7/10 failed for Data/StockHistory/AACIU.csv: 404 Client Error: Not Found
[DL] Retrying Data/StockHistory/AACIU.csv in 30 s …
```

**Root cause:** `Stock_List.csv` contains a broader universe of ticker symbols than
what actually exists as files in the Kaggle dataset
(`footballjoe789/us-stock-dataset`). When a symbol has no corresponding CSV in the
dataset, Kaggle correctly returns **HTTP 404**. The old code treated a 404 exactly
like a transient failure (network blip, rate limit) and retried it **10 times**
with a growing backoff (5s → 30s), wasting **2–5 minutes per missing ticker**.
Across thousands of ticker/warrant/unit variants in `Stock_List.csv` that Kaggle
doesn't have, this added up to a large chunk of wasted wall-clock time.

**Fix:** added a `KaggleFileNotFound` exception. A 404 (from either the `kaggle`
CLI or the REST fallback) is now treated as **permanent** and is skipped
**immediately**, with a single `INFO`-level log line — no retries wasted. Genuine
transient errors (HTTP 429 rate limits, timeouts, network errors) still retry
with backoff exactly as before.

- Skipped files are now split into `not_found_files` (404 — ticker doesn't exist
  in the dataset) vs other `skipped_files` (empty file, missing required columns)
  in the final producer summary, so you can tell the two apart.
- No CLI flags, environment variables, or file locations changed — this is a
  drop-in replacement for `kafka/producer.py`.

---

## 🧭 Getting Started on a New Machine — What You MUST Check/Change

If you're cloning this repo onto a new machine (or handing it off to a
teammate), these are the things that are **environment-specific** and will
break silently if you don't check them:

| # | What | Where | What to do |
|---|---|---|---|
| 1 | **Kaggle credentials** | `.env` (copy from `env.example`) | Replace `KAGGLE_USERNAME` / `KAGGLE_KEY` with your own — the values checked into `env.example` are placeholders and will not work for you. Get yours at <https://www.kaggle.com/settings> → API → Create New Token. |
| 2 | **`.env` is real secrets — don't commit it** | repo root | `.env` is loaded by both `producer.py` and `consumer.py` via `python-dotenv`. Never commit your real `.env`; only `env.example` should be tracked. |
| 3 | **`Stock_List.csv` path** | repo root **and** `kafka/Stock_List.csv` | The producer looks for the ticker list at `/app/Stock_List.csv` **inside the container**, which Docker Compose provides via the `./kafka:/app` volume mount — so the file must physically live at `kafka/Stock_List.csv` on your host. If you're running the producer **outside Docker**, pass the path explicitly: `python kafka/producer.py --stock-list kafka/Stock_List.csv` (or wherever your copy lives). See [What is Stock_List.csv](#-what-is-stock_listcsv-and-how-is-it-used) below. |
| 4 | **Kafka bootstrap address** | `.env` → `KAFKA_BOOTSTRAP` | Inside Docker (`docker compose up`), this is auto-injected as `kafka-1:29092,kafka-2:29093,kafka-3:29094` — you don't need to touch it. Running the producer/consumer **from your host machine** instead, use `localhost:9092,localhost:9093,localhost:9094`. |
| 5 | **Host ports free** | `docker-compose.yml` | `8080` (Kafka UI), `9092`/`9093`/`9094` (brokers). If any are already in use on your machine, either stop the conflicting service or change the host-side port mapping in `docker-compose.yml`. |
| 6 | **Docker resources** | Docker Desktop → Settings → Resources | Allocate **at least 8 GB RAM** — three Kafka brokers are memory-hungry and will restart/crash-loop if starved. |
| 7 | **`producer.py` code changes are picked up without rebuilding the image** | `docker-compose.yml` | The `./kafka:/app` volume mount means edits to `producer.py` / `consumer.py` take effect on the next `docker compose up -d --force-recreate producer` — you do **not** need `--build` unless you changed `requirements.txt` or `Dockerfile.python`. |
| 8 | **Expect some 404s from Kaggle — this is normal** | producer logs | `Stock_List.csv` is a broader ticker universe than the Kaggle dataset actually contains. You'll see `[DL] ... not in dataset — skipping (no retries).` for some symbols — that's expected (see the Changelog above) and not something to "fix". |

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
Stock_List.csv (ticker universe)
    │
    │  build_file_list_from_csv()
    ▼
Kaggle Dataset (footballjoe789/us-stock-dataset)
    │
    │  kaggle CLI / REST API  — 404s from tickers not in the dataset
    │  are skipped immediately (KaggleFileNotFound), no retries wasted
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
├── Stock_List.csv            # (repo root copy, if kept here) ticker universe — see below
└── kafka/
    ├── producer.py           # Kaggle → Kafka streaming producer
    ├── consumer.py           # Validating micro-batch consumer
    ├── topic_config.py       # Single source of truth: topic specs & event schema
    └── Stock_List.csv        # ← the copy actually mounted into the containers (./kafka:/app)
```

> ⚠️ Note there are two copies of `Stock_List.csv` in this repo (root and
> `kafka/`). The one that matters at runtime is **`kafka/Stock_List.csv`**,
> because that's the directory Docker Compose mounts to `/app` inside the
> producer container. If you update your ticker list, update
> **`kafka/Stock_List.csv`** (and keep the root copy in sync if you want a
> reference outside Docker).

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

## 🧾 What is Stock_List.csv and how is it used?

`Stock_List.csv` is a **single-column CSV** (header `Symbol`) listing the
universe of US stock ticker symbols the producer will attempt to stream —
currently ~6,600 symbols, covering common stock, ETFs, warrants (`...W`),
units (`...U`), and other share-class variants.

**Why it exists:** the Kaggle dataset (`footballjoe789/us-stock-dataset`)
doesn't expose a reliable, paginated way to list every file it contains.
Instead of querying Kaggle's file listing API (slow and unreliable at this
scale), `producer.py`:

1. Reads every symbol from `Stock_List.csv` via `build_file_list_from_csv()`.
2. Builds the expected Kaggle path for each one:
   `Data/StockHistory/<SYMBOL>.csv`
3. Hands that full list to the download thread, which fetches each file
   from Kaggle (CLI first, REST fallback second) and pushes the resulting
   DataFrame onto a queue for the main thread to stream into Kafka.

**Important caveat:** `Stock_List.csv` is a *broader* ticker universe than
what Kaggle's dataset actually contains. Some symbols in the list — often
warrant/unit/rights variants like `AACBR`, `AACIU`, `AAPG` — have **no
matching file** in the dataset and will 404 when requested. As of the fix
above, this is handled gracefully: the producer logs it once at `INFO`
level and moves on immediately, it does **not** retry or crash.

**Where the file lives / which copy is used:**
- `kafka/Stock_List.csv` — this is the one that matters. It's mounted into
  the producer container at `/app/Stock_List.csv` via the `./kafka:/app`
  volume in `docker-compose.yml`, which is the producer's default
  `--stock-list` path.
- `Stock_List.csv` (repo root) — a convenience copy for reference /
  running outside Docker from the repo root.

**To use a different or filtered ticker list:**
```bash
# Docker: edit kafka/Stock_List.csv directly, then recreate the container
docker compose up -d --force-recreate producer

# Local (outside Docker): point at any CSV with a Symbol/Ticker column
python kafka/producer.py --stock-list path/to/my_tickers.csv
```

The CSV loader (`build_file_list_from_csv`) also accepts `symbol`,
`Ticker`, or `ticker` as the column header, or falls back to the first
column if none of those match.

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

Open `.env` and fill in your **own** Kaggle credentials (the values in
`env.example` are placeholders and will not work):

```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

Also confirm `kafka/Stock_List.csv` exists — it's required by the producer
(see [What is Stock_List.csv](#-what-is-stock_listcsv-and-how-is-it-used)).

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

# Default: reads kafka/Stock_List.csv is NOT the default outside Docker —
# you must point at it explicitly (the /app/Stock_List.csv default only
# applies inside the container).
python kafka/producer.py --stock-list kafka/Stock_List.csv

# Stream only one ticker symbol
python kafka/producer.py --stock-list kafka/Stock_List.csv --ticker AAPL --rows-per-sec 1

# Override the target topic
python kafka/producer.py --stock-list kafka/Stock_List.csv --topic my-topic --rows-per-sec 10
```

### Consumer — local (outside Docker)

```bash
# Default settings (batch=100 messages, max-wait=5 s, idle-timeout=120 s)
python kafka/consumer.py

# Print every valid message as it arrives
python kafka/consumer.py --live

# Larger batches, faster flush interval
python kafka/consumer.py --batch-size 500 --max-wait-ms 1000
```

### Producer CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--rows-per-sec` | `1.0` | Rows produced per second (controls replay speed) |
| `--batch-size` | `50` | Rows per Kafka micro-batch |
| `--ticker` | *(all tickers)* | Stream only this ticker symbol |
| `--topic` | `us-stocks-raw` | Override the target Kafka topic |
| `--stock-list` | `/app/Stock_List.csv` (Docker default) | Path to the ticker CSV. **Outside Docker, you must pass this explicitly** — e.g. `--stock-list kafka/Stock_List.csv`. |

### Consumer CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--batch-size` | `100` | Messages per processing batch (size trigger) |
| `--max-wait-ms` | `5000` | Max milliseconds before a time-triggered flush |
| `--idle-timeout` | `120` | Seconds all partitions must stay at EOF before the consumer exits |
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

The producer runs in **steady real-time mode** using two coordinated threads:

| Thread | Role |
|---|---|
| **Download thread** | Downloads CSV files from Kaggle one at a time (per the ticker list built from `Stock_List.csv`); pushes `(filename, DataFrame)` pairs to an in-memory queue (max 5 files buffered). Retries each *transient* failure up to 10 times with backoff; skips 404s (file not in dataset) immediately without retrying — see [Changelog](#-changelog). |
| **Main thread** | Pops files from the queue, converts each row to a JSON event, and produces to Kafka at a precise controlled rate set by `--rows-per-sec`. |

A `next_row_time` clock enforces the rate accurately at the individual row level — not per batch — so the pace is consistent even for small files. The producer **never stops early**; it runs until every file in the dataset has been fully streamed (or permanently skipped).

### Speed Control

The `--rows-per-sec` flag controls exactly how many rows are produced per second. Pair it with `--batch-size` for best throughput:

| `--rows-per-sec` | `--batch-size` | Use case |
|---|---|---|
| `1` | `50` | Slow — see every message arrive in Kafka UI |
| `10` | `50` | Moderate — good for development and testing |
| `100` | `100` | Fast |
| `1000` | `500` | Very fast — bulk loading |
| `10000` | `1000` | Maximum speed — as fast as the network allows |

To change speed without restarting the cluster, edit `docker-compose.yml` and run:

```bash
docker compose up -d --force-recreate producer
```

### Kafka Producer Settings

| Setting | Value | Reason |
|---|---|---|
| `acks` | `all` | Strongest durability — leader + all ISR replicas confirm write |
| `retries` | `10` | Automatic retry on transient broker errors |
| `retry.backoff.ms` | `1000` | Pause between retries |
| `linger.ms` | `20` | Accumulate small messages into larger batches |
| `batch.size` | `65536` | 64 KB produce batch |
| `compression.type` | `lz4` | Fast compression, matches topic-level config |
| `enable.idempotence` | `true` | Exactly-once semantics on the producer side |
| `max.in.flight.requests.per.connection` | `5` | Safe with idempotence enabled |

### Dataset Source

| Detail | Value |
|---|---|
| Kaggle dataset | `footballjoe789/us-stock-dataset` |
| File pattern matched | `Data/StockHistory/<TICKER>.csv` |
| Required columns | `date`, `open`, `high`, `low`, `close`, `volume` |
| Ticker derivation | From the CSV filename — `AAPL.csv` → `ticker = "AAPL"` |
| Ticker universe | `Stock_List.csv` (~6,600 symbols) — see [What is Stock_List.csv](#-what-is-stock_listcsv-and-how-is-it-used) |

### Handling missing tickers (404s) — not a bug

Because `Stock_List.csv` includes symbols Kaggle doesn't actually have a
file for, you will see log lines like:

```
[DL] Data/StockHistory/AACIU.csv not in dataset — skipping (no retries).
```

This is **expected** and does not indicate a pipeline failure — it means
that specific symbol simply isn't present in the Kaggle dataset. It's
counted separately in the final summary (`not-in-dataset` vs other
`skipped`). See the [Changelog](#-changelog) for the fix that made this
fail fast instead of retrying for minutes per ticker.

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
| `max.poll.interval.ms` | `600000` (10 minutes — producer can be slow) |
| `session.timeout.ms` | `60000` (60 seconds) |
| `fetch.min.bytes` | `1` |
| `fetch.wait.max.ms` | `1000` |

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
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", CONSUMER_GROUP) \
    .option("failOnDataLoss", "false") \
    .load()

# raw_stream columns: key (binary), value (binary), topic, partition, offset,
#                     timestamp (Kafka ingest time), timestampType
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

**Producer logs `not in dataset — skipping` repeatedly for many tickers**

> This is expected — see [Handling missing tickers (404s)](#handling-missing-tickers-404s--not-a-bug) and the [Changelog](#-changelog). It means `Stock_List.csv` includes symbols that Kaggle's dataset doesn't have files for; they're skipped immediately, not retried.

**Producer looks "stuck" on one ticker for minutes**

> If you're running an older copy of `producer.py` (before the fix in the [Changelog](#-changelog)), 404s were retried 10× with backoff, costing minutes per missing ticker. Update to the current `kafka/producer.py` and recreate the container: `docker compose up -d --force-recreate producer`.

**Consumer shows `Topic not found` and keeps retrying**

> The consumer retries for up to 10 minutes (120 × 5 s). Ensure `kafka-init` completed successfully:
> ```bash
> docker compose logs kafka-init
> ```
> The final line should read `Topics ready. Done.`

**Port already in use on startup**

> Check for conflicting services on ports `8080`, `9092`, `9093`, `9094` and stop them, or edit the host-side port mappings in `docker-compose.yml`.

**Out of memory / brokers restarting**

> Allocate at least 8 GB RAM to Docker in Docker Desktop → Settings → Resources. Three Kafka brokers are memory-intensive.