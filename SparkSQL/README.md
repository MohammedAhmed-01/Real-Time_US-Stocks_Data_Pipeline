# ⚡ SparkSQL Analytics — Real-Time US Stocks Data Pipeline

> **Apache Spark 3.5.7 · SparkSQL · MinIO (S3) · PostgreSQL · Docker**

This module reads clean Parquet data from MinIO, runs **10 SparkSQL analytics queries**, and writes each result table into PostgreSQL — all containerised and ready to connect to Power BI.

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Prerequisites](#-prerequisites)
- [Credentials & Connection Info](#-credentials--connection-info)
- [What analytics_job.py Does](#-what-analytics_jobpy-does)
- [The 10 Analytics Tables](#-the-10-analytics-tables)
- [Running the Full Stack First](#-running-the-full-stack-first)
- [How to Run the Analytics Job](#-how-to-run-the-analytics-job)
- [Apply Indexes & Views](#-apply-indexes--views)
- [Verify Output in PostgreSQL](#-verify-output-in-postgresql)
- [Web UIs & Ports](#-web-uis--ports)
- [Troubleshooting](#-troubleshooting)

---

## 🔎 Overview

```
MinIO  s3a://stocks/clean/*          ← per-ticker Parquet (from streaming_job.py)
MinIO  s3a://stocks/aggregated/*     ← 1-min windowed Parquet (from streaming_job.py)
         │
         ▼
  analytics_job.py  (SparkSQL)
         │
         ▼
PostgreSQL  stocks_analytics  ← 10 tables + indexes + views
         │
         ▼
  Power BI / pgAdmin / psql
```

---

## 🏗 Architecture

```
┌────────────────────────────────────────────────────────────┐
│  Docker Network: stocks-net                                │
│                                                            │
│  ┌──────────────┐     ┌──────────────────────────────┐    │
│  │    MinIO     │────►│   Spark (analytics_job.py)   │    │
│  │  :9000/:9001 │     │   SparkSQL — 10 queries      │    │
│  └──────────────┘     └──────────────┬───────────────┘    │
│                                      │  JDBC              │
│                                      ▼                    │
│                       ┌──────────────────────────────┐    │
│                       │      PostgreSQL :5432         │    │
│                       │   db: stocks_analytics        │    │
│                       │   user: stocks                │    │
│                       └──────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
```

---

## 🧰 Prerequisites

| Requirement | Detail |
|---|---|
| Docker Desktop | v24+ — allocate **at least 8 GB RAM** |
| All M1 + M2 services running | Kafka, MinIO, Spark, Producer must be up |
| Streaming job ran first | `streaming_job.py` must have written Parquet to MinIO |
| Free host ports | `8081` (Spark UI), `9000/9001` (MinIO), `5432` (Postgres) |

---

## 🔑 Credentials & Connection Info

### MinIO (S3-compatible object store)

| Setting | Value |
|---|---|
| S3 Endpoint (internal) | `http://minio:9000` |
| S3 Endpoint (host) | `http://localhost:9000` |
| Web Console | `http://localhost:9001` |
| Access Key | `minioadmin` |
| Secret Key | `minioadmin` |
| Bucket | `stocks` |
| Clean data path | `s3a://stocks/clean` |
| Aggregated data path | `s3a://stocks/aggregated` |

### PostgreSQL

| Setting | Value |
|---|---|
| Host (internal Docker) | `postgres` |
| Host (from your machine) | `localhost` |
| Port | `5432` |
| Database | `stocks_analytics` |
| Username | `stocks` |
| Password | `stocks123` |
| JDBC URL | `jdbc:postgresql://localhost:5432/stocks_analytics` |

### Spark

| Setting | Value |
|---|---|
| Spark Master UI | `http://localhost:8081` |
| Spark Master URL | `spark://stocks-spark:7077` |
| Submit mode used | `local[4]` (uses 4 cores on the Spark container) |

---

## 📋 What analytics_job.py Does

`spark/analytics_job.py` is a **batch SparkSQL job** — not a streaming job. It:

1. Connects to MinIO via the S3A connector
2. Reads all clean Parquet files from `s3a://stocks/clean` using `mergeSchema=true`
   - This handles the case where some ticker Parquet files were written with `partitionBy("ticker")` (13 columns) and others were not (14 columns)
   - Ticker is injected automatically from the directory partition path
3. Reads aggregated windowed Parquet from `s3a://stocks/aggregated` (if available)
4. Registers both as Spark SQL temp views (`stocks` and `aggregated`)
5. Runs **10 SparkSQL queries** — one per analytics insight
6. Writes each result to a PostgreSQL table via JDBC (`mode=overwrite`)
7. Stops the Spark session

**Key fix applied:** The old version used a manual per-ticker loop + `reduce(DataFrame.union)` which crashed with `NUM_COLUMNS_MISMATCH`. The new version uses a single `spark.read.option("mergeSchema","true").parquet(S3_CLEAN)` call which handles all schema differences automatically.

---

## 📊 The 10 Analytics Tables

| # | Table | Description | Key Columns |
|---|---|---|---|
| 1 | `stock_summary` | Overall per-ticker stats | `ticker`, `trading_days`, `avg_close`, `total_volume`, `first_date`, `last_date` |
| 2 | `price_volatility` | Risk and spread metrics | `ticker`, `coeff_variation_pct`, `all_time_high`, `all_time_low`, `win_rate_pct` |
| 3 | `monthly_performance` | Monthly OHLCV per ticker | `ticker`, `year`, `month`, `month_open`, `month_close`, `month_return_pct` |
| 4 | `yearly_performance` | Annual OHLCV per ticker | `ticker`, `year`, `year_open`, `year_close`, `year_return_pct` |
| 5 | `top_performers` | All-time % price change | `ticker`, `pct_change`, `direction` (GAINER/LOSER/FLAT), `first_close`, `last_close` |
| 6 | `volume_leaders` | Most traded stocks | `ticker`, `total_volume`, `avg_daily_volume`, `high_volume_days` |
| 7 | `dividend_analysis` | Income / dividend stocks | `ticker`, `total_dividends_paid`, `num_dividend_events`, `approx_dividend_yield_pct` |
| 8 | `stochastic_signals` | Overbought / oversold counts | `ticker`, `overbought_k`, `oversold_k`, `overbought_rate_pct` |
| 9 | `daily_market_breadth` | Market-wide daily snapshot | `date`, `active_stocks`, `advancing_stocks`, `advance_decline_pct` |
| 10 | `streaming_window_summary` | 1-min window aggregations | `ticker`, `window_start`, `window_end`, `avg_close`, `total_volume` |

---

## ▶️ Running the Full Stack First

Before running the analytics job, the full pipeline must be up and streaming data into MinIO.

### Step 1 — Start the full stack

```powershell
docker compose up -d
```

### Step 2 — Verify all services are healthy

```powershell
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
stocks-postgres        healthy         0.0.0.0:5432->5432/tcp
```

### Step 3 — Run the Spark Streaming job (fills MinIO with Parquet)

```powershell
docker exec -it stocks-spark `
  /opt/spark/bin/spark-submit `
    --master spark://stocks-spark:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    /opt/spark/work-dir/streaming_job.py
```

Let this run for **at least 5 minutes** so MinIO has enough data. You can verify data is flowing at `http://localhost:9001` → Buckets → stocks → clean.

### Step 4 — Verify MinIO has data

```powershell
docker run --rm --network stocks-net minio/mc `
  alias set local http://minio:9000 minioadmin minioadmin

docker run --rm --network stocks-net minio/mc `
  ls local/stocks/clean/
```

You should see ticker folders like `ticker=AAPL/`, `ticker=MSFT/` etc.

---

## 🚀 How to Run the Analytics Job

Once MinIO has Parquet data, submit the analytics job:

```powershell
docker exec -it stocks-spark `
  /opt/spark/bin/spark-submit `
    --master local[4] `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
    /opt/spark/work-dir/analytics_job.py
```

Expected output (success):

```
✓  Stock Summary                  → table=stock_summary                rows=...
✓  Price Volatility               → table=price_volatility             rows=...
✓  Monthly Performance            → table=monthly_performance          rows=...
✓  Yearly Performance             → table=yearly_performance           rows=...
✓  Top Performers                 → table=top_performers               rows=...
✓  Volume Leaders                 → table=volume_leaders               rows=...
✓  Dividend Analysis              → table=dividend_analysis            rows=...
✓  Stochastic Signals             → table=stochastic_signals           rows=...
✓  Daily Market Breadth           → table=daily_market_breadth         rows=...
✓  Streaming Window Summary       → table=streaming_window_summary     rows=...
Analytics complete in ~40s
```

---

## 🗂 Apply Indexes & Views

After the analytics job finishes, run `post_analytics.sql` to add indexes and views:

```powershell
# Copy the SQL file into the postgres container
docker cp spark/post_analytics.sql stocks-postgres:/tmp/post_analytics.sql

# Run it
docker exec -i stocks-postgres psql -U stocks -d stocks_analytics `
  -f /tmp/post_analytics.sql
```

Expected output:

```
=== Creating indexes ===
=== Creating views ===
=== post_analytics.sql complete ===
```

---

## ✅ Verify Output in PostgreSQL

Connect to PostgreSQL and check:

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics
```

Then inside psql:

```sql
-- List all tables
\dt

-- List all views
\dv

-- Row counts for all tables
SELECT 'stock_summary'       AS tbl, COUNT(*) FROM stock_summary       UNION ALL
SELECT 'price_volatility',          COUNT(*) FROM price_volatility      UNION ALL
SELECT 'monthly_performance',       COUNT(*) FROM monthly_performance   UNION ALL
SELECT 'yearly_performance',        COUNT(*) FROM yearly_performance    UNION ALL
SELECT 'top_performers',            COUNT(*) FROM top_performers        UNION ALL
SELECT 'volume_leaders',            COUNT(*) FROM volume_leaders        UNION ALL
SELECT 'dividend_analysis',         COUNT(*) FROM dividend_analysis     UNION ALL
SELECT 'stochastic_signals',        COUNT(*) FROM stochastic_signals    UNION ALL
SELECT 'daily_market_breadth',      COUNT(*) FROM daily_market_breadth  UNION ALL
SELECT 'streaming_window_summary',  COUNT(*) FROM streaming_window_summary;

-- Quick sample
SELECT ticker, pct_change, direction FROM top_performers ORDER BY pct_change DESC LIMIT 5;

-- Exit
\q
```

---

## 🖥 Web UIs & Ports

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | None |
| Spark Master UI | http://localhost:8081 | None |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | — |
| pgAdmin (if added) | http://localhost:5050 | `admin@stocks.com` / `admin123` |

---

## 🐛 Troubleshooting

| Problem | Fix |
|---|---|
| `NUM_COLUMNS_MISMATCH` on union | Fixed in updated `analytics_job.py` — uses `mergeSchema=true` instead of manual union |
| `Cannot read clean data from MinIO` | Run `streaming_job.py` first; check MinIO at `http://localhost:9001` |
| `Volume Leaders FAILED: window inside aggregate` | Fixed in updated `analytics_job.py` — window function moved into a CTE |
| `Connection refused` to PostgreSQL | Check `docker compose ps` — postgres must be `healthy` |
| `No such file or directory` for post_analytics.sql | Use `docker cp` to copy it into the container first |
| Spark UI shows no active apps | Normal after job completes — check completed apps section |
| Port 4040 already in use | Spark auto-increments to 4041 — this is not an error |

---

## 🔄 Re-running the Analytics Job

The job uses `mode=overwrite` so it is **safe to re-run** at any time. Each run drops and recreates all 10 tables with the latest data from MinIO. Remember to re-run `post_analytics.sql` after each run to restore indexes and views:

```powershell
# Re-run analytics
docker exec -it stocks-spark `
  /opt/spark/bin/spark-submit `
    --master local[4] `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
    /opt/spark/work-dir/analytics_job.py

# Re-apply indexes and views
docker cp spark/post_analytics.sql stocks-postgres:/tmp/post_analytics.sql
docker exec -i stocks-postgres psql -U stocks -d stocks_analytics `
  -f /tmp/post_analytics.sql
```
