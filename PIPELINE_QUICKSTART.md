# 🚀 Real-Time US Stocks Data Pipeline — Full Run Guide (PowerShell)

> All commands below are written for **Windows PowerShell**. Copy and paste them exactly.
> The backtick `` ` `` is PowerShell's line-continuation character (replaces `\` from bash).

---

## ⚠️ Prerequisites Checklist

Before you start, confirm:

- [ ] Docker Desktop installed (v24+) with **at least 8 GB RAM** allocated (Docker Desktop → Settings → Resources)
- [ ] You have a Kaggle account and API token ([get one here](https://www.kaggle.com/settings) → API → Create New Token)
- [ ] The following host ports are free: `5050`, `5432`, `7077`, `8080`, `8081`, `9000`, `9001`, `9092`, `9093`, `9094`

---

## 🌐 All Localhost URLs (bookmark these)

| Service | URL | Login |
|---|---|---|
| **Kafka UI** | http://localhost:8080 | None |
| **Spark Master UI** | http://localhost:8081 | None |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **MinIO S3 API** | http://localhost:9000 | — |
| **pgAdmin** | http://localhost:5050 | `admin@stocks.com` / `admin123` |
| **PostgreSQL** | `localhost:5432` | user: `stocks` / pass: `stocks123` / db: `stocks_analytics` |

---

## STEP 1 — Clone & Configure

```powershell
# Clone the repo
git clone <your-repo-url>
cd <repo-directory>

# Copy the example env file
Copy-Item env.example .env
```

Open `.env` in any text editor and fill in your Kaggle credentials:

```powershell
notepad .env
```

Replace the placeholder values:

```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

Leave everything else as-is — the MinIO, Kafka, and PostgreSQL defaults work out of the box.

---

## STEP 2 — Build Docker Images

Run this once (or whenever `requirements.txt` or either `Dockerfile` changes):

```powershell
docker compose build
```

This builds two custom images:
- `Dockerfile.python` — Python producer + consumer
- `spark/Dockerfile` — Spark 3.5.7 with all JARs pre-baked (Kafka, S3A, PostgreSQL JDBC)

---

## STEP 3 — Start the Full Stack

```powershell
docker compose up -d
```

Docker Compose starts services in dependency order:

| # | Service | What it does |
|---|---|---|
| 1 | `kafka-1`, `kafka-2`, `kafka-3` | 3-broker KRaft Kafka cluster |
| 2 | `kafka-init` | Creates `us-stocks-raw` (6 partitions) and `us-stocks-dead-letter` topics, then exits |
| 3 | `minio` | S3-compatible object store (replaces HDFS) |
| 4 | `minio-init` | Creates the `stocks` bucket, then exits |
| 5 | `postgres` | PostgreSQL 16 — analytics output store |
| 5 | `producer` | Downloads Kaggle CSVs and streams events to Kafka at 500 rows/s |
| 5 | `consumer` | M1 validation consumer — logs batch stats and schema checks |
| 5 | `kafka-ui` | Kafka monitoring dashboard |
| 5 | `spark` | Spark Master node |
| 5 | `spark-worker` | Spark Worker (4 cores, 4 GB RAM) |
| 5 | `pgadmin` | pgAdmin 4 web UI |

---

## STEP 4 — Verify Everything Is Healthy

```powershell
docker compose ps
```

Expected output (all three brokers must show `healthy`; both init containers must show `exited (0)`):

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
stocks-spark           running         0.0.0.0:7077->7077, 0.0.0.0:8081->8080/tcp
stocks-spark-worker    running
stocks-postgres        healthy         0.0.0.0:5432->5432/tcp
stocks-pgadmin         running         0.0.0.0:5050->80/tcp
```

> ⏳ KRaft leader election can take up to 90 seconds on first boot. If brokers are not healthy yet, wait a moment and run `docker compose ps` again.

Check that events are flowing into Kafka:

```powershell
docker compose logs --tail=20 producer
```

You should see lines like:
```
[PRODUCER]  START  file=AAPL.csv  rows=9876  files=3/6600 done  total=0
[PRODUCER]    AAPL.csv  sent=  50/9876 ( 0.5%)  total=50  elapsed=00:00:01
```

Open **http://localhost:8080** → Topics → `us-stocks-raw` → Messages to confirm events are arriving visually.

---

## STEP 5 — Run the Spark Streaming Job (M2)

This reads from Kafka, validates events, and writes clean Parquet files + windowed aggregations to MinIO.

```powershell
docker exec -it stocks-spark `
  /opt/spark/bin/spark-submit `
    --master spark://stocks-spark:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    /opt/spark/work-dir/streaming_job.py
```

> ⏳ Let this run for **at least 5 minutes** before moving to Step 6, so MinIO has enough data.
> You can verify data is flowing at **http://localhost:9001** → Buckets → stocks → clean.

Watch streaming progress in a separate PowerShell window:

```powershell
docker compose logs -f spark-worker
```

Verify MinIO has data (optional):

```powershell
# Set up mc alias
docker run --rm --network stocks-net minio/mc `
  alias set local http://minio:9000 minioadmin minioadmin

# List clean ticker folders
docker run --rm --network stocks-net minio/mc `
  ls local/stocks/clean/
```

You should see folders like `ticker=AAPL/`, `ticker=MSFT/`, etc.

---

## STEP 6 — Run the SparkSQL Analytics Job (M3)

This reads all Parquet from MinIO, runs 10 analytics queries, and writes results to PostgreSQL.

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

## STEP 7 — Apply PostgreSQL Indexes & Views

Run this after every analytics job to restore indexes and views (Spark's `mode=overwrite` drops them):

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

## STEP 8 — Verify PostgreSQL Data

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics
```

Inside psql, run:

```sql
-- Row counts for all 10 tables
SELECT 'stock_summary'       AS tbl, COUNT(*) AS rows FROM stock_summary       UNION ALL
SELECT 'price_volatility',          COUNT(*) FROM price_volatility              UNION ALL
SELECT 'monthly_performance',       COUNT(*) FROM monthly_performance           UNION ALL
SELECT 'yearly_performance',        COUNT(*) FROM yearly_performance            UNION ALL
SELECT 'top_performers',            COUNT(*) FROM top_performers                UNION ALL
SELECT 'volume_leaders',            COUNT(*) FROM volume_leaders                UNION ALL
SELECT 'dividend_analysis',         COUNT(*) FROM dividend_analysis             UNION ALL
SELECT 'stochastic_signals',        COUNT(*) FROM stochastic_signals            UNION ALL
SELECT 'daily_market_breadth',      COUNT(*) FROM daily_market_breadth          UNION ALL
SELECT 'streaming_window_summary',  COUNT(*) FROM streaming_window_summary;

-- Quick sample — top 5 all-time gainers
SELECT ticker, pct_change, direction FROM top_performers ORDER BY pct_change DESC LIMIT 5;

-- Exit psql
\q
```

---

## 📊 pgAdmin — Connect to PostgreSQL via Browser

1. Open **http://localhost:5050**
2. Login: `admin@stocks.com` / `admin123`
3. Right-click **Servers** → **Register** → **Server**
4. **General** tab → Name: `US Stocks Pipeline`
5. **Connection** tab:

| Field | Value |
|---|---|
| Host name / address | `postgres` ← use this, NOT `localhost` |
| Port | `5432` |
| Maintenance database | `stocks_analytics` |
| Username | `stocks` |
| Password | `stocks123` |
| Save password | ✅ Yes |

6. Click **Save** — all 10 tables and 8 views will appear under Schemas → public → Tables.

---

## 🔄 Re-Running the Analytics Job

The analytics job is safe to re-run at any time (`mode=overwrite` replaces all data). Always follow it with the post-analytics SQL:

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

---

## 🛑 Stopping the Pipeline

```powershell
# Stop all containers — data is preserved in named Docker volumes
docker compose down

# Full reset — deletes all Kafka logs, MinIO Parquet, and PostgreSQL data
docker compose down -v
```

---

## 📋 Useful Log Commands

```powershell
# Watch all services at once
docker compose logs -f

# Producer only (Kaggle → Kafka)
docker compose logs -f producer

# Consumer only (M1 validation)
docker compose logs -f consumer

# Spark worker (streaming job output)
docker compose logs -f spark-worker

# Kafka topic initialisation (one-time run)
docker compose logs kafka-init

# MinIO bucket creation (one-time run)
docker compose logs minio-init

# PostgreSQL
docker compose logs postgres
```

---

## 🐛 Quick Troubleshooting

| Problem | Fix |
|---|---|
| Brokers not healthy after `up` | KRaft election takes up to 90s on first boot — wait and re-run `docker compose ps` |
| `kafka-init` exited with error | Run `docker compose logs kafka-init` — last line should be `Topics ready. Done.` |
| Producer Kaggle auth error | Open `.env` and verify `KAGGLE_USERNAME` and `KAGGLE_KEY` are correct |
| `Cannot read clean data from MinIO` | Step 5 (streaming job) must run first — MinIO starts completely empty |
| Tables missing in PostgreSQL | Run Step 6 (analytics job) — PostgreSQL also starts empty |
| Views missing after analytics run | Run Step 7 (`post_analytics.sql`) — Spark's `mode=overwrite` drops views every run |
| Port already in use | Stop the conflicting service, or change the host-side port in `docker-compose.yml` |
| Spark OOM / worker keeps restarting | Allocate ≥ 8 GB RAM to Docker: Docker Desktop → Settings → Resources → Memory |
| pgAdmin can't reach postgres | Use `postgres` as the host (not `localhost`) — they share the `stocks-net` Docker network |
| PowerShell says "unexpected token" | Make sure each line ends with a backtick `` ` `` and there is **no space after the backtick** |

---

## 🗂️ What Lives Where

| Data | Location |
|---|---|
| Raw Kafka events | Topic `us-stocks-raw` — browse at http://localhost:8080 |
| Invalid events | Topic `us-stocks-dead-letter` — browse at http://localhost:8080 |
| Clean Parquet (per ticker) | `s3a://stocks/clean/<TICKER>/` — browse at http://localhost:9001 |
| Windowed aggregations | `s3a://stocks/aggregated/window_date=YYYY-MM-DD/` — browse at http://localhost:9001 |
| Spark checkpoints | `s3a://stocks/checkpoints/` — stored in MinIO |
| Analytics tables (10) | PostgreSQL `stocks_analytics` — connect at `localhost:5432` or via http://localhost:5050 |
| Analytics views (8) | PostgreSQL `stocks_analytics` — `v_top_gainers`, `v_top_losers`, `v_market_trend`, etc. |
