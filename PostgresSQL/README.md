# 🐘 PostgreSQL Analytics Store — Real-Time US Stocks Data Pipeline

> **PostgreSQL 16 · Docker · Power BI · pgAdmin · SparkSQL JDBC**

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Credentials](#-credentials)
- [Prerequisites](#-prerequisites)
- [Step 1 — Start PostgreSQL](#step-1--start-postgresql)
- [Step 2 — Connect to PostgreSQL](#step-2--connect-to-postgresql)
- [Step 3 — Run the Analytics Job](#step-3--run-the-analytics-job)
- [Step 4 — Apply Indexes and Views](#step-4--apply-indexes-and-views)
- [Step 5 — Bulk Load the Full Kaggle Dataset](#step-5--bulk-load-the-full-kaggle-dataset)
- [Step 6 — Verify Everything](#step-6--verify-everything)
- [Database Schema](#-database-schema)
- [Useful Queries](#-useful-queries)
- [Maintenance Commands](#-maintenance-commands)
- [Troubleshooting](#-troubleshooting)

---

## 🔎 Overview

PostgreSQL is the final destination for all analytics computed by `analytics_job.py`. Spark reads Parquet from MinIO, runs 10 SparkSQL queries, and writes results here via JDBC. A separate bulk loader (`bulk_load_to_postgres.py`) loads the full raw Kaggle dataset into a `stocks_raw` table for Power BI historical dashboards.

```
MinIO Parquet
     │
     ▼
analytics_job.py  (Spark)
     │  JDBC  mode=overwrite
     ▼
PostgreSQL  stocks_analytics
     ├── 10 analytics tables   ← from Spark analytics job
     ├── stocks_raw            ← from bulk loader (full Kaggle history)
     ├── 8 views
     └── 17+ indexes
     │
     ▼
Power BI / pgAdmin / psql / DBeaver
```

---

## 🔑 Credentials

| Setting | Value |
|---|---|
| Host (from your machine) | `localhost` |
| Host (inside Docker network) | `postgres` |
| Port | `5432` |
| Database | `stocks_analytics` |
| Username | `stocks` |
| Password | `stocks123` |
| JDBC URL | `jdbc:postgresql://localhost:5432/stocks_analytics` |

> When connecting from **inside Docker** (Spark container, pgAdmin), always use `postgres` as the host. When connecting from your **Windows machine** (Power BI, DBeaver, psql), use `localhost`.

---

## 🧰 Prerequisites

| Requirement | Detail |
|---|---|
| Docker Desktop | v24+ with at least **8 GB RAM** allocated |
| Full stack running | `docker compose up -d` must show all services healthy |
| Analytics job ran | `analytics_job.py` must have completed at least once |
| StockHistory CSVs | Kaggle dataset copied into the Spark container at `/tmp/StockHistory` |

---

## Step 1 — Start PostgreSQL

PostgreSQL starts automatically with the full stack. Run this from your project root:

```powershell
docker compose up -d
```

Confirm it is healthy:

```powershell
docker compose ps
```

You should see `stocks-postgres` with status `healthy`. If not, check logs:

```powershell
docker compose logs postgres
```

---

## Step 2 — Connect to PostgreSQL

### Option A — psql in the terminal (no install needed)

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics
```

Useful psql commands once connected:

```sql
\dt          -- list all tables
\dv          -- list all views
\d stocks_raw  -- describe a table
\x           -- toggle expanded display (good for wide rows)
\q           -- exit
```

### Option B — pgAdmin (browser UI)

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

6. Click **Save**

### Option C — Power BI

1. Install the Npgsql driver from https://github.com/npgsql/npgsql/releases → download the `.msi` → install → **restart your PC**
2. Open Power BI Desktop → **Home** → **Get Data** → **More** → search **PostgreSQL** → **Connect**
3. Server: `localhost:5432` — Database: `stocks_analytics` → **OK**
4. Authentication tab → Username: `stocks` → Password: `stocks123` → **Connect**
5. Select the tables and views you want and click **Load**

> Use `localhost` in Power BI (not `postgres`) — Power BI runs on your Windows machine, not inside Docker.

### Option D — DBeaver / DataGrip / TablePlus

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `stocks_analytics` |
| User | `stocks` |
| Password | `stocks123` |
| SSL | Disabled |

---

## Step 3 — Run the Analytics Job

This reads Parquet from MinIO, runs 10 SparkSQL queries, and writes results to PostgreSQL. Run the streaming job first (Step 5 of the main quickstart) so MinIO has data, then:

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

Expected output when successful:

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

## Step 4 — Apply Indexes and Views

Spark's `mode=overwrite` drops and recreates tables without indexes or views. Run this after every analytics job to restore them:

```powershell
docker cp spark/post_analytics.sql stocks-postgres:/tmp/post_analytics.sql
```

```powershell
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

## Step 5 — Bulk Load the Full Kaggle Dataset

This loads every `<TICKER>.csv` from the Kaggle dataset directly into a `stocks_raw` table. This gives Power BI access to the full raw historical data independently of the streaming pipeline.

> **Important:** The bulk loader script (`PostgresSQL/bulk_load_to_postgres.py`) has been updated to fix two bugs from the original version:
> - **Timezone-aware dates** like `1980-12-12 00:00:00-05:00` now parse correctly (`.dt.tz_localize(None).dt.date` instead of `.dt.date`)
> - **`COPY FROM` replaced with `execute_values`** — handles special characters, NaN, and extra CSV columns (RSI, MACD, BB, etc.) without crashing

### 5a — Install Python dependencies inside the Spark container

Only needed once per container lifetime:

```powershell
docker exec -it stocks-spark bash -c "pip3 install --target=/tmp/pylibs psycopg2-binary pandas tqdm"
```

### 5b — Copy the bulk loader script into the container

```powershell
docker cp "PostgresSQL\bulk_load_to_postgres.py" stocks-spark:/tmp/bulk_load_to_postgres.py
```

### 5c — Copy the Kaggle stock CSV files into the container

```powershell
docker cp "C:\Users\moham\Desktop\ETA_FinalProject\Data\archive\Data\StockHistory" stocks-spark:/tmp/StockHistory
```

> This copies 6,000+ CSV files and will take several minutes with no progress bar — wait for the PowerShell prompt to return.

### 5d — Verify the files were copied

```powershell
docker exec -it stocks-spark bash -c "ls /tmp/StockHistory | head -10 && echo '---' && ls /tmp/StockHistory | wc -l"
```

You should see ticker filenames like `AAPL.csv`, `MSFT.csv` and a total count matching your local folder.

### 5e — Test on a single ticker first

Before running the full load, confirm the script works on one ticker:

```powershell
docker exec -it stocks-spark bash -c "PYTHONPATH=/tmp/pylibs python3 /tmp/bulk_load_to_postgres.py --data-dir /tmp/StockHistory --host postgres --port 5432 --db stocks_analytics --user stocks --password stocks123 --ticker AAPL --verbose"
```

Expected output (0 errors):

```
PostgreSQL connection OK.
Files to load : 1
Table 'stocks_raw' is ready.
Loading: 100% | 1/1 [errs=0, rows=11388]
BULK LOAD COMPLETE
  Files with errors: 0
  Rows inserted    : 11388
```

### 5f — Run the full bulk load

```powershell
docker exec -it stocks-spark bash -c "PYTHONPATH=/tmp/pylibs python3 /tmp/bulk_load_to_postgres.py --data-dir /tmp/StockHistory --host postgres --port 5432 --db stocks_analytics --user stocks --password stocks123 --workers 4 --chunk 50000 --drop"
```

Flag reference:

| Flag | Value | Meaning |
|---|---|---|
| `--data-dir` | `/tmp/StockHistory` | Path to CSV files inside the container |
| `--host` | `postgres` | Internal Docker hostname — NOT `localhost` |
| `--workers` | `4` | Parallel file loaders |
| `--chunk` | `50000` | Rows per INSERT batch |
| `--drop` | — | Drop and recreate `stocks_raw` before loading (safe re-run) |

### 5g — Re-running the bulk loader

The loader is always safe to re-run. Use `--drop` to start fresh, or omit it to append (duplicates are skipped via `ON CONFLICT DO NOTHING`):

```powershell
docker exec -it stocks-spark bash -c "PYTHONPATH=/tmp/pylibs python3 /tmp/bulk_load_to_postgres.py --data-dir /tmp/StockHistory --host postgres --port 5432 --db stocks_analytics --user stocks --password stocks123 --workers 4 --chunk 50000 --drop"
```

---

## Step 6 — Verify Everything

### Check all table row counts

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "
SELECT 'stock_summary'             AS tbl, COUNT(*) AS rows FROM stock_summary       UNION ALL
SELECT 'price_volatility',                  COUNT(*) FROM price_volatility            UNION ALL
SELECT 'monthly_performance',               COUNT(*) FROM monthly_performance         UNION ALL
SELECT 'yearly_performance',               COUNT(*) FROM yearly_performance           UNION ALL
SELECT 'top_performers',                    COUNT(*) FROM top_performers              UNION ALL
SELECT 'volume_leaders',                    COUNT(*) FROM volume_leaders              UNION ALL
SELECT 'dividend_analysis',                 COUNT(*) FROM dividend_analysis           UNION ALL
SELECT 'stochastic_signals',                COUNT(*) FROM stochastic_signals          UNION ALL
SELECT 'daily_market_breadth',              COUNT(*) FROM daily_market_breadth        UNION ALL
SELECT 'streaming_window_summary',          COUNT(*) FROM streaming_window_summary    UNION ALL
SELECT 'stocks_raw',                        COUNT(*) FROM stocks_raw;
"
```

### Check the raw data loaded correctly

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "SELECT ticker, COUNT(*) AS rows FROM stocks_raw GROUP BY ticker ORDER BY rows DESC LIMIT 10;"
```

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "SELECT * FROM stocks_raw WHERE ticker='AAPL' ORDER BY date LIMIT 5;"
```

### Check top performers

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "SELECT ticker, pct_change, direction FROM top_performers ORDER BY pct_change DESC LIMIT 10;"
```

---

## 🗂 Database Schema

### Analytics tables (written by `analytics_job.py`)

| Table | Description | Key Columns |
|---|---|---|
| `stock_summary` | Overall per-ticker stats | `ticker`, `trading_days`, `avg_close`, `total_volume` |
| `price_volatility` | Risk and spread metrics | `ticker`, `coeff_variation_pct`, `all_time_high`, `win_rate_pct` |
| `monthly_performance` | Monthly OHLCV per ticker | `ticker`, `year`, `month`, `month_return_pct` |
| `yearly_performance` | Annual OHLCV per ticker | `ticker`, `year`, `year_return_pct` |
| `top_performers` | All-time % price change | `ticker`, `pct_change`, `direction` |
| `volume_leaders` | Most traded stocks | `ticker`, `total_volume`, `high_volume_days` |
| `dividend_analysis` | Income / dividend stocks | `ticker`, `total_dividends_paid`, `approx_dividend_yield_pct` |
| `stochastic_signals` | Overbought / oversold counts | `ticker`, `overbought_k`, `oversold_rate_pct` |
| `daily_market_breadth` | Market-wide daily snapshot | `date`, `advancing_stocks`, `advance_decline_pct` |
| `streaming_window_summary` | 1-minute window aggregations | `ticker`, `window_start`, `avg_close` |

### Raw table (written by `bulk_load_to_postgres.py`)

**`stocks_raw`** — Full Kaggle dataset, one row per ticker per trading day

| Column | Type | Notes |
|---|---|---|
| `id` | bigserial | Auto-incrementing primary key |
| `ticker` | text | e.g. `AAPL` |
| `date` | date | Trading date (timezone stripped on load) |
| `open` | double precision | Opening price |
| `high` | double precision | Session high |
| `low` | double precision | Session low |
| `close` | double precision | Closing price |
| `volume` | double precision | Shares traded |
| `dividends` | double precision | 0.0 on non-event days |
| `stock_splits` | double precision | 0.0 on non-event days |
| `stochk_14_3_3` | double precision | Nullable during indicator warm-up |
| `stochd_14_3_3` | double precision | Nullable during indicator warm-up |
| `source_file` | text | Original CSV filename |
| `loaded_at` | timestamptz | When this row was loaded |

> Extra CSV columns (RSI_14, MACD, BBL, WILLR, OBV, AD, etc.) are present in the Kaggle CSVs but are intentionally ignored by the bulk loader — only the canonical columns above are stored.

### Views (created by `post_analytics.sql`)

| View | Description |
|---|---|
| `v_top_gainers` | Top 50 all-time gainers by % change |
| `v_top_losers` | Top 50 all-time losers by % change |
| `v_high_volatility` | Stocks with coefficient of variation > 50% |
| `v_most_traded` | Top 100 tickers by total volume |
| `v_dividend_champions` | Dividend stocks sorted by total payout |
| `v_overbought_stocks` | Stocks most frequently in overbought territory |
| `v_market_trend` | 30-day rolling advance/decline ratio |
| `v_full_stock_profile` | Combined single-row summary per ticker |

---

## 💡 Useful Queries

```sql
-- Top 10 all-time gainers
SELECT ticker, first_close, last_close, pct_change, direction
FROM top_performers
ORDER BY pct_change DESC
LIMIT 10;

-- Top 10 all-time losers
SELECT ticker, first_close, last_close, pct_change
FROM top_performers
ORDER BY pct_change ASC
LIMIT 10;

-- Most volatile stocks
SELECT ticker, coeff_variation_pct, all_time_high, all_time_low, win_rate_pct
FROM price_volatility
ORDER BY coeff_variation_pct DESC
LIMIT 10;

-- Highest volume stocks
SELECT ticker, total_volume, avg_daily_volume, high_volume_days
FROM volume_leaders
ORDER BY total_volume DESC
LIMIT 10;

-- Best dividend payers
SELECT ticker, total_dividends_paid, num_dividend_events, approx_dividend_yield_pct
FROM dividend_analysis
ORDER BY total_dividends_paid DESC
LIMIT 10;

-- Market trend over time (last 30 days)
SELECT date, active_stocks, advancing_stocks, declining_stocks, advance_decline_pct
FROM daily_market_breadth
ORDER BY date DESC
LIMIT 30;

-- Full profile for one ticker
SELECT * FROM v_full_stock_profile WHERE ticker = 'AAPL';

-- Monthly returns for one ticker
SELECT year, month, month_open, month_close, month_return_pct
FROM monthly_performance
WHERE ticker = 'AAPL'
ORDER BY year, month;

-- Raw historical data for one ticker
SELECT date, open, high, low, close, volume
FROM stocks_raw
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 20;

-- All distinct tickers in stocks_raw
SELECT DISTINCT ticker FROM stocks_raw ORDER BY ticker;

-- Row counts for all tables
SELECT 'stock_summary'            AS tbl, COUNT(*) AS rows FROM stock_summary       UNION ALL
SELECT 'price_volatility',                COUNT(*) FROM price_volatility             UNION ALL
SELECT 'monthly_performance',             COUNT(*) FROM monthly_performance          UNION ALL
SELECT 'yearly_performance',              COUNT(*) FROM yearly_performance           UNION ALL
SELECT 'top_performers',                  COUNT(*) FROM top_performers               UNION ALL
SELECT 'volume_leaders',                  COUNT(*) FROM volume_leaders               UNION ALL
SELECT 'dividend_analysis',               COUNT(*) FROM dividend_analysis            UNION ALL
SELECT 'stochastic_signals',              COUNT(*) FROM stochastic_signals           UNION ALL
SELECT 'daily_market_breadth',            COUNT(*) FROM daily_market_breadth         UNION ALL
SELECT 'streaming_window_summary',        COUNT(*) FROM streaming_window_summary     UNION ALL
SELECT 'stocks_raw',                      COUNT(*) FROM stocks_raw;
```

---

## 🔧 Maintenance Commands

```powershell
# Connect to psql
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics

# Re-apply indexes and views after analytics job
docker cp spark/post_analytics.sql stocks-postgres:/tmp/post_analytics.sql
docker exec -i stocks-postgres psql -U stocks -d stocks_analytics `
  -f /tmp/post_analytics.sql

# Backup the database
docker exec stocks-postgres pg_dump -U stocks stocks_analytics > backup.sql

# Restore from backup
docker exec -i stocks-postgres psql -U stocks -d stocks_analytics < backup.sql

# Check database size
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics `
  -c "SELECT pg_size_pretty(pg_database_size('stocks_analytics'));"

# Check table sizes
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "
  SELECT tablename,
         pg_size_pretty(pg_total_relation_size(tablename::text)) AS size
  FROM pg_tables
  WHERE schemaname = 'public'
  ORDER BY pg_total_relation_size(tablename::text) DESC;"

# Stop everything (keeps data)
docker compose down

# Full reset — deletes all data
docker compose down -v
```

---

## 🐛 Troubleshooting

| Problem | Fix |
|---|---|
| `Connection refused` on port 5432 | Run `docker compose ps` — postgres must show `healthy`. If not: `docker compose up -d postgres` |
| `password authentication failed` from Windows host | Always connect via the Spark container using `--host postgres`, not `127.0.0.1` |
| Tables missing or empty | Run Step 3 (analytics job) — PostgreSQL starts completely empty |
| `stocks_raw` table missing | Run Step 5 (bulk loader) |
| Views missing after analytics run | Run Step 4 (`post_analytics.sql`) — Spark's `mode=overwrite` drops views on every run |
| Bulk loader: `AttributeError: Can only use .dt accessor with datetimelike values` | You are using the old `bulk_load_to_postgres.py`. Replace it with the updated version — the fix is parsing dates with `utc=True` then calling `.dt.tz_localize(None).dt.date` |
| Bulk loader: high error count (~2300/6200) | Same root cause as above — old script used `COPY FROM` which chokes on timezone strings. Replace with the updated script |
| Bulk loader: `No CSV files found` | Check `--data-dir` path: `docker exec -it stocks-spark bash -c "ls /tmp/StockHistory | head"` |
| Bulk loader: duplicate rows on re-run | Add `--drop` flag — the table has a `UNIQUE (ticker, date)` constraint so duplicates are skipped automatically without `--drop` |
| Power BI can't connect | Install Npgsql `.msi` and **restart your PC** before opening Power BI. Use `localhost` not `postgres` as the server |
| pgAdmin can't reach postgres | Use host `postgres` (not `localhost`) in the pgAdmin server registration — they communicate via the internal `stocks-net` Docker network |
| Port 5432 already in use | Stop the conflicting service or change the host-side port in `docker-compose.yml` |
| Slow queries after analytics run | Re-run Step 4 (`post_analytics.sql`) to restore indexes |
