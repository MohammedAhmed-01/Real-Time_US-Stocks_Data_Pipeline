# 🐘 PostgreSQL Analytics Store — Real-Time US Stocks Data Pipeline

> **PostgreSQL 16 · Docker · Power BI · pgAdmin · SparkSQL JDBC**

This document covers everything about the PostgreSQL layer of the pipeline: credentials, how to connect, all tables, views, indexes, the bulk loader, and how to query the data from psql, pgAdmin, and Power BI.

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Credentials](#-credentials)
- [Prerequisites & Installation](#-prerequisites--installation)
- [Starting PostgreSQL](#-starting-postgresql)
- [Connecting to PostgreSQL](#-connecting-to-postgresql)
  - [psql (terminal)](#1-psql-terminal)
  - [pgAdmin (web UI)](#2-pgadmin-web-ui)
  - [Power BI](#3-power-bi)
  - [DBeaver / TablePlus / DataGrip](#4-dbeaver--tableplus--datagrip)
- [Bulk Loading the Full Kaggle Dataset](#-bulk-loading-the-full-kaggle-dataset)
  - [Why use the Spark container](#why-use-the-spark-container)
  - [Step-by-step bulk load commands](#step-by-step-bulk-load-commands)
  - [Troubleshooting the bulk load](#troubleshooting-the-bulk-load)
- [Database Schema](#-database-schema)
  - [Tables](#tables)
  - [Views](#views)
  - [Indexes](#indexes)
- [Useful Queries](#-useful-queries)
- [How Data Gets In](#-how-data-gets-in)
- [Maintenance Commands](#-maintenance-commands)
- [Troubleshooting](#-troubleshooting)

---

## 🔎 Overview

PostgreSQL is the final destination for all analytics computed by `analytics_job.py`. Spark reads Parquet from MinIO, runs SparkSQL queries, and writes results here via JDBC. The database is also populated by `bulk_load_to_postgres.py` which loads the full raw Kaggle dataset directly into a `stocks_raw` table for use in Power BI historical dashboards.

```
MinIO Parquet
     │
     ▼
analytics_job.py (Spark)
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
| **Host (from your machine)** | `localhost` |
| **Host (inside Docker network)** | `postgres` |
| **Port** | `5432` |
| **Database** | `stocks_analytics` |
| **Username** | `stocks` |
| **Password** | `stocks123` |
| **JDBC URL** | `jdbc:postgresql://localhost:5432/stocks_analytics` |
| **Connection string** | `postgresql://stocks:stocks123@localhost:5432/stocks_analytics` |

> ⚠️ **Important:** When connecting from **inside Docker** (e.g. from the Spark container or pgAdmin), always use `postgres` as the host — not `localhost`. When connecting from your **Windows host machine** (Power BI, DBeaver, psql), use `localhost`.

---

## 🧰 Prerequisites & Installation

### What you need

| Requirement | Detail |
|---|---|
| Docker Desktop | v24+ with at least **8 GB RAM** allocated |
| Full stack running | `docker compose up -d` must be healthy |
| Analytics job ran | `analytics_job.py` must have completed at least once for the 10 analytics tables |
| (Optional) pgAdmin | Already included in `docker-compose.yml` |
| (Optional) Npgsql driver | Required for Power BI on Windows — see Power BI section |

### Install pgAdmin (already in docker-compose.yml)

pgAdmin is already defined in `docker-compose.yml`. It starts automatically with `docker compose up -d`. Open **http://localhost:5050** after startup.

---

## 🚀 Starting PostgreSQL

PostgreSQL starts automatically as part of the full stack:

```powershell
# Start everything
docker compose up -d

# Check postgres is healthy
docker compose ps

# View postgres logs
docker compose logs postgres

# Stop everything
docker compose down

# Stop and delete all data (full reset)
docker compose down -v
```

---

## 🔌 Connecting to PostgreSQL

### 1. psql (terminal)

```powershell
# Connect directly via Docker (no local psql install needed)
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics
```

Once inside psql:

```sql
-- List all tables
\dt

-- List all views
\dv

-- Describe a table's columns
\d stock_summary

-- Toggle expanded vertical display (great for wide rows)
\x

-- Exit
\q
```

### 2. pgAdmin (web UI)

1. Open **http://localhost:5050**
2. Login: `admin@stocks.com` / `admin123`
3. Right-click **Servers** → **Register** → **Server**
4. Fill in the **General** tab — Name: `US Stocks Pipeline`
5. Fill in the **Connection** tab:

| Field | Value |
|---|---|
| Host name / address | `postgres` ← use this inside Docker, NOT `localhost` |
| Port | `5432` |
| Maintenance database | `stocks_analytics` |
| Username | `stocks` |
| Password | `stocks123` |
| Save password | ✅ Yes |

6. Click **Save**

> ⚠️ Always use `postgres` as the host in pgAdmin — it connects via the internal `stocks-net` Docker network, not your Windows host. Using `localhost` will fail.

### 3. Power BI

**Step 1 — Install the Npgsql driver (required — do this before opening Power BI)**

1. Go to https://github.com/npgsql/npgsql/releases
2. Download the latest `Npgsql-x.x.x.msi`
3. Run the installer
4. **Restart your PC** (required — Power BI won't see the driver without a restart)
5. Open Power BI Desktop

**Step 2 — Connect Power BI to PostgreSQL**

1. Click **Home** → **Get Data** → **More…**
2. Search **PostgreSQL** → click **Connect**
3. Enter connection details:

| Field | Value |
|---|---|
| Server | `localhost:5432` |
| Database | `stocks_analytics` |

4. Click **OK**
5. When prompted, select the **Database** tab and enter:

| Field | Value |
|---|---|
| User name | `stocks` |
| Password | `stocks123` |

6. Click **Connect**
7. In the Navigator, select the tables and views you want — recommended selection:

```
✅ stock_summary
✅ price_volatility
✅ monthly_performance
✅ yearly_performance
✅ top_performers
✅ volume_leaders
✅ dividend_analysis
✅ stochastic_signals
✅ daily_market_breadth
✅ streaming_window_summary
✅ stocks_raw              ← full historical data from bulk loader
✅ v_top_gainers
✅ v_top_losers
✅ v_high_volatility
✅ v_most_traded
✅ v_dividend_champions
✅ v_overbought_stocks
✅ v_market_trend
✅ v_full_stock_profile
```

8. Click **Load** or **Transform Data**

> **Power BI tip:** Use `localhost` (not `postgres`) — Power BI runs on your Windows machine, not inside Docker.

### 4. DBeaver / TablePlus / DataGrip

Use these connection settings in any SQL client:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `stocks_analytics` |
| User | `stocks` |
| Password | `stocks123` |
| SSL | Disabled |

---

## 📦 Bulk Loading the Full Kaggle Dataset

`bulk_load_to_postgres.py` loads every `<TICKER>.csv` from the downloaded Kaggle dataset directly into a `stocks_raw` table in PostgreSQL. This table is separate from the 10 analytics tables written by Spark — it gives Power BI access to the full raw historical data without going through the streaming pipeline.

### Why use the Spark container

Connecting to PostgreSQL directly from your Windows host via `127.0.0.1:5432` can fail due to Docker's port proxy caching auth state incorrectly. The most reliable approach is to run the bulk loader **from inside the Spark container**, which connects to PostgreSQL via the internal `stocks-net` Docker network (`host=postgres`) — bypassing the Windows host networking layer entirely.

The Spark container already has Python available, and we install the required libraries into a writable `/tmp/pylibs` directory to avoid permission issues.

### Step-by-step bulk load commands

Run all of these in PowerShell in order:

**Step 1 — Install Python dependencies inside the Spark container**

```powershell
docker exec -it stocks-spark bash -c "pip3 install --target=/tmp/pylibs psycopg2-binary pandas tqdm"
```

This installs `psycopg2-binary`, `pandas`, and `tqdm` into `/tmp/pylibs` inside the container — no root or sudo needed. The `--target` flag writes to a writable directory.

Expected output: several `Downloading ...` lines followed by `Successfully installed ...`

**Step 2 — Copy the bulk loader script into the Spark container**

```powershell
docker cp PostgresSQL\bulk_load_to_postgres.py stocks-spark:/tmp/bulk_load_to_postgres.py
```

This copies the script from your Windows machine into the container's `/tmp/` directory.

**Step 3 — Copy the Kaggle stock data into the Spark container**

```powershell
docker cp "C:\Users\moham\Desktop\ETA_FinalProject\Data\archive\Data\StockHistory" stocks-spark:/tmp/StockHistory
```

> ⏳ This copies 6,000+ CSV files and will take several minutes. You will see no progress bar — wait for the PowerShell prompt to return.

**Step 4 — Verify the data was copied**

```powershell
docker exec -it stocks-spark bash -c "ls /tmp/StockHistory | head -20 && echo '---' && ls /tmp/StockHistory | wc -l"
```

You should see ticker filenames like `AAPL.csv`, `MSFT.csv` and a total file count matching your local folder.

**Step 5 — Run the bulk loader**

```powershell
docker exec -it stocks-spark bash -c "PYTHONPATH=/tmp/pylibs python3 /tmp/bulk_load_to_postgres.py --data-dir /tmp/StockHistory --host postgres --port 5432 --db stocks_analytics --user stocks --password stocks123 --workers 4 --chunk 100000"
```

Key flags explained:

| Flag | Value | Meaning |
|---|---|---|
| `--data-dir` | `/tmp/StockHistory` | Path to the CSV files inside the container |
| `--host` | `postgres` | Internal Docker network hostname — NOT `localhost` |
| `--port` | `5432` | PostgreSQL port |
| `--db` | `stocks_analytics` | Target database |
| `--user` | `stocks` | Database username |
| `--password` | `stocks123` | Database password |
| `--workers` | `4` | Parallel file loaders (raise to 8 on machines with more RAM) |
| `--chunk` | `100000` | Rows per COPY batch (raise to 500000 if you have lots of RAM) |

Expected output:

```
[BULK-LOAD] INFO  PostgreSQL connection OK.
[BULK-LOAD] INFO  Table 'stocks_raw' is ready.
Loading: 100%|████████████████| 6227/6227 [file]
[BULK-LOAD] INFO  Creating indexes on 'stocks_raw' ...
[BULK-LOAD] INFO  Indexes created.
[BULK-LOAD] INFO  ============================================================
[BULK-LOAD] INFO  BULK LOAD COMPLETE
[BULK-LOAD] INFO  Files processed : 6227
[BULK-LOAD] INFO  Files skipped   : ...
[BULK-LOAD] INFO  Rows inserted   : ...
[BULK-LOAD] INFO  Rows in DB now  : ...
[BULK-LOAD] INFO  Wall time       : ... s
```

**Step 6 — Verify the data is in PostgreSQL**

```powershell
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "SELECT COUNT(*) FROM stocks_raw;"
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "SELECT ticker, COUNT(*) AS rows FROM stocks_raw GROUP BY ticker ORDER BY rows DESC LIMIT 10;"
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics -c "SELECT * FROM stocks_raw WHERE ticker='AAPL' ORDER BY date LIMIT 5;"
```

**Step 7 — Re-run the bulk loader (safe to repeat)**

The bulk loader uses `CREATE TABLE IF NOT EXISTS` — it will append to existing data. If you want a clean reload, add `--drop`:

```powershell
docker exec -it stocks-spark bash -c "PYTHONPATH=/tmp/pylibs python3 /tmp/bulk_load_to_postgres.py --data-dir /tmp/StockHistory --host postgres --port 5432 --db stocks_analytics --user stocks --password stocks123 --workers 4 --chunk 100000 --drop"
```

The `--drop` flag drops and recreates `stocks_raw` before loading — use this if you want to avoid duplicate rows from a previous run.

**Load a single ticker for testing:**

```powershell
docker exec -it stocks-spark bash -c "PYTHONPATH=/tmp/pylibs python3 /tmp/bulk_load_to_postgres.py --data-dir /tmp/StockHistory --host postgres --port 5432 --db stocks_analytics --user stocks --password stocks123 --ticker AAPL"
```

### Troubleshooting the bulk load

| Problem | Fix |
|---|---|
| `pip3 install` fails with permission error | Use `--target=/tmp/pylibs` as shown above — avoids system permission issues |
| `docker cp` of StockHistory takes very long | Normal — 6000+ files takes several minutes; wait for the PowerShell prompt |
| `Cannot connect to PostgreSQL` from host | Use `--host postgres` (internal Docker name), not `127.0.0.1` |
| `password authentication failed` from host | Known Docker proxy issue — always connect via the Spark container as shown above |
| `No CSV files found` | Check `--data-dir` path; verify with `docker exec -it stocks-spark bash -c "ls /tmp/StockHistory | head"` |
| Files skipped with "missing required columns" | Normal for warrant/unit variants that have different CSV structures |
| Rows duplicated on re-run | Add `--drop` flag to start fresh |

---

## 🗂 Database Schema

### Tables

#### Pipeline analytics tables (written by `analytics_job.py`)

| Table | Description |
|---|---|
| `stock_summary` | Overall per-ticker stats |
| `price_volatility` | Risk and spread metrics |
| `monthly_performance` | Monthly OHLCV per ticker |
| `yearly_performance` | Annual OHLCV per ticker |
| `top_performers` | All-time % price change |
| `volume_leaders` | Most traded stocks |
| `dividend_analysis` | Income / dividend stocks |
| `stochastic_signals` | Overbought / oversold counts |
| `daily_market_breadth` | Market-wide daily snapshot |
| `streaming_window_summary` | 1-minute window aggregations |

#### Raw historical table (written by `bulk_load_to_postgres.py`)

**`stocks_raw`** — Full Kaggle dataset, one row per ticker per trading day

| Column | Type | Description |
|---|---|---|
| `id` | bigserial | Auto-incrementing primary key |
| `ticker` | text | Stock ticker symbol (e.g. `AAPL`) |
| `date` | date | Trading date |
| `open` | double precision | Opening price |
| `high` | double precision | Session high |
| `low` | double precision | Session low |
| `close` | double precision | Closing price |
| `volume` | double precision | Shares traded |
| `dividends` | double precision | Dividend amount (0.0 on non-event days) |
| `stock_splits` | double precision | Split ratio (0.0 on non-event days) |
| `stochk_14_3_3` | double precision | Stochastic %K (null during warm-up) |
| `stochd_14_3_3` | double precision | Stochastic %D (null during warm-up) |
| `source_file` | text | Original CSV filename |
| `loaded_at` | timestamptz | When this row was loaded |

### Views

Views are created by `post_analytics.sql` after each analytics run.

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

### Indexes

All indexes are created by `post_analytics.sql`. They are dropped and recreated on each analytics run because Spark JDBC `mode=overwrite` drops the table and its indexes before writing.

The `stocks_raw` table has its own permanent indexes created by the bulk loader:

| Index | Table | Column(s) |
|---|---|---|
| `stocks_raw_ticker_idx` | `stocks_raw` | `ticker` |
| `stocks_raw_date_idx` | `stocks_raw` | `date DESC` |
| `stocks_raw_ticker_date` | `stocks_raw` | `ticker, date` |
| `stocks_raw_close_idx` | `stocks_raw` | `close` |

---

## 💡 Useful Queries

```sql
-- ── Top 10 all-time gainers ──────────────────────────────────────────────────
SELECT ticker, first_close, last_close, pct_change, direction
FROM top_performers
ORDER BY pct_change DESC
LIMIT 10;

-- ── Top 10 all-time losers ───────────────────────────────────────────────────
SELECT ticker, first_close, last_close, pct_change
FROM top_performers
ORDER BY pct_change ASC
LIMIT 10;

-- ── Most volatile stocks ─────────────────────────────────────────────────────
SELECT ticker, coeff_variation_pct, all_time_high, all_time_low, win_rate_pct
FROM price_volatility
ORDER BY coeff_variation_pct DESC
LIMIT 10;

-- ── Highest volume stocks ────────────────────────────────────────────────────
SELECT ticker, total_volume, avg_daily_volume, high_volume_days
FROM volume_leaders
ORDER BY total_volume DESC
LIMIT 10;

-- ── Best dividend payers ─────────────────────────────────────────────────────
SELECT ticker, total_dividends_paid, num_dividend_events, approx_dividend_yield_pct
FROM dividend_analysis
ORDER BY total_dividends_paid DESC
LIMIT 10;

-- ── Market trend over time ───────────────────────────────────────────────────
SELECT date, active_stocks, advancing_stocks, declining_stocks, advance_decline_pct
FROM daily_market_breadth
ORDER BY date DESC
LIMIT 30;

-- ── Full profile for a specific ticker ───────────────────────────────────────
SELECT * FROM v_full_stock_profile WHERE ticker = 'AAPL';

-- ── Monthly return for a ticker ──────────────────────────────────────────────
SELECT year, month, month_open, month_close, month_return_pct
FROM monthly_performance
WHERE ticker = 'AAPL'
ORDER BY year, month;

-- ── Raw historical data for a ticker (from bulk loader) ──────────────────────
SELECT date, open, high, low, close, volume
FROM stocks_raw
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 20;

-- ── All tickers in stocks_raw ────────────────────────────────────────────────
SELECT DISTINCT ticker FROM stocks_raw ORDER BY ticker;

-- ── Row counts for all tables ────────────────────────────────────────────────
SELECT 'stock_summary'              AS tbl, COUNT(*) AS rows FROM stock_summary       UNION ALL
SELECT 'price_volatility',                  COUNT(*) FROM price_volatility             UNION ALL
SELECT 'monthly_performance',               COUNT(*) FROM monthly_performance          UNION ALL
SELECT 'yearly_performance',                COUNT(*) FROM yearly_performance           UNION ALL
SELECT 'top_performers',                    COUNT(*) FROM top_performers               UNION ALL
SELECT 'volume_leaders',                    COUNT(*) FROM volume_leaders               UNION ALL
SELECT 'dividend_analysis',                 COUNT(*) FROM dividend_analysis            UNION ALL
SELECT 'stochastic_signals',                COUNT(*) FROM stochastic_signals           UNION ALL
SELECT 'daily_market_breadth',              COUNT(*) FROM daily_market_breadth         UNION ALL
SELECT 'streaming_window_summary',          COUNT(*) FROM streaming_window_summary     UNION ALL
SELECT 'stocks_raw',                        COUNT(*) FROM stocks_raw;
```

---

## ⚙️ How Data Gets In

### Via Spark analytics job (`analytics_job.py`)

```python
spark.sql(query)
  .write
  .format("jdbc")
  .option("url", "jdbc:postgresql://postgres:5432/stocks_analytics")
  .option("user", "stocks")
  .option("password", "stocks123")
  .option("driver", "org.postgresql.Driver")
  .mode("overwrite")   # drops and recreates each table
  .save()
```

`mode=overwrite` means each analytics run **replaces all data** in the 10 analytics tables. Run `post_analytics.sql` after every analytics job to restore indexes and views.

### Via bulk loader (`bulk_load_to_postgres.py`)

Uses PostgreSQL `COPY` via `psycopg2` — the fastest possible bulk insert method. Reads CSVs in parallel across multiple workers and streams rows directly into PostgreSQL without buffering the entire file in memory. The `stocks_raw` table is independent of the Spark pipeline tables and is safe to run at any time.

---

## 🔧 Maintenance Commands

```powershell
# Connect to psql
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics

# Copy and run post_analytics.sql (restores indexes + views after analytics job)
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

# Check active connections
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics `
  -c "SELECT pid, usename, application_name, state FROM pg_stat_activity WHERE datname = 'stocks_analytics';"

# Full reset (delete all data)
docker compose down -v
docker compose up -d
```

---

## 🐛 Troubleshooting

| Problem | Fix |
|---|---|
| `Connection refused` on port 5432 | Run `docker compose ps` — postgres must show `healthy`. If not: `docker compose up -d postgres` |
| `password authentication failed` from Windows host | Known Docker proxy issue — run the bulk loader from inside the Spark container using `--host postgres` as documented above |
| `password authentication failed` even after ALTER USER | Run: `docker exec -it stocks-postgres psql -U stocks -d postgres -c "SET password_encryption='md5'; ALTER USER stocks WITH ENCRYPTED PASSWORD 'stocks123';"` then `docker compose restart postgres` |
| Tables missing / empty | Run `analytics_job.py` first — PostgreSQL starts empty; Spark populates it |
| `stocks_raw` missing | Run the bulk loader steps documented in this README |
| Views missing after analytics run | Run `post_analytics.sql` — Spark's `mode=overwrite` drops tables and their dependent views |
| Power BI can't connect | Install Npgsql `.msi` driver and **restart your PC** before opening Power BI. Use `localhost` not `postgres` as the server |
| pgAdmin can't reach postgres | Use host `postgres` (not `localhost`) in the pgAdmin server registration |
| `stocks_analytics` database not found | Database is created on first boot via `init_postgres.sql` — check: `docker compose logs postgres` |
| Slow queries | Re-run `post_analytics.sql` to restore indexes — Spark drops them on every overwrite |
| Bulk loader: `No CSV files found` | Verify the `--data-dir` path matches where you copied the data inside the container |
| Bulk loader: duplicate rows | Add `--drop` flag to drop and recreate `stocks_raw` before loading |
| `pip3 install` permission denied in Spark container | Always use `--target=/tmp/pylibs` and set `PYTHONPATH=/tmp/pylibs` when running the script |
