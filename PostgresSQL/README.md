# 🐘 PostgreSQL Analytics Store — Real-Time US Stocks Data Pipeline

> **PostgreSQL 16 · Docker · Power BI · pgAdmin · SparkSQL JDBC**

This document covers everything about the PostgreSQL layer of the pipeline: credentials, how to connect, all tables, views, indexes, and how to query the data from psql, pgAdmin, and Power BI.

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

PostgreSQL is the final destination for all analytics computed by `analytics_job.py`. Spark reads Parquet from MinIO, runs SparkSQL queries, and writes results here via JDBC. The database is then used by Power BI, pgAdmin, or any SQL client for reporting and dashboards.

```
MinIO Parquet
     │
     ▼
analytics_job.py (Spark)
     │  JDBC  mode=overwrite
     ▼
PostgreSQL  stocks_analytics
     ├── 10 analytics tables
     ├── 8 views
     └── 17 indexes
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

---

## 🧰 Prerequisites & Installation

### What you need

| Requirement | Detail |
|---|---|
| Docker Desktop | v24+ with at least **8 GB RAM** allocated |
| Analytics job ran | `analytics_job.py` must have completed at least once |
| (Optional) pgAdmin | Added to `docker-compose.yml` — see below |
| (Optional) Npgsql driver | Required for Power BI on Windows |

### Install pgAdmin (optional web UI)

Add this to `docker-compose.yml` under `services:`:

```yaml
  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: stocks-pgadmin
    restart: unless-stopped
    networks:
      - stocks-net
    ports:
      - "5050:80"
    environment:
      PGADMIN_DEFAULT_EMAIL:    admin@stocks.com
      PGADMIN_DEFAULT_PASSWORD: admin123
    depends_on:
      postgres:
        condition: service_healthy
```

Then start it:

```powershell
docker compose up -d pgadmin
```

Open **http://localhost:5050**

### Install Npgsql driver (required for Power BI)

1. Go to https://github.com/npgsql/npgsql/releases
2. Download the latest `Npgsql-x.x.x.msi`
3. Run the installer
4. **Restart your PC**
5. Open Power BI Desktop

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

-- Clear screen
\! cls

-- Exit
\q
```

### 2. pgAdmin (web UI)

1. Open **http://localhost:5050**
2. Login: `admin@stocks.com` / `admin123`
3. Right-click **Servers** → **Register** → **Server**
4. Fill in the **General** tab:
   - Name: `US Stocks Pipeline`
5. Fill in the **Connection** tab:

| Field | Value |
|---|---|
| Host name / address | `postgres` |
| Port | `5432` |
| Maintenance database | `stocks_analytics` |
| Username | `stocks` |
| Password | `stocks123` |
| Save password | ✅ Yes |

6. Click **Save**
7. Expand: Servers → US Stocks Pipeline → Databases → stocks_analytics → Schemas → public → Tables

### 3. Power BI

**Step 1 — Install Npgsql driver** (see Prerequisites above — must do this first)

**Step 2 — Connect**

1. Open **Power BI Desktop**
2. Click **Home** → **Get Data** → **More…**
3. Search **PostgreSQL** → click **Connect**
4. Enter connection details:

| Field | Value |
|---|---|
| Server | `localhost:5432` |
| Database | `stocks_analytics` |

5. Click **OK**
6. When prompted, select the **Database** tab and enter:

| Field | Value |
|---|---|
| User name | `stocks` |
| Password | `stocks123` |

7. Click **Connect**
8. In the Navigator, select the tables and views you want:

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
✅ v_top_gainers
✅ v_top_losers
✅ v_high_volatility
✅ v_most_traded
✅ v_dividend_champions
✅ v_overbought_stocks
✅ v_market_trend
✅ v_full_stock_profile
```

9. Click **Load** or **Transform Data**

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

## 🗂 Database Schema

### Tables

#### 1. `stock_summary` — Overall per-ticker statistics

```sql
\d stock_summary
```

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `trading_days` | bigint | Total number of trading days in dataset |
| `first_date` | date | Earliest date in dataset |
| `last_date` | date | Latest date in dataset |
| `first_close` | numeric | Closing price on first date |
| `last_close` | numeric | Closing price on last date |
| `avg_close` | numeric | Average closing price all-time |
| `min_close` | numeric | All-time lowest closing price |
| `max_close` | numeric | All-time highest closing price |
| `avg_open` | numeric | Average opening price |
| `avg_high` | numeric | Average daily high |
| `avg_low` | numeric | Average daily low |
| `total_volume` | bigint | Total shares traded all-time |
| `avg_daily_volume` | numeric | Average shares traded per day |
| `total_dividends` | numeric | Sum of all dividend payments |
| `num_splits` | bigint | Number of stock split events |
| `avg_daily_range` | numeric | Average of (high - low) per day |

---

#### 2. `price_volatility` — Risk and spread metrics

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `close_stddev` | numeric | Standard deviation of closing price |
| `coeff_variation_pct` | numeric | Coefficient of variation % (stddev/mean × 100) |
| `avg_daily_range` | numeric | Average of (high - low) |
| `avg_daily_range_pct` | numeric | Average daily range as % of close |
| `all_time_high` | numeric | Highest high ever recorded |
| `all_time_low` | numeric | Lowest low ever recorded |
| `total_price_range` | numeric | All-time high minus all-time low |
| `green_days` | bigint | Days where close > open |
| `red_days` | bigint | Days where close < open |
| `flat_days` | bigint | Days where close = open |
| `win_rate_pct` | numeric | % of days that were green |

---

#### 3. `monthly_performance` — Monthly OHLCV per ticker

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `year_month` | text | Format: `2024-01` |
| `year` | integer | Calendar year |
| `month` | integer | Calendar month (1–12) |
| `trading_days` | bigint | Trading days in this month |
| `avg_close` | numeric | Average close for the month |
| `monthly_low` | numeric | Lowest low of the month |
| `monthly_high` | numeric | Highest high of the month |
| `month_open` | numeric | Opening price on first day of month |
| `month_close` | numeric | Closing price on last day of month |
| `total_volume` | bigint | Total volume for the month |
| `avg_daily_volume` | numeric | Average daily volume |
| `total_dividends` | numeric | Dividends paid this month |
| `month_return_pct` | numeric | % return = (month_close - month_open) / month_open × 100 |

---

#### 4. `yearly_performance` — Annual OHLCV per ticker

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `year` | integer | Calendar year |
| `trading_days` | bigint | Trading days in this year |
| `avg_close` | numeric | Average close for the year |
| `yearly_low` | numeric | Lowest low of the year |
| `yearly_high` | numeric | Highest high of the year |
| `year_open` | numeric | Opening price on first day of year |
| `year_close` | numeric | Closing price on last day of year |
| `total_volume` | bigint | Total volume for the year |
| `avg_daily_volume` | numeric | Average daily volume |
| `total_dividends` | numeric | Dividends paid this year |
| `num_splits` | bigint | Stock splits this year |
| `year_return_pct` | numeric | % return for the year |

---

#### 5. `top_performers` — All-time % price change

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `first_date` | date | Date of first available price |
| `last_date` | date | Date of last available price |
| `days_held` | integer | Total days between first and last |
| `first_close` | numeric | Closing price on first date |
| `last_close` | numeric | Closing price on last date |
| `absolute_change` | numeric | last_close - first_close |
| `pct_change` | numeric | % change from first to last close |
| `direction` | text | `GAINER`, `LOSER`, or `FLAT` |

---

#### 6. `volume_leaders` — Most traded stocks

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `total_volume` | bigint | All-time total shares traded |
| `avg_daily_volume` | numeric | Average shares traded per day |
| `max_single_day_volume` | bigint | Highest single-day volume |
| `min_single_day_volume` | bigint | Lowest single-day volume |
| `volume_stddev` | numeric | Standard deviation of daily volume |
| `trading_days` | bigint | Total trading days |
| `high_volume_days` | bigint | Days where volume > 2× ticker average |

---

#### 7. `dividend_analysis` — Income and dividend stocks

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `num_dividend_events` | bigint | Number of times a dividend was paid |
| `total_dividends_paid` | numeric | Sum of all dividend amounts |
| `avg_dividend_per_event` | numeric | Average dividend payment |
| `max_single_dividend` | numeric | Largest single dividend payment |
| `first_dividend_date` | date | Date of first dividend |
| `last_dividend_date` | date | Date of most recent dividend |
| `approx_dividend_yield_pct` | numeric | total_dividends / avg_close × 100 |

Only contains tickers that paid at least one dividend.

---

#### 8. `stochastic_signals` — Overbought / oversold analysis

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `rows_with_stoch` | bigint | Rows where stochastic indicators are non-null |
| `overbought_k` | bigint | Days where %K > 80 (overbought) |
| `oversold_k` | bigint | Days where %K < 20 (oversold) |
| `neutral_k` | bigint | Days where %K between 20 and 80 |
| `avg_stochk` | numeric | Average %K value |
| `avg_stochd` | numeric | Average %D value |
| `max_stochk` | numeric | Maximum %K value |
| `min_stochk` | numeric | Minimum %K value |
| `k_above_d` | bigint | Days where %K > %D (bullish signal) |
| `k_below_d` | bigint | Days where %K < %D (bearish signal) |
| `overbought_rate_pct` | numeric | % of days that were overbought |
| `oversold_rate_pct` | numeric | % of days that were oversold |

---

#### 9. `daily_market_breadth` — Market-wide daily snapshot

| Column | Type | Description |
|---|---|---|
| `date` | date | Trading date |
| `active_stocks` | bigint | Number of tickers trading on this day |
| `total_market_volume` | bigint | Sum of volume across all tickers |
| `avg_close` | numeric | Market-wide average close |
| `avg_open` | numeric | Market-wide average open |
| `avg_high` | numeric | Market-wide average high |
| `avg_low` | numeric | Market-wide average low |
| `advancing_stocks` | bigint | Tickers where close > open |
| `declining_stocks` | bigint | Tickers where close < open |
| `unchanged_stocks` | bigint | Tickers where close = open |
| `advance_decline_pct` | numeric | % of stocks that advanced |
| `avg_price_change_pct` | numeric | Market-wide average % price change |
| `total_dividends_paid` | numeric | Total dividends paid market-wide |
| `avg_daily_range` | numeric | Market-wide average (high - low) |
| `overbought_count` | bigint | Tickers with %K > 80 |
| `oversold_count` | bigint | Tickers with %K < 20 |

---

#### 10. `streaming_window_summary` — 1-minute window aggregations

| Column | Type | Description |
|---|---|---|
| `ticker` | text | Stock ticker symbol |
| `window_date` | date | Calendar date of the window |
| `window_start` | timestamp | Start of the 1-minute window |
| `window_end` | timestamp | End of the 1-minute window |
| `avg_close` | numeric | Average close price in the window |
| `total_volume` | bigint | Total volume in the window |
| `event_count` | bigint | Number of events in the window |

---

### Views

Views are created by `post_analytics.sql` after each analytics run.

#### `v_top_gainers` — Top 50 all-time gainers
```sql
SELECT * FROM v_top_gainers LIMIT 10;
```
Filters `top_performers` where `direction = 'GAINER'`, ordered by `pct_change DESC`.

#### `v_top_losers` — Top 50 all-time losers
```sql
SELECT * FROM v_top_losers LIMIT 10;
```
Filters `top_performers` where `direction = 'LOSER'`, ordered by `pct_change ASC`.

#### `v_high_volatility` — Most volatile stocks (CV > 50%)
```sql
SELECT * FROM v_high_volatility LIMIT 10;
```
Joins `price_volatility` with `stock_summary`. Only includes tickers with `coeff_variation_pct > 50`.

#### `v_most_traded` — Top 100 tickers by total volume
```sql
SELECT * FROM v_most_traded LIMIT 10;
```
Joins `volume_leaders` with `stock_summary`, ordered by `total_volume DESC`.

#### `v_dividend_champions` — Dividend stocks sorted by total payout
```sql
SELECT * FROM v_dividend_champions LIMIT 10;
```
Joins `dividend_analysis` with `stock_summary`.

#### `v_overbought_stocks` — Stocks most frequently overbought (rate > 20%)
```sql
SELECT * FROM v_overbought_stocks LIMIT 10;
```
Filters `stochastic_signals` where `overbought_rate_pct > 20`.

#### `v_market_trend` — 30-day rolling advance/decline ratio
```sql
SELECT * FROM v_market_trend LIMIT 30;
```
Adds a 30-day rolling average of `advance_decline_pct` and `avg_price_change_pct` over `daily_market_breadth`.

#### `v_full_stock_profile` — Combined single-row summary per ticker
```sql
SELECT * FROM v_full_stock_profile LIMIT 5;
```
Joins all major tables (`stock_summary`, `price_volatility`, `top_performers`, `dividend_analysis`, `stochastic_signals`) into one wide row per ticker.

---

### Indexes

All indexes are created by `post_analytics.sql`. They are dropped and recreated on each analytics run because Spark JDBC `mode=overwrite` drops the table (and its indexes) before writing.

| Index | Table | Column(s) |
|---|---|---|
| `idx_stock_summary_ticker` | `stock_summary` | `ticker` |
| `idx_stock_summary_total_volume` | `stock_summary` | `total_volume DESC` |
| `idx_stock_summary_avg_close` | `stock_summary` | `avg_close DESC` |
| `idx_price_vol_ticker` | `price_volatility` | `ticker` |
| `idx_price_vol_cv` | `price_volatility` | `coeff_variation_pct DESC` |
| `idx_monthly_ticker_month` | `monthly_performance` | `ticker, year, month` |
| `idx_monthly_year_month` | `monthly_performance` | `year, month` |
| `idx_yearly_ticker_year` | `yearly_performance` | `ticker, year` |
| `idx_top_performers_ticker` | `top_performers` | `ticker` |
| `idx_top_performers_pct` | `top_performers` | `pct_change DESC` |
| `idx_top_performers_direction` | `top_performers` | `direction` |
| `idx_volume_leaders_ticker` | `volume_leaders` | `ticker` |
| `idx_volume_leaders_total` | `volume_leaders` | `total_volume DESC` |
| `idx_dividend_ticker` | `dividend_analysis` | `ticker` |
| `idx_dividend_total` | `dividend_analysis` | `total_dividends_paid DESC` |
| `idx_stoch_ticker` | `stochastic_signals` | `ticker` |
| `idx_stoch_overbought` | `stochastic_signals` | `overbought_k DESC` |
| `idx_daily_breadth_date` | `daily_market_breadth` | `date DESC` |
| `idx_window_ticker` | `streaming_window_summary` | `ticker` |
| `idx_window_start` | `streaming_window_summary` | `window_start DESC` |
| `idx_window_date` | `streaming_window_summary` | `window_date DESC` |

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

-- ── Overbought stocks ────────────────────────────────────────────────────────
SELECT ticker, overbought_k, overbought_rate_pct, avg_stochk
FROM stochastic_signals
ORDER BY overbought_rate_pct DESC
LIMIT 10;

-- ── Row counts for all tables ────────────────────────────────────────────────
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
```

---

## ⚙️ How Data Gets In

Data flows into PostgreSQL only via `analytics_job.py`:

```
spark.sql(query)
  .write
  .format("jdbc")
  .option("url", "jdbc:postgresql://postgres:5432/stocks_analytics")
  .option("user", "stocks")
  .option("password", "stocks123")
  .option("driver", "org.postgresql.Driver")
  .mode("overwrite")   ← drops and recreates each table
  .save()
```

`mode=overwrite` means each analytics run **replaces all data** — tables are dropped and recreated. Run `post_analytics.sql` after every analytics job to restore indexes and views.

---

## 🔧 Maintenance Commands

```powershell
# Connect to psql
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics

# Copy and run post_analytics.sql (restores indexes + views)
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

# List all indexes
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics `
  -c "\di"

# Check active connections
docker exec -it stocks-postgres psql -U stocks -d stocks_analytics `
  -c "SELECT pid, usename, application_name, state FROM pg_stat_activity WHERE datname = 'stocks_analytics';"

# Full reset (delete all data — requires docker compose down -v and restart)
docker compose down -v
docker compose up -d
```

---

## 🐛 Troubleshooting

| Problem | Fix |
|---|---|
| `Connection refused` on port 5432 | Run `docker compose ps` — postgres must show `healthy`. If not: `docker compose up -d postgres` |
| `password authentication failed` | Username is `stocks`, password is `stocks123`, database is `stocks_analytics` — not `postgres` |
| Tables missing / empty | Run `analytics_job.py` first — PostgreSQL starts empty; Spark populates it |
| Views missing after analytics run | Run `post_analytics.sql` — Spark's `mode=overwrite` drops tables and their dependent views |
| Power BI can't connect | Install Npgsql `.msi` driver and **restart your PC** before opening Power BI |
| pgAdmin can't reach postgres | Use host `postgres` (not `localhost`) inside the pgAdmin server registration — they share the `stocks-net` Docker network |
| `stocks_analytics` database not found | The database is created by Docker entrypoint on first boot via `init_postgres.sql` — make sure the container started cleanly: `docker compose logs postgres` |
| Slow queries | Re-run `post_analytics.sql` to restore indexes — Spark drops them on every overwrite |
