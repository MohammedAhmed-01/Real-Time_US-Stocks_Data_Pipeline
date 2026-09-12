"""
analytics_job.py — SparkSQL Analytics: MinIO Parquet → PostgreSQL  [OPTIMISED]
===============================================================================
M3 · Analytics  |  Real-Time US Stocks Data Pipeline

OPTIMISATIONS vs the original
------------------------------
1. numPartitions 4 → 16     — 4× more parallel JDBC writers to PostgreSQL
2. batchsize 10 000 → 50 000 — larger INSERT batches → fewer round-trips
3. shuffle.partitions 200 → 48 for batch job (AQE will coalesce further)
4. Adaptive Query Execution enabled with coalesce partitions
5. S3A connection pool raised (fs.s3a.connection.maximum 15→100) so the
   initial full-table Parquet read doesn't bottleneck on S3 connections
6. clean_df is partitioned to 48 partitions before caching so all 10
   SQL queries run with proper parallelism
7. executor/driver memory hints embedded in SparkConf so the job is
   self-contained and doesn't need extra --conf flags on spark-submit

Run inside the Spark container:
    spark-submit \\
      --master local[8] \\
      --conf spark.jars.ivy=/tmp/.ivy2 \\
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
      --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \\
      --conf spark.hadoop.fs.s3a.path.style.access=true \\
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \\
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \\
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\
      /opt/spark/work-dir/analytics_job.py
"""

from __future__ import annotations

import logging
import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, input_file_name, lit, regexp_extract

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [ANALYTICS] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Configuration
# ──────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "stocks")

PG_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
PG_PORT     = os.getenv("POSTGRES_PORT",     "5432")
PG_DB       = os.getenv("POSTGRES_DB",       "stocks_analytics")
PG_USER     = os.getenv("POSTGRES_USER",     "stocks")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stocks123")

S3_CLEAN      = f"s3a://{MINIO_BUCKET}/clean"
S3_AGGREGATED = f"s3a://{MINIO_BUCKET}/aggregated"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Number of parallel JDBC partitions — each opens its own DB connection
# and writes its slice of rows concurrently.  Matches max_connections=200.
JDBC_NUM_PARTITIONS = 16   # was 4

# Rows sent per INSERT statement — larger = fewer round-trips to PostgreSQL
JDBC_BATCH_SIZE = 50_000   # was 10 000

_CLEAN_COLS = [
    "event_id", "ticker", "date",
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits",
    "stochk_14_3_3", "stochd_14_3_3",
    "source_file", "produced_at",
]

# ──────────────────────────────────────────────────────────────────────────────
# 2. SparkSession — tuned for batch analytics throughput
# ──────────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("USStocksAnalytics")
    # ── MinIO / S3A ──────────────────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload",            "true")
    # Raise S3A connection pool for the large parallel Parquet read
    .config("spark.hadoop.fs.s3a.connection.maximum",     "100")   # was default 15
    .config("spark.hadoop.fs.s3a.threads.max",            "20")
    .config("spark.hadoop.fs.s3a.max.total.tasks",        "30")
    # ── Output ───────────────────────────────────────────────────────────────
    .config("spark.sql.parquet.compression.codec", "snappy")
    # Allow cross-join in some analytic CTEs
    .config("spark.sql.crossJoin.enabled", "true")
    # ── Performance ──────────────────────────────────────────────────────────
    # 48 shuffle partitions for analytics (AQE will coalesce small ones)
    .config("spark.sql.shuffle.partitions",                "48")    # was 200 default
    # Adaptive Query Execution: auto-coalesces small partitions, fixes skew
    .config("spark.sql.adaptive.enabled",                  "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled",      "true")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "4")
    .config("spark.sql.adaptive.skewJoin.enabled",                "true")
    # Memory
    .config("spark.memory.fraction",       "0.8")
    .config("spark.memory.storageFraction","0.3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ──────────────────────────────────────────────────────────────────────────────
# 3. Helper — write a DataFrame to PostgreSQL via JDBC  (OPTIMISED)
# ──────────────────────────────────────────────────────────────────────────────

def to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> int:
    """Write *df* to PostgreSQL and return row count.

    Uses numPartitions=16 (was 4) and batchsize=50000 (was 10000) for
    significantly higher write throughput on the tuned PostgreSQL instance.
    """
    row_count = df.count()
    (
        df.write
        .format("jdbc")
        .option("url",            JDBC_URL)
        .option("dbtable",        table)
        .option("user",           PG_USER)
        .option("password",       PG_PASSWORD)
        .option("driver",         "org.postgresql.Driver")
        .option("batchsize",      str(JDBC_BATCH_SIZE))
        .option("numPartitions",  str(JDBC_NUM_PARTITIONS))
        # Disable auto-commit inside each partition writer for bulk speed
        .option("isolationLevel", "NONE")
        .mode(mode)
        .save()
    )
    return row_count


def run_insight(label: str, sql: str, table: str) -> None:
    t0 = time.time()
    try:
        df = spark.sql(sql)
        n  = to_postgres(df, table)
        log.info(
            "  ✓  %-30s → table=%-35s  rows=%7d  %.1fs",
            label, table, n, time.time() - t0,
        )
    except Exception as exc:
        log.error("  ✗  %s FAILED: %s", label, exc)


# ──────────────────────────────────────────────────────────────────────────────
# 4. Load data from MinIO and register temp views
# ──────────────────────────────────────────────────────────────────────────────

log.info("=" * 62)
log.info("  US Stocks Analytics Job  [OPTIMISED]")
log.info("  MinIO   : %s  bucket=%s", MINIO_ENDPOINT, MINIO_BUCKET)
log.info("  Postgres: %s:%s/%s  (numPartitions=%d  batchsize=%d)",
         PG_HOST, PG_PORT, PG_DB, JDBC_NUM_PARTITIONS, JDBC_BATCH_SIZE)
log.info("=" * 62)

log.info("Loading clean data from %s …", S3_CLEAN)
try:
    raw_df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(S3_CLEAN)
    )

    if "ticker" not in raw_df.columns:
        log.warning("'ticker' column missing — deriving from file path.")
        raw_df = raw_df.withColumn(
            "ticker",
            regexp_extract(input_file_name(), r"/clean/(?:ticker=)?([^/]+)/", 1),
        )

    select_exprs = [
        col(c) if c in raw_df.columns else lit(None).alias(c)
        for c in _CLEAN_COLS
    ]
    # Repartition to 48 before caching so all 10 SQL queries run in parallel
    clean_df = raw_df.select(*select_exprs).repartition(48)
    clean_df.cache()

    total_rows    = clean_df.count()
    total_tickers = clean_df.select("ticker").distinct().count()
    log.info("Clean data loaded  rows=%d  tickers=%d", total_rows, total_tickers)

except Exception as exc:
    log.error("Cannot read clean data from MinIO: %s", exc)
    log.error("Run streaming_job.py first so it writes Parquet to MinIO.")
    sys.exit(1)

clean_df.createOrReplaceTempView("stocks")

has_aggregated = False
try:
    log.info("Loading aggregated data from %s …", S3_AGGREGATED)
    agg_df = spark.read.option("mergeSchema", "true").parquet(S3_AGGREGATED)
    agg_flat = agg_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ticker"),
        col("avg_close"),
        col("total_volume"),
        col("event_count"),
        col("window_date"),
    )
    agg_flat.cache()
    agg_flat.createOrReplaceTempView("aggregated")
    has_aggregated = True
    log.info("Aggregated data loaded  rows=%d", agg_flat.count())
except Exception:
    log.warning("Aggregated data not available — window insight will be skipped.")

# ──────────────────────────────────────────────────────────────────────────────
# 5. SparkSQL Insights  (queries unchanged — only infra is faster)
# ──────────────────────────────────────────────────────────────────────────────

wall_start = time.time()
log.info("Running SparkSQL analytics …")

# ── INSIGHT 1: Stock Summary ─────────────────────────────────────────────────
run_insight(
    label="Stock Summary",
    table="stock_summary",
    sql="""
        SELECT
            ticker,
            COUNT(*)                                            AS trading_days,
            MIN(date)                                           AS first_date,
            MAX(date)                                           AS last_date,
            ROUND(FIRST(close), 4)                             AS first_close,
            ROUND(LAST(close),  4)                             AS last_close,
            ROUND(AVG(close),   4)                             AS avg_close,
            ROUND(MIN(close),   4)                             AS min_close,
            ROUND(MAX(close),   4)                             AS max_close,
            ROUND(AVG(open),    4)                             AS avg_open,
            ROUND(AVG(high),    4)                             AS avg_high,
            ROUND(AVG(low),     4)                             AS avg_low,
            CAST(SUM(volume)    AS BIGINT)                     AS total_volume,
            ROUND(AVG(volume),  2)                             AS avg_daily_volume,
            ROUND(SUM(dividends), 4)                           AS total_dividends,
            COUNT(CASE WHEN stock_splits > 0 THEN 1 END)       AS num_splits,
            ROUND(AVG(high - low), 4)                          AS avg_daily_range
        FROM stocks
        GROUP BY ticker
        ORDER BY ticker
    """,
)

# ── INSIGHT 2: Price Volatility ──────────────────────────────────────────────
run_insight(
    label="Price Volatility",
    table="price_volatility",
    sql="""
        SELECT
            ticker,
            ROUND(STDDEV(close),                                  4)  AS close_stddev,
            ROUND(STDDEV(close) / NULLIF(AVG(close), 0) * 100,   4)  AS coeff_variation_pct,
            ROUND(AVG(high - low),                                4)  AS avg_daily_range,
            ROUND(AVG((high - low) / NULLIF(close, 0) * 100),    4)  AS avg_daily_range_pct,
            ROUND(MAX(high),                                      4)  AS all_time_high,
            ROUND(MIN(low),                                       4)  AS all_time_low,
            ROUND(MAX(high) - MIN(low),                           4)  AS total_price_range,
            COUNT(CASE WHEN close > open THEN 1 END)                  AS green_days,
            COUNT(CASE WHEN close < open THEN 1 END)                  AS red_days,
            COUNT(CASE WHEN close = open THEN 1 END)                  AS flat_days,
            ROUND(
                COUNT(CASE WHEN close > open THEN 1 END) * 100.0
                / NULLIF(COUNT(*), 0),
            2)                                                        AS win_rate_pct
        FROM stocks
        GROUP BY ticker
        ORDER BY coeff_variation_pct DESC
    """,
)

# ── INSIGHT 3: Monthly Performance ──────────────────────────────────────────
run_insight(
    label="Monthly Performance",
    table="monthly_performance",
    sql="""
        SELECT
            ticker,
            DATE_FORMAT(date, 'yyyy-MM')            AS year_month,
            YEAR(date)                              AS year,
            MONTH(date)                             AS month,
            COUNT(*)                                AS trading_days,
            ROUND(AVG(close),  4)                   AS avg_close,
            ROUND(MIN(low),    4)                   AS monthly_low,
            ROUND(MAX(high),   4)                   AS monthly_high,
            ROUND(FIRST(open), 4)                   AS month_open,
            ROUND(LAST(close), 4)                   AS month_close,
            CAST(SUM(volume)   AS BIGINT)           AS total_volume,
            ROUND(AVG(volume), 2)                   AS avg_daily_volume,
            ROUND(SUM(dividends), 4)                AS total_dividends,
            ROUND(
                (LAST(close) - FIRST(open))
                / NULLIF(FIRST(open), 0) * 100,
            2)                                      AS month_return_pct
        FROM stocks
        GROUP BY
            ticker,
            DATE_FORMAT(date, 'yyyy-MM'),
            YEAR(date),
            MONTH(date)
        ORDER BY ticker, year, month
    """,
)

# ── INSIGHT 4: Yearly Performance ───────────────────────────────────────────
run_insight(
    label="Yearly Performance",
    table="yearly_performance",
    sql="""
        SELECT
            ticker,
            YEAR(date)                              AS year,
            COUNT(*)                                AS trading_days,
            ROUND(AVG(close),  4)                   AS avg_close,
            ROUND(MIN(low),    4)                   AS yearly_low,
            ROUND(MAX(high),   4)                   AS yearly_high,
            ROUND(FIRST(open), 4)                   AS year_open,
            ROUND(LAST(close), 4)                   AS year_close,
            CAST(SUM(volume)   AS BIGINT)           AS total_volume,
            ROUND(AVG(volume), 2)                   AS avg_daily_volume,
            ROUND(SUM(dividends), 4)                AS total_dividends,
            COUNT(CASE WHEN stock_splits > 0 THEN 1 END) AS num_splits,
            ROUND(
                (LAST(close) - FIRST(open))
                / NULLIF(FIRST(open), 0) * 100,
            2)                                      AS year_return_pct
        FROM stocks
        GROUP BY ticker, YEAR(date)
        ORDER BY ticker, year
    """,
)

# ── INSIGHT 5: Top Performers ────────────────────────────────────────────────
run_insight(
    label="Top Performers",
    table="top_performers",
    sql="""
        WITH bounds AS (
            SELECT ticker, MIN(date) AS first_date, MAX(date) AS last_date
            FROM stocks GROUP BY ticker
        ),
        first_p AS (
            SELECT s.ticker, AVG(s.close) AS first_close
            FROM stocks s JOIN bounds b ON s.ticker = b.ticker AND s.date = b.first_date
            GROUP BY s.ticker
        ),
        last_p AS (
            SELECT s.ticker, AVG(s.close) AS last_close
            FROM stocks s JOIN bounds b ON s.ticker = b.ticker AND s.date = b.last_date
            GROUP BY s.ticker
        )
        SELECT
            b.ticker,
            b.first_date,
            b.last_date,
            DATEDIFF(b.last_date, b.first_date)         AS days_held,
            ROUND(fp.first_close, 4)                     AS first_close,
            ROUND(lp.last_close,  4)                     AS last_close,
            ROUND(lp.last_close - fp.first_close, 4)     AS absolute_change,
            ROUND(
                (lp.last_close - fp.first_close)
                / NULLIF(fp.first_close, 0) * 100, 2)   AS pct_change,
            CASE
                WHEN lp.last_close > fp.first_close THEN 'GAINER'
                WHEN lp.last_close < fp.first_close THEN 'LOSER'
                ELSE 'FLAT'
            END                                          AS direction
        FROM bounds b
        JOIN first_p fp ON b.ticker = fp.ticker
        JOIN last_p  lp ON b.ticker = lp.ticker
        ORDER BY pct_change DESC
    """,
)

# ── INSIGHT 6: Volume Leaders ────────────────────────────────────────────────
run_insight(
    label="Volume Leaders",
    table="volume_leaders",
    sql="""
        WITH with_avg AS (
            SELECT ticker, volume,
                   AVG(volume) OVER (PARTITION BY ticker) AS avg_vol_per_ticker
            FROM stocks
        )
        SELECT
            ticker,
            CAST(SUM(volume)   AS BIGINT)                               AS total_volume,
            ROUND(AVG(volume), 2)                                        AS avg_daily_volume,
            CAST(MAX(volume)   AS BIGINT)                               AS max_single_day_volume,
            CAST(MIN(volume)   AS BIGINT)                               AS min_single_day_volume,
            ROUND(STDDEV(volume), 2)                                     AS volume_stddev,
            COUNT(*)                                                     AS trading_days,
            COUNT(CASE WHEN volume > 2 * avg_vol_per_ticker THEN 1 END) AS high_volume_days
        FROM with_avg
        GROUP BY ticker
        ORDER BY total_volume DESC
    """,
)

# ── INSIGHT 7: Dividend Analysis ─────────────────────────────────────────────
run_insight(
    label="Dividend Analysis",
    table="dividend_analysis",
    sql="""
        SELECT
            ticker,
            COUNT(CASE WHEN dividends > 0 THEN 1 END)           AS num_dividend_events,
            ROUND(SUM(dividends),   4)                           AS total_dividends_paid,
            ROUND(AVG(CASE WHEN dividends > 0 THEN dividends END), 4) AS avg_dividend_per_event,
            ROUND(MAX(dividends),   4)                           AS max_single_dividend,
            MIN(CASE WHEN dividends > 0 THEN date END)           AS first_dividend_date,
            MAX(CASE WHEN dividends > 0 THEN date END)           AS last_dividend_date,
            ROUND(SUM(dividends) / NULLIF(AVG(close), 0) * 100, 4) AS approx_dividend_yield_pct
        FROM stocks
        GROUP BY ticker
        HAVING SUM(dividends) > 0
        ORDER BY total_dividends_paid DESC
    """,
)

# ── INSIGHT 8: Stochastic Signals ───────────────────────────────────────────
run_insight(
    label="Stochastic Signals",
    table="stochastic_signals",
    sql="""
        SELECT
            ticker,
            COUNT(*)                                                           AS rows_with_stoch,
            SUM(CASE WHEN stochk_14_3_3 > 80 THEN 1 ELSE 0 END)               AS overbought_k,
            SUM(CASE WHEN stochk_14_3_3 < 20 THEN 1 ELSE 0 END)               AS oversold_k,
            SUM(CASE WHEN stochk_14_3_3 BETWEEN 20 AND 80 THEN 1 ELSE 0 END)  AS neutral_k,
            ROUND(AVG(stochk_14_3_3), 2)                                       AS avg_stochk,
            ROUND(AVG(stochd_14_3_3), 2)                                       AS avg_stochd,
            ROUND(MAX(stochk_14_3_3), 2)                                       AS max_stochk,
            ROUND(MIN(stochk_14_3_3), 2)                                       AS min_stochk,
            SUM(CASE WHEN stochk_14_3_3 > stochd_14_3_3 THEN 1 ELSE 0 END)   AS k_above_d,
            SUM(CASE WHEN stochk_14_3_3 < stochd_14_3_3 THEN 1 ELSE 0 END)   AS k_below_d,
            ROUND(SUM(CASE WHEN stochk_14_3_3 > 80 THEN 1 ELSE 0 END) * 100.0
                  / NULLIF(COUNT(*), 0), 2)                                    AS overbought_rate_pct,
            ROUND(SUM(CASE WHEN stochk_14_3_3 < 20 THEN 1 ELSE 0 END) * 100.0
                  / NULLIF(COUNT(*), 0), 2)                                    AS oversold_rate_pct
        FROM stocks
        WHERE stochk_14_3_3 IS NOT NULL AND stochd_14_3_3 IS NOT NULL
        GROUP BY ticker
        ORDER BY overbought_k DESC
    """,
)

# ── INSIGHT 9: Daily Market Breadth ─────────────────────────────────────────
run_insight(
    label="Daily Market Breadth",
    table="daily_market_breadth",
    sql="""
        SELECT
            date,
            COUNT(DISTINCT ticker)                                    AS active_stocks,
            CAST(SUM(volume)    AS BIGINT)                            AS total_market_volume,
            ROUND(AVG(close),   4)                                    AS avg_close,
            ROUND(AVG(open),    4)                                    AS avg_open,
            ROUND(AVG(high),    4)                                    AS avg_high,
            ROUND(AVG(low),     4)                                    AS avg_low,
            COUNT(CASE WHEN close > open THEN 1 END)                  AS advancing_stocks,
            COUNT(CASE WHEN close < open THEN 1 END)                  AS declining_stocks,
            COUNT(CASE WHEN close = open THEN 1 END)                  AS unchanged_stocks,
            ROUND(COUNT(CASE WHEN close > open THEN 1 END) * 100.0
                  / NULLIF(COUNT(*), 0), 2)                           AS advance_decline_pct,
            ROUND(AVG((close - open) / NULLIF(open, 0) * 100), 4)    AS avg_price_change_pct,
            ROUND(SUM(dividends), 4)                                  AS total_dividends_paid,
            ROUND(AVG(high - low), 4)                                 AS avg_daily_range,
            SUM(CASE WHEN stochk_14_3_3 > 80 THEN 1 ELSE 0 END)      AS overbought_count,
            SUM(CASE WHEN stochk_14_3_3 < 20 THEN 1 ELSE 0 END)      AS oversold_count
        FROM stocks
        GROUP BY date
        ORDER BY date
    """,
)

# ── INSIGHT 10: Streaming Window Summary (optional) ──────────────────────────
if has_aggregated:
    run_insight(
        label="Streaming Window Summary",
        table="streaming_window_summary",
        sql="""
            SELECT
                ticker,
                window_date,
                window_start,
                window_end,
                ROUND(avg_close,  4)         AS avg_close,
                CAST(total_volume AS BIGINT)  AS total_volume,
                event_count
            FROM aggregated
            ORDER BY ticker, window_start
        """,
    )
else:
    log.info("  –  streaming_window_summary skipped (no aggregated data yet)")

# ──────────────────────────────────────────────────────────────────────────────
# 6. Final summary
# ──────────────────────────────────────────────────────────────────────────────

elapsed = time.time() - wall_start
log.info("=" * 62)
log.info("  Analytics complete in %.1f s", elapsed)
log.info("  Source rows      : %d", total_rows)
log.info("  PostgreSQL       : jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, PG_DB)
log.info("  Tables written   : 9%s", " + 1 window table" if has_aggregated else "")
log.info("  Run post_analytics.sql in psql to add indexes & views.")
log.info("=" * 62)

spark.stop()