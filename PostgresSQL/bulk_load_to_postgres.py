"""
bulk_load_to_postgres.py — Load full Kaggle US Stocks dataset → PostgreSQL
===========================================================================
Reads every <TICKER>.csv from the downloaded Kaggle dataset and bulk-loads
them into PostgreSQL table  stocks_raw  (separate from the pipeline tables).

✅ Safe to run while the Kafka/Spark pipeline is still running — it writes
   to its OWN table (stocks_raw) and never touches the pipeline's tables.
✅ Power BI can connect directly to stocks_raw for the full historical view.
✅ Uses PostgreSQL COPY (via psycopg2) — fastest possible bulk insert.

Data path  : C:\\Users\\moham\\Desktop\\ETA_FinalProject\\Data\\archive\\Data\\StockHistory
Script path: C:\\Users\\moham\\Desktop\\ETA_FinalProject\\Real-Time_US-Stocks_Data_Pipeline\\PostgresSQL\\bulk_load_to_postgres.py

Run from the project root:
----------------------------------------------------------------------
cd C:\\Users\\moham\\Desktop\\ETA_FinalProject\\Real-Time_US-Stocks_Data_Pipeline

pip install -r requirements.txt

python PostgresSQL\\bulk_load_to_postgres.py
----------------------------------------------------------------------

Optional flags:
    --drop              DROP + recreate stocks_raw before loading (clean slate)
    --ticker  AAPL      Load only one ticker (quick test before full run)
    --workers 8         More parallel workers = faster (needs more RAM)
    --chunk   100000    Rows per COPY batch (raise on machines with lots of RAM)
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import psycopg2
from tqdm import tqdm

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [BULK-LOAD] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
# These match your machine — no need to pass flags every time you run.

DEFAULT_DATA_DIR = (
    r"C:\Users\moham\Desktop\ETA_FinalProject"
    r"\Data\archive\Data\StockHistory"
)
DEFAULT_HOST     = "localhost"
DEFAULT_PORT     = 5432
DEFAULT_DB       = "stocks_analytics"
DEFAULT_USER     = "stocks"
DEFAULT_PASSWORD = "stocks123"
DEFAULT_TABLE    = "stocks_raw"     # Power BI table; pipeline tables untouched
DEFAULT_CHUNK    = 50_000           # rows per COPY batch
DEFAULT_WORKERS  = 4                # parallel file loaders

# ── Table DDL ─────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id              BIGSERIAL        PRIMARY KEY,
    ticker          TEXT             NOT NULL,
    date            DATE             NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    dividends       DOUBLE PRECISION DEFAULT 0.0,
    stock_splits    DOUBLE PRECISION DEFAULT 0.0,
    stochk_14_3_3   DOUBLE PRECISION,
    stochd_14_3_3   DOUBLE PRECISION,
    source_file     TEXT,
    loaded_at       TIMESTAMPTZ      DEFAULT NOW()
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS {table}_ticker_idx   ON {table} (ticker);",
    "CREATE INDEX IF NOT EXISTS {table}_date_idx     ON {table} (date DESC);",
    "CREATE INDEX IF NOT EXISTS {table}_ticker_date  ON {table} (ticker, date);",
    "CREATE INDEX IF NOT EXISTS {table}_close_idx    ON {table} (close);",
]

# ── Column normalisation ──────────────────────────────────────────────────────

_COL_MAP = {
    "date":          "date",
    "timestamp":     "date",
    "open":          "open",
    "high":          "high",
    "low":           "low",
    "close":         "close",
    "adj_close":     "close",
    "volume":        "volume",
    "dividends":     "dividends",
    "dividend":      "dividends",
    "stock_splits":  "stock_splits",
    "stock splits":  "stock_splits",
    "stocksplits":   "stock_splits",
    "stochk_14_3_3": "stochk_14_3_3",
    "stoch%k":       "stochk_14_3_3",
    "stochd_14_3_3": "stochd_14_3_3",
    "stoch%d":       "stochd_14_3_3",
}

_REQUIRED  = {"date", "open", "high", "low", "close", "volume"}
_COPY_COLS = [
    "ticker", "date",
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits",
    "stochk_14_3_3", "stochd_14_3_3",
    "source_file",
]


def _normalise(df: pd.DataFrame, ticker: str, source_file: str):
    """Rename columns, add ticker/source, validate. Returns None to skip."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    rename = {}
    for raw, canonical in _COL_MAP.items():
        if raw in df.columns and canonical not in df.columns:
            rename[raw] = canonical
    if rename:
        df = df.rename(columns=rename)

    if not _REQUIRED.issubset(df.columns):
        missing = _REQUIRED - set(df.columns)
        log.warning("%-8s  missing %s — skipped", ticker, missing)
        return None

    df["ticker"]      = ticker
    df["source_file"] = source_file

    for col in ("dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3"):
        if col not in df.columns:
            df[col] = None

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "close"])

    if df.empty:
        return None

    for col in ("open", "high", "low", "close", "volume",
                "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[[c for c in _COPY_COLS if c in df.columns]]


# ── Fast COPY via psycopg2 ────────────────────────────────────────────────────

def _df_to_tsv_buffer(df: pd.DataFrame) -> io.StringIO:
    buf = io.StringIO()
    df.to_csv(
        buf,
        sep="\t",
        index=False,
        header=False,
        na_rep="\\N",
        date_format="%Y-%m-%d",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    buf.seek(0)
    return buf


def _copy_df(conn, df: pd.DataFrame, table: str) -> int:
    buf  = _df_to_tsv_buffer(df)
    cols = list(df.columns)
    with conn.cursor() as cur:
        cur.copy_from(buf, table, sep="\t", null="\\N", columns=cols)
    conn.commit()
    return len(df)


# ── Per-file worker ───────────────────────────────────────────────────────────

def _load_file(csv_path: Path, table: str, dsn: str, chunk_size: int):
    """Load one CSV file into Postgres. Returns (ticker, rows_loaded, error)."""
    ticker = csv_path.stem.upper()
    total  = 0
    conn   = None
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False

        for chunk in pd.read_csv(csv_path, low_memory=False, chunksize=chunk_size):
            df = _normalise(chunk, ticker, csv_path.name)
            if df is None or df.empty:
                continue
            total += _copy_df(conn, df, table)

        conn.close()
        return ticker, total, ""

    except Exception as exc:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return ticker, total, str(exc)


# ── Database helpers ──────────────────────────────────────────────────────────

def _build_dsn(args: argparse.Namespace) -> str:
    return (
        f"host={args.host} port={args.port} dbname={args.db} "
        f"user={args.user} password={args.password}"
    )


def _test_connection(dsn: str) -> None:
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        conn.close()
        log.info("PostgreSQL connection OK.")
    except Exception as exc:
        log.error("Cannot connect to PostgreSQL: %s", exc)
        log.error("")
        log.error("  Is the pipeline running?  docker compose up -d postgres")
        log.error("  Or start only Postgres:   docker compose up -d postgres")
        sys.exit(1)


def _setup_table(dsn: str, table: str, drop: bool) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        if drop:
            log.warning("--drop flag set: dropping table '%s' ...", table)
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        cur.execute(CREATE_TABLE_SQL.format(table=table))
    conn.close()
    log.info("Table '%s' is ready.", table)


def _create_indexes(dsn: str, table: str) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    log.info("Creating indexes on '%s' (may take a minute on large datasets) ...", table)
    with conn.cursor() as cur:
        for stmt in CREATE_INDEXES_SQL:
            cur.execute(stmt.format(table=table))
    conn.close()
    log.info("Indexes created.")


def _row_count(dsn: str, table: str) -> int:
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        n = cur.fetchone()[0]
    conn.close()
    return n


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk-load full Kaggle US Stocks dataset into PostgreSQL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--data-dir",  default=DEFAULT_DATA_DIR,
                   help="Path to folder with <TICKER>.csv files")
    p.add_argument("--host",      default=DEFAULT_HOST)
    p.add_argument("--port",      default=DEFAULT_PORT, type=int)
    p.add_argument("--db",        default=DEFAULT_DB)
    p.add_argument("--user",      default=DEFAULT_USER)
    p.add_argument("--password",  default=DEFAULT_PASSWORD)
    p.add_argument("--table",     default=DEFAULT_TABLE,
                   help="Target table — does NOT touch pipeline tables")
    p.add_argument("--chunk",     default=DEFAULT_CHUNK, type=int,
                   help="Rows per read/COPY batch")
    p.add_argument("--workers",   default=DEFAULT_WORKERS, type=int,
                   help="Parallel file workers")
    p.add_argument("--drop",      action="store_true",
                   help="DROP + recreate the table before loading")
    p.add_argument("--ticker",    default=None,
                   help="Load only this ticker (e.g. --ticker AAPL) for testing")
    return p.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    args  = _parse_args()
    dsn   = _build_dsn(args)
    table = args.table

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        log.error("Data directory not found: %s", data_dir)
        log.error("Check --data-dir or the DEFAULT_DATA_DIR at the top of this file.")
        sys.exit(1)

    csv_files = sorted(data_dir.glob("*.csv"))

    if args.ticker:
        csv_files = [f for f in csv_files if f.stem.upper() == args.ticker.upper()]
        if not csv_files:
            log.error("No file found for ticker '%s' in %s", args.ticker, data_dir)
            sys.exit(1)

    if not csv_files:
        log.error("No CSV files found in: %s", data_dir)
        sys.exit(1)

    log.info("=" * 62)
    log.info("  Bulk-load: Kaggle CSV  →  PostgreSQL")
    log.info("  Data dir  : %s", data_dir)
    log.info("  CSV files : %d", len(csv_files))
    log.info("  Target    : %s@%s:%s/%s  table=%s",
             args.user, args.host, args.port, args.db, table)
    log.info("  Workers   : %d  |  Chunk: %d rows", args.workers, args.chunk)
    log.info("  Drop first: %s", args.drop)
    log.info("=" * 62)

    _test_connection(dsn)
    _setup_table(dsn, table, drop=args.drop)

    wall_start    = time.time()
    total_rows    = 0
    total_skipped = 0
    errors: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_load_file, f, table, dsn, args.chunk): f
            for f in csv_files
        }
        with tqdm(total=len(csv_files), unit="file", desc="Loading") as pbar:
            for fut in as_completed(futures):
                ticker, rows, err = fut.result()
                if err:
                    errors.append((ticker, err))
                    log.warning("  %-8s  ERROR: %s", ticker, err)
                    total_skipped += 1
                elif rows == 0:
                    total_skipped += 1
                else:
                    total_rows += rows
                pbar.set_postfix(rows=f"{total_rows:,}", errors=len(errors))
                pbar.update(1)

    _create_indexes(dsn, table)

    elapsed  = time.time() - wall_start
    db_count = _row_count(dsn, table)

    log.info("=" * 62)
    log.info("  BULK LOAD COMPLETE")
    log.info("  Files processed : %d", len(csv_files))
    log.info("  Files skipped   : %d  (empty / bad columns / errors)", total_skipped)
    log.info("  Rows inserted   : %d", total_rows)
    log.info("  Rows in DB now  : %d", db_count)
    log.info("  Wall time       : %.1f s  (%.0f rows/s)",
             elapsed, total_rows / max(elapsed, 1))
    if errors:
        log.warning("  Files with errors (%d):", len(errors))
        for t, e in errors[:20]:
            log.warning("    %-8s  %s", t, e)
    log.info("=" * 62)
    log.info("")
    log.info("  Power BI connection")
    log.info("  ─────────────────────────────────────")
    log.info("  Server   : localhost:5432")
    log.info("  Database : %s", args.db)
    log.info("  Username : %s", args.user)
    log.info("  Password : %s", args.password)
    log.info("  Table    : %s   ← use this in Power BI", table)
    log.info("")
    log.info("  Quick checks in pgAdmin / psql:")
    log.info("    SELECT COUNT(*) FROM %s;", table)
    log.info("    SELECT * FROM %s WHERE ticker='AAPL' ORDER BY date LIMIT 10;", table)
    log.info("    SELECT DISTINCT ticker FROM %s ORDER BY 1;", table)
    log.info("=" * 62)


if __name__ == "__main__":
    main()
