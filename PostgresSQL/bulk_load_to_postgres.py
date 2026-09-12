"""
bulk_load_to_postgres.py — Robust bulk loader for Kaggle stock CSVs → PostgreSQL
=================================================================================
Fixes:
  1. Handles timezone-aware dates (e.g. "1980-12-12 00:00:00-05:00") by
     stripping tz before calling .dt.date — fixes AttributeError on .dt.date.
  2. Uses execute_values (multi-row INSERT) instead of COPY FROM — handles
     special characters, NaN, and extra columns without crashing.
  3. ON CONFLICT (ticker, date) DO NOTHING — safe to re-run anytime.
  4. Row-by-row fallback if a batch INSERT fails.
  5. Accepts CSVs with any extra columns (RSI, MACD, BB, etc.) — only
     the canonical columns are loaded, extras are silently ignored.
"""

import logging
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import traceback as tb

import pandas as pd
import psycopg2
import psycopg2.extras
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BULK] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Table DDL ──────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id            BIGSERIAL PRIMARY KEY,
    ticker        TEXT             NOT NULL,
    date          DATE             NOT NULL,
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        DOUBLE PRECISION,
    dividends     DOUBLE PRECISION DEFAULT 0.0,
    stock_splits  DOUBLE PRECISION DEFAULT 0.0,
    stochk_14_3_3 DOUBLE PRECISION,
    stochd_14_3_3 DOUBLE PRECISION,
    source_file   TEXT,
    loaded_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, date)
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS {t}_ticker ON {table} (ticker);",
    "CREATE INDEX IF NOT EXISTS {t}_date   ON {table} (date DESC);",
    "CREATE INDEX IF NOT EXISTS {t}_td     ON {table} (ticker, date);",
    "CREATE INDEX IF NOT EXISTS {t}_close  ON {table} (close);",
]

# ── Column mapping (CSV name → canonical DB name) ──────────────────────────────

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
    "stock_splits":  "stock_splits",
    "stochk_14_3_3": "stochk_14_3_3",
    "stochd_14_3_3": "stochd_14_3_3",
}

_REQUIRED  = {"date", "open", "high", "low", "close", "volume"}

# Only these columns are written to the DB — all others (RSI, MACD, etc.) ignored
_COPY_COLS = [
    "ticker", "date", "open", "high", "low", "close", "volume",
    "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3", "source_file",
]

_NUMERIC = {
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3",
}


# ── Normalise one DataFrame chunk ──────────────────────────────────────────────

def normalise(df: pd.DataFrame, ticker: str, src: str):
    df = df.copy()

    # Lowercase + underscore column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Map known CSV column names to canonical names
    for raw, can in _COL_MAP.items():
        if raw in df.columns and can not in df.columns:
            df = df.rename(columns={raw: can})

    # Must have all required columns
    if not _REQUIRED.issubset(df.columns):
        return None

    df["ticker"]      = ticker
    df["source_file"] = src

    # Fill optional columns with None if missing
    for c in ("dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3"):
        if c not in df.columns:
            df[c] = None

    # ── FIX: handle timezone-aware dates ──────────────────────────────────────
    # Dates like "1980-12-12 00:00:00-05:00" parse as tz-aware Timestamps.
    # .dt.date fails on tz-aware series in some pandas versions.
    # Solution: parse, strip tz, then extract date.
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.tz_localize(None).dt.date   # strip tz → plain date

    df = df.dropna(subset=["close"])
    if df.empty:
        return None

    # Convert numeric columns — non-parseable → NaN → will become NULL
    for c in _NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep only the canonical columns we write to the DB
    cols = [c for c in _COPY_COLS if c in df.columns]
    return df[cols]


# ── Insert a DataFrame using execute_values ────────────────────────────────────

def insert_df(conn, df: pd.DataFrame, table: str) -> int:
    cols = list(df.columns)
    sql  = """
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT (ticker, date) DO NOTHING
    """.format(table=table, cols=", ".join(cols))

    # Replace float NaN with None so psycopg2 sends SQL NULL
    records = [
        tuple(None if (isinstance(v, float) and v != v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=5000)
    conn.commit()
    return len(records)


# ── Load one CSV file ──────────────────────────────────────────────────────────

def load_file(csv_path: Path, table: str, dsn: str, chunk_size: int, verbose: bool = False):
    ticker = csv_path.stem.upper()
    total  = 0
    conn   = None
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False

        for chunk_df in pd.read_csv(csv_path, low_memory=False, chunksize=chunk_size):
            df = normalise(chunk_df, ticker, csv_path.name)
            if df is None or df.empty:
                continue

            try:
                total += insert_df(conn, df, table)
            except Exception as chunk_err:
                conn.rollback()
                # Row-by-row fallback — one bad row won't lose the whole chunk
                bad = 0
                for i in range(len(df)):
                    single = df.iloc[i:i+1]
                    try:
                        total += insert_df(conn, single, table)
                    except Exception:
                        bad += 1
                        conn.rollback()
                if bad and verbose:
                    log.warning("  %s: %d rows skipped in chunk (%s)", ticker, bad, chunk_err)

        conn.close()
        return ticker, total, ""

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        err_msg = tb.format_exc() if verbose else str(exc)
        return ticker, total, err_msg


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Bulk-load Kaggle stock CSVs into PostgreSQL"
    )
    p.add_argument("--data-dir",  required=True)
    p.add_argument("--host",      default="postgres")
    p.add_argument("--port",      type=int, default=5432)
    p.add_argument("--db",        default="stocks_analytics")
    p.add_argument("--user",      default="stocks")
    p.add_argument("--password",  default="stocks123")
    p.add_argument("--table",     default="stocks_raw")
    p.add_argument("--chunk",     type=int, default=50000)
    p.add_argument("--workers",   type=int, default=4)
    p.add_argument("--drop",      action="store_true",
                   help="DROP and recreate table before loading")
    p.add_argument("--ticker",    default=None,
                   help="Load only this one ticker (for testing)")
    p.add_argument("--verbose",   action="store_true",
                   help="Print full traceback for each file error")
    args = p.parse_args()

    dsn = (
        f"host={args.host} port={args.port} "
        f"dbname={args.db} user={args.user} password={args.password}"
    )

    # Verify connection
    try:
        psycopg2.connect(dsn).close()
        log.info("PostgreSQL connection OK.")
    except Exception as e:
        log.error("Cannot connect: %s", e)
        raise SystemExit(1)

    # Discover files
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        log.error("Data directory not found: %s", data_dir)
        raise SystemExit(1)

    files = sorted(data_dir.glob("*.csv"))
    if args.ticker:
        files = [f for f in files if f.stem.upper() == args.ticker.upper()]
    if not files:
        log.error("No CSV files found in %s", data_dir)
        raise SystemExit(1)

    log.info("Files to load : %d", len(files))
    log.info("Target        : %s@%s:%s/%s  table=%s",
             args.user, args.host, args.port, args.db, args.table)

    # Create / reset table
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    if args.drop:
        log.info("Dropping table %s ...", args.table)
        cur.execute(f"DROP TABLE IF EXISTS {args.table} CASCADE;")
    cur.execute(CREATE_TABLE_SQL.format(table=args.table))
    conn.close()
    log.info("Table '%s' is ready.", args.table)

    # Parallel load
    wall_start = time.time()
    total_rows = 0
    errors     = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {
            pool.submit(load_file, f, args.table, dsn, args.chunk, args.verbose): f
            for f in files
        }
        with tqdm(total=len(files), unit="file", desc="Loading") as bar:
            for fut in as_completed(futs):
                ticker, n, err = fut.result()
                if err:
                    errors.append((ticker, err))
                    if args.verbose:
                        log.warning("ERROR %s:\n%s", ticker, err)
                else:
                    total_rows += n
                bar.set_postfix(rows=f"{total_rows:,}", errs=str(len(errors)))
                bar.update(1)

    # Create indexes
    log.info("Creating indexes on '%s' ...", args.table)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur  = conn.cursor()
    t    = args.table.replace("-", "_")
    for idx_sql in CREATE_INDEXES:
        cur.execute(idx_sql.format(table=args.table, t=t))
    conn.close()
    log.info("Indexes created.")

    # Final count
    conn = psycopg2.connect(dsn)
    cur  = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {args.table};")
    db_count = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - wall_start
    log.info("=" * 62)
    log.info("  BULK LOAD COMPLETE")
    log.info("  Files processed  : %d", len(files))
    log.info("  Files with errors: %d", len(errors))
    log.info("  Rows inserted    : %d", total_rows)
    log.info("  Rows in DB now   : %d", db_count)
    log.info("  Wall time        : %.1f s  (%.0f rows/s)",
             elapsed, total_rows / max(elapsed, 1))
    log.info("=" * 62)

    if errors:
        log.warning("Files with errors — first 20:")
        for ticker, err in errors[:20]:
            log.warning("  %-10s  %s", ticker, err.split("\n")[0])
        if not args.verbose:
            log.info("Re-run with --verbose to see full tracebacks.")

    log.info("Check: SELECT COUNT(*) FROM %s;", args.table)


main()
