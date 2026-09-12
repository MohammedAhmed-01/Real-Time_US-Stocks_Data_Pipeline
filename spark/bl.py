import io
import logging
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import psycopg2
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BULK] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CREATE_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS {table} ("
    "id BIGSERIAL PRIMARY KEY, "
    "ticker TEXT NOT NULL, "
    "date DATE NOT NULL, "
    "open DOUBLE PRECISION, "
    "high DOUBLE PRECISION, "
    "low DOUBLE PRECISION, "
    "close DOUBLE PRECISION, "
    "volume DOUBLE PRECISION, "
    "dividends DOUBLE PRECISION DEFAULT 0.0, "
    "stock_splits DOUBLE PRECISION DEFAULT 0.0, "
    "stochk_14_3_3 DOUBLE PRECISION, "
    "stochd_14_3_3 DOUBLE PRECISION, "
    "source_file TEXT, "
    "loaded_at TIMESTAMPTZ DEFAULT NOW()"
    ");"
)

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS {t}_ticker ON {table} (ticker);",
    "CREATE INDEX IF NOT EXISTS {t}_date   ON {table} (date DESC);",
    "CREATE INDEX IF NOT EXISTS {t}_td     ON {table} (ticker, date);",
    "CREATE INDEX IF NOT EXISTS {t}_close  ON {table} (close);",
]

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
_COPY_COLS = [
    "ticker", "date", "open", "high", "low", "close", "volume",
    "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3", "source_file",
]
_NUMERIC = {
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3",
}


def normalise(df, ticker, src):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for raw, can in _COL_MAP.items():
        if raw in df.columns and can not in df.columns:
            df = df.rename(columns={raw: can})
    if not _REQUIRED.issubset(df.columns):
        return None
    df["ticker"] = ticker
    df["source_file"] = src
    for c in ("dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3"):
        if c not in df.columns:
            df[c] = None
    # FIX: assign result back before calling .dt.date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.date
    df = df.dropna(subset=["close"])
    if df.empty:
        return None
    for c in _NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[[c for c in _COPY_COLS if c in df.columns]]


def copy_df(conn, df, table):
    # FIX: fillna("") so NaN becomes empty field -> PostgreSQL NULL for all types
    buf = io.StringIO()
    df.fillna("").to_csv(buf, sep="\t", index=False, header=False, date_format="%Y-%m-%d")
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(buf, table, sep="\t", null="", columns=list(df.columns))
    conn.commit()
    return len(df)


def load_file(csv_path, table, dsn, chunk_size):
    ticker = csv_path.stem.upper()
    total = 0
    conn = None
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        for chunk_df in pd.read_csv(csv_path, low_memory=False, chunksize=chunk_size):
            df = normalise(chunk_df, ticker, csv_path.name)
            if df is None or df.empty:
                continue
            total += copy_df(conn, df, table)
        conn.close()
        return ticker, total, ""
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        return ticker, total, str(exc)


def main():
    p = argparse.ArgumentParser(description="Bulk-load Kaggle stock CSVs into PostgreSQL")
    p.add_argument("--data-dir",  required=True, help="Folder containing <TICKER>.csv files")
    p.add_argument("--host",      default="postgres")
    p.add_argument("--port",      type=int, default=5432)
    p.add_argument("--db",        default="stocks_analytics")
    p.add_argument("--user",      default="stocks")
    p.add_argument("--password",  default="stocks123")
    p.add_argument("--table",     default="stocks_raw")
    p.add_argument("--chunk",     type=int, default=50000)
    p.add_argument("--workers",   type=int, default=4)
    p.add_argument("--drop",      action="store_true", help="DROP table before loading")
    p.add_argument("--ticker",    default=None, help="Load only this ticker (for testing)")
    args = p.parse_args()

    dsn = (
        "host=" + args.host +
        " port=" + str(args.port) +
        " dbname=" + args.db +
        " user=" + args.user +
        " password=" + args.password
    )

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
    log.info("Target        : %s@%s:%s/%s table=%s", args.user, args.host, args.port, args.db, args.table)

    # Setup table
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    if args.drop:
        log.info("Dropping table %s ...", args.table)
        cur.execute("DROP TABLE IF EXISTS " + args.table + " CASCADE;")
    cur.execute(CREATE_TABLE_SQL.format(table=args.table))
    conn.close()
    log.info("Table ready.")

    # Load files in parallel
    wall_start = time.time()
    total_rows = 0
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(load_file, f, args.table, dsn, args.chunk): f for f in files}
        with tqdm(total=len(files), unit="file", desc="Loading") as bar:
            for fut in as_completed(futs):
                ticker, n, err = fut.result()
                if err:
                    errors.append((ticker, err))
                else:
                    total_rows += n
                bar.set_postfix(rows=str(total_rows), errs=str(len(errors)))
                bar.update(1)

    # Create indexes
    log.info("Creating indexes ...")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    tbl = args.table
    t = tbl.replace("-", "_")
    for idx in CREATE_INDEXES:
        cur.execute(idx.format(table=tbl, t=t))
    conn.close()
    log.info("Indexes created.")

    # Final count
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM " + args.table + ";")
    db_count = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - wall_start
    log.info("=" * 60)
    log.info("BULK LOAD COMPLETE")
    log.info("Files processed : %d", len(files))
    log.info("Rows inserted   : %d", total_rows)
    log.info("Rows in DB now  : %d", db_count)
    log.info("Errors          : %d", len(errors))
    log.info("Wall time       : %.1f s  (%.0f rows/s)", elapsed, total_rows / max(elapsed, 1))
    if errors:
        log.warning("First 10 errors:")
        for ticker, err in errors[:10]:
            log.warning("  %-8s  %s", ticker, err)
    log.info("=" * 60)
    log.info("Power BI: localhost:5432 / db=stocks_analytics / user=stocks / pass=stocks123 / table=%s", args.table)


main()