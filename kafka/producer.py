"""
producer.py — US Stock Dataset Kafka Producer (Steady Real-Time Mode)
=====================================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

File discovery:
    Instead of querying the Kaggle API for a file list (which is paginated
    and unreliable for large datasets), the producer reads ticker symbols
    directly from a local CSV file (Stock_List.csv) and constructs the
    Kaggle file paths from those symbols.  This guarantees all 6 000+
    tickers are covered without any API pagination issues.

    Default lookup path : /app/Stock_List.csv  (inside Docker)
    Override with       : --stock-list <path>

    NOTE: Stock_List.csv is a broader ticker universe than what actually
    exists in the Kaggle dataset (footballjoe789/us-stock-dataset). Some
    symbols (e.g. warrant/unit variants like "AACBR", "AACIU") simply do
    not have a corresponding CSV file in the dataset and will 404. This
    is expected and handled as a permanent "not found" — see
    KaggleFileNotFound below — rather than a transient error worth
    retrying.

Architecture:
    Download Thread  →  queue(fname, df)  →  Main Thread (producer)

Usage:
    python producer.py [--rows-per-sec FLOAT] [--batch-size INT]
                       [--ticker SYMBOL] [--stock-list PATH]

Environment variables (or .env):
    KAGGLE_USERNAME   Kaggle account username
    KAGGLE_KEY        Kaggle API key
    KAFKA_BOOTSTRAP   Broker list, default localhost:9092
    KAFKA_TOPIC       Target topic, default us-stocks-raw
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PRODUCER] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KAGGLE_DATASET       = "footballjoe789/us-stock-dataset"
KAGGLE_API_BASE      = "https://www.kaggle.com/api/v1"
DEFAULT_TOPIC        = "us-stocks-raw"
DEFAULT_BROKER       = "localhost:9092"
DEFAULT_ROWS_PER_SEC = 1.0
DEFAULT_BATCH_SIZE   = 50
QUEUE_MAXSIZE        = 5

STOCK_PATH_PREFIX    = "Data/StockHistory"
REQUIRED_STOCK_COLS  = {"date", "open", "high", "low", "close", "volume"}

# Default location of the ticker list inside Docker (volume-mounted at /app)
DEFAULT_STOCK_LIST   = "/app/Stock_List.csv"

MIN_DELAY_BETWEEN_CALLS = 2.0
RATE_LIMIT_BASE_WAIT    = 15
RATE_LIMIT_MAX_WAIT     = 120

_SENTINEL = object()


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class KaggleFileNotFound(Exception):
    """Raised when the dataset genuinely has no file at this path (HTTP 404).

    This is a PERMANENT condition, not a transient one — retrying it
    10 times with backoff (as used to happen) only wastes minutes per
    missing ticker with zero chance of success. Callers should skip the
    file immediately instead of retrying.
    """
    pass


# ---------------------------------------------------------------------------
# Runtime statistics (thread-safe)
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self, total_files: int) -> None:
        self.total_files   = total_files
        self.done_files    = 0
        self.skipped_files = 0
        self.not_found_files = 0
        self.total_events  = 0
        self.start_time    = time.time()
        self._lock         = threading.Lock()

    def add_events(self, n: int) -> None:
        with self._lock:
            self.total_events += n

    def file_done(self) -> None:
        with self._lock:
            self.done_files += 1

    def file_skipped(self) -> None:
        with self._lock:
            self.skipped_files += 1

    def file_not_found(self) -> None:
        with self._lock:
            self.not_found_files += 1
            self.skipped_files += 1

    def elapsed_str(self) -> str:
        elapsed = int(time.time() - self.start_time)
        h = elapsed // 3600
        m = (elapsed % 3600) // 60
        s = elapsed % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def ev_per_s(self) -> float:
        elapsed = max(time.time() - self.start_time, 1e-9)
        return self.total_events / elapsed


# ---------------------------------------------------------------------------
# Kaggle authentication
# ---------------------------------------------------------------------------

def _kaggle_auth() -> tuple[str, str]:
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")

    if not username or not key:
        import json as _json
        cfg_path = os.path.expanduser("~/.kaggle/kaggle.json")
        if os.path.exists(cfg_path):
            with open(cfg_path) as fh:
                creds    = _json.load(fh)
                username = creds.get("username")
                key      = creds.get("key")

    if not username or not key:
        raise EnvironmentError(
            "Kaggle credentials not found. "
            "Set KAGGLE_USERNAME / KAGGLE_KEY env vars, "
            "or place kaggle.json in ~/.kaggle/."
        )
    return username, key


def _kaggle_env() -> dict:
    username, key = _kaggle_auth()
    return {**os.environ, "KAGGLE_USERNAME": username, "KAGGLE_KEY": key}


# ---------------------------------------------------------------------------
# Build file list from Stock_List.csv  (primary — always complete)
# ---------------------------------------------------------------------------

def build_file_list_from_csv(stock_list_path: str) -> list[str]:
    """Read ticker symbols from Stock_List.csv and return Kaggle file paths.

    The CSV has a single column called 'Symbol' (header on row 1).
    Each ticker maps to:  data/stockhistory/<TICKER>.csv
    """
    log.info("Reading ticker list from '%s' …", stock_list_path)
    df = pd.read_csv(stock_list_path, dtype=str)

    # Accept 'Symbol', 'symbol', 'Ticker', 'ticker', or bare first column
    col = None
    for candidate in ("Symbol", "symbol", "Ticker", "ticker"):
        if candidate in df.columns:
            col = candidate
            break
    if col is None:
        col = df.columns[0]

    tickers = sorted(df[col].dropna().str.strip().str.upper().unique())
    paths   = [f"{STOCK_PATH_PREFIX}/{t}.csv" for t in tickers]
    log.info("Built file list: %d tickers from Stock_List.csv", len(paths))
    return paths


# ---------------------------------------------------------------------------
# Kaggle: download one CSV file  (with retries + rate-limit backoff)
# ---------------------------------------------------------------------------

def _normalise_df(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "ticker" not in df.columns and "symbol" not in df.columns:
        ticker = os.path.splitext(os.path.basename(filename))[0].upper()
        df.insert(0, "ticker", ticker)
    elif "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    return df


def _is_stock_file(df: pd.DataFrame) -> bool:
    return REQUIRED_STOCK_COLS.issubset(set(df.columns))


def _is_rate_limit_stderr(stderr: str) -> bool:
    if not stderr:
        return False
    lowered = stderr.lower()
    return "429" in lowered or "too many requests" in lowered


def _is_not_found_stderr(stderr: str) -> bool:
    if not stderr:
        return False
    lowered = stderr.lower()
    return "404" in lowered or "not found" in lowered


def download_single_csv(
    filename: str,
    dataset: str = KAGGLE_DATASET,
    max_retries: int = 10,
    min_delay_between_calls: float = MIN_DELAY_BETWEEN_CALLS,
) -> pd.DataFrame:
    for attempt in range(1, max_retries + 1):
        # --- CLI path ---
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = subprocess.run(
                    [
                        "kaggle", "datasets", "download",
                        dataset, "--file", filename,
                        "--path", tmpdir, "--unzip", "--quiet",
                    ],
                    capture_output=True, text=True,
                    env=_kaggle_env(), timeout=300,
                )
                if result.returncode == 0:
                    for root, _dirs, files in os.walk(tmpdir):
                        for fname in files:
                            if fname.lower().endswith(".csv"):
                                df = pd.read_csv(
                                    os.path.join(root, fname), low_memory=False
                                )
                                time.sleep(min_delay_between_calls)
                                return _normalise_df(df, filename)

                # Permanent: file genuinely doesn't exist in the dataset —
                # don't waste retries on it, fail fast.
                if _is_not_found_stderr(result.stderr):
                    raise KaggleFileNotFound(
                        f"{filename} not present in dataset '{dataset}' (404)."
                    )

                if _is_rate_limit_stderr(result.stderr):
                    wait = min(RATE_LIMIT_MAX_WAIT, RATE_LIMIT_BASE_WAIT * attempt)
                    log.warning(
                        "[DL] Rate-limited (CLI) for %s attempt %d/%d — waiting %ds …",
                        filename, attempt, max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
        except KaggleFileNotFound:
            raise
        except Exception as cli_err:
            log.warning("[DL] CLI attempt %d/%d failed for %s: %s",
                        attempt, max_retries, filename, cli_err)

        # --- REST fallback ---
        try:
            username, key = _kaggle_auth()
            url  = f"{KAGGLE_API_BASE}/datasets/download/{dataset}/{filename}"
            resp = requests.get(url, auth=(username, key), stream=True, timeout=300)

            # Permanent: 404 means the file isn't in the dataset at all.
            # Retrying this 10x with backoff (the old behaviour) wastes
            # minutes per missing ticker for zero benefit — fail fast instead.
            if resp.status_code == 404:
                raise KaggleFileNotFound(
                    f"{filename} not present in dataset '{dataset}' (404)."
                )

            if resp.status_code == 429:
                wait = min(RATE_LIMIT_MAX_WAIT, RATE_LIMIT_BASE_WAIT * attempt)
                log.warning(
                    "[DL] Rate-limited (REST) for %s attempt %d/%d — waiting %ds …",
                    filename, attempt, max_retries, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            buf = io.BytesIO(resp.content)
            if zipfile.is_zipfile(buf):
                buf.seek(0)
                with zipfile.ZipFile(buf) as zf:
                    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if csv_names:
                        with zf.open(csv_names[0]) as fh:
                            df = pd.read_csv(fh, low_memory=False)
                            time.sleep(min_delay_between_calls)
                            return _normalise_df(df, filename)
            else:
                buf.seek(0)
                df = pd.read_csv(buf, low_memory=False)
                time.sleep(min_delay_between_calls)
                return _normalise_df(df, filename)
        except KaggleFileNotFound:
            raise
        except Exception as rest_err:
            log.warning("[DL] REST attempt %d/%d failed for %s: %s",
                        attempt, max_retries, filename, rest_err)

        wait = min(30, 5 * attempt)
        log.warning("[DL] Retrying %s in %d s …", filename, wait)
        time.sleep(wait)

    raise RuntimeError(f"All {max_retries} download attempts failed for {filename}.")


# ---------------------------------------------------------------------------
# Background download thread
# ---------------------------------------------------------------------------

def _download_worker(
    csv_files: list[str],
    ticker_filter: Optional[str],
    file_queue: queue.Queue,
    stats: _Stats,
) -> None:
    for fname in csv_files:
        ticker_hint = os.path.splitext(os.path.basename(fname))[0].upper()

        if ticker_filter and ticker_hint != ticker_filter.upper():
            stats.file_skipped()
            continue

        log.info("[DL] Downloading %s …", fname)
        try:
            df = download_single_csv(fname)

            if df.empty:
                log.warning("[DL] %s is empty — skipping.", fname)
                stats.file_skipped()
                continue

            if not _is_stock_file(df):
                log.warning("[DL] %s missing required columns — skipping.", fname)
                stats.file_skipped()
                continue

            date_cols = [c for c in df.columns if c in ("date", "timestamp", "time")]
            if date_cols:
                df = df.sort_values(date_cols[0], ascending=True)

            log.info("[DL] Ready  file=%-30s  rows=%d", os.path.basename(fname), len(df))
            file_queue.put((fname, df))

        except KaggleFileNotFound:
            # Expected/permanent: this ticker just isn't in the dataset.
            # Log at INFO (not ERROR) since it's not a failure of the pipeline,
            # and move on immediately — no retries wasted.
            log.info("[DL] %s not in dataset — skipping (no retries).", fname)
            stats.file_not_found()
            continue

        except Exception as exc:
            log.error("[DL] Permanently skipping %s after all retries: %s", fname, exc)
            stats.file_skipped()

    file_queue.put(_SENTINEL)
    log.info(
        "[DL] All files queued. Download thread done. "
        "(%d not found in dataset, %d skipped total)",
        stats.not_found_files, stats.skipped_files,
    )


# ---------------------------------------------------------------------------
# Event construction
# ---------------------------------------------------------------------------

def row_to_event(row: pd.Series, source_file: str, event_id: int) -> dict:
    def _safe(key: str) -> Optional[float]:
        val = row.get(key)
        if val is None:
            return None
        try:
            f = float(val)
            return None if pd.isna(f) else f
        except (TypeError, ValueError):
            return None

    raw_date = row.get("date")
    date_str: Optional[str] = None
    if raw_date is not None and str(raw_date) not in ("", "nan", "NaT"):
        date_str = str(raw_date)[:10]

    return {
        "event_id":       event_id,
        "ticker":         str(row.get("ticker", "UNKNOWN")).upper(),
        "date":           date_str,
        "open":           _safe("open"),
        "high":           _safe("high"),
        "low":            _safe("low"),
        "close":          _safe("close"),
        "volume":         _safe("volume"),
        "dividends":      _safe("dividends"),
        "stock_splits":   _safe("stock_splits"),
        "stochk_14_3_3":  _safe("stochk_14_3_3"),
        "stochd_14_3_3":  _safe("stochd_14_3_3"),
        "source_file":    source_file,
        "produced_at":    datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Kafka producer factory
# ---------------------------------------------------------------------------

def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers":                     bootstrap_servers,
        "acks":                                  "all",
        "retries":                               10,
        "retry.backoff.ms":                      1000,
        "linger.ms":                             20,
        "batch.size":                            65536,
        "compression.type":                      "lz4",
        "enable.idempotence":                    True,
        "max.in.flight.requests.per.connection": 5,
    })


def delivery_callback(err, msg) -> None:
    if err:
        log.error("Delivery FAILED | topic=%s | %s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s | partition=%d | offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def _produce_batch(producer: Producer, topic: str, batch: list[dict]) -> None:
    for event in batch:
        producer.produce(
            topic=topic,
            key=event["ticker"].encode(),
            value=json.dumps(event, default=str).encode(),
            callback=delivery_callback,
        )
    producer.poll(0)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="US Stocks Kafka Producer — steady controlled pace"
    )
    parser.add_argument(
        "--rows-per-sec", type=float, default=DEFAULT_ROWS_PER_SEC,
        help="Rows to produce per second (default: 1.0)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help="Rows per Kafka micro-batch (default: 50)",
    )
    parser.add_argument(
        "--ticker", type=str, default=None,
        help="Stream only this ticker symbol",
    )
    parser.add_argument(
        "--topic", type=str, default=None,
        help="Override the target Kafka topic",
    )
    parser.add_argument(
        "--stock-list", type=str, default=DEFAULT_STOCK_LIST,
        help=f"Path to Stock_List.csv (default: {DEFAULT_STOCK_LIST})",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()
    args = _parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    rows_per_sec  = max(args.rows_per_sec, 0.01)
    sleep_per_row = 1.0 / rows_per_sec

    log.info("=" * 62)
    log.info("  US Stocks Kafka Producer  [STEADY REAL-TIME MODE]")
    log.info("  Broker       : %s", bootstrap)
    log.info("  Topic        : %s", topic)
    log.info("  Rows/sec     : %.2f  (%.3f s between rows)", rows_per_sec, sleep_per_row)
    log.info("  Batch size   : %d rows", args.batch_size)
    log.info("  Stock list   : %s", args.stock_list)
    log.info("=" * 62)

    # ── Build complete file list from Stock_List.csv ─────────────────────────
    if not os.path.exists(args.stock_list):
        log.error(
            "Stock list not found at '%s'. "
            "Mount Stock_List.csv into the container or use --stock-list <path>.",
            args.stock_list,
        )
        sys.exit(1)

    csv_files = build_file_list_from_csv(args.stock_list)
    log.info("Stock files to stream: %d", len(csv_files))

    stats      = _Stats(total_files=len(csv_files))
    file_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

    dl_thread = threading.Thread(
        target=_download_worker,
        args=(csv_files, args.ticker, file_queue, stats),
        daemon=True,
        name="kaggle-downloader",
    )
    dl_thread.start()

    producer     = build_producer(bootstrap)
    total_events = 0
    event_id     = 0
    start_wall   = time.time()
    next_row_time = time.monotonic()

    try:
        while True:
            item = file_queue.get()
            if item is _SENTINEL:
                log.info("=" * 62)
                log.info("All files downloaded and produced — flushing …")
                log.info("=" * 62)
                break

            fname, df   = item
            basename    = os.path.basename(fname)
            file_rows   = len(df)
            file_events = 0
            file_start  = time.time()

            log.info(
                "START  file=%-14s  rows=%d  files=%d/%d done  total=%d",
                basename, file_rows,
                stats.done_files, len(csv_files),
                total_events,
            )

            batch: list[dict] = []

            for _, row in df.iterrows():
                now = time.monotonic()
                if next_row_time > now:
                    time.sleep(next_row_time - now)
                next_row_time = time.monotonic() + sleep_per_row

                batch.append(row_to_event(row, fname, event_id))
                event_id += 1

                if len(batch) >= args.batch_size:
                    _produce_batch(producer, topic, batch)
                    total_events  += len(batch)
                    file_events   += len(batch)
                    stats.add_events(len(batch))

                    log.info(
                        "  %-14s  sent=%5d/%5d (%5.1f%%)  total=%d  "
                        "elapsed=%s  rate=%.2f rows/s",
                        basename,
                        file_events, file_rows,
                        file_events / max(file_rows, 1) * 100,
                        total_events,
                        stats.elapsed_str(),
                        rows_per_sec,
                    )
                    batch = []

            if batch:
                _produce_batch(producer, topic, batch)
                total_events += len(batch)
                file_events  += len(batch)
                stats.add_events(len(batch))

            stats.file_done()
            file_elapsed = time.time() - file_start
            log.info(
                "DONE   file=%-14s  events=%d  time=%.1fs  "
                "files=%d/%d  grand_total=%d",
                basename, file_events, file_elapsed,
                stats.done_files, len(csv_files),
                total_events,
            )

    except KeyboardInterrupt:
        log.info("Interrupted by user — flushing …")
    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        producer.flush(timeout=60)
        elapsed = time.time() - start_wall
        log.info("=" * 62)
        log.info("  PRODUCER FINISHED")
        log.info("  Total events   : %d", total_events)
        log.info("  Files done     : %d / %d", stats.done_files, len(csv_files))
        log.info("  Files skipped  : %d  (of which not-in-dataset: %d)",
                  stats.skipped_files, stats.not_found_files)
        log.info("  Wall time      : %.1f s  (%s)", elapsed, stats.elapsed_str())
        log.info("  Topic          : %s", topic)
        log.info("=" * 62)


if __name__ == "__main__":
    main()