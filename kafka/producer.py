"""
Kafka Producer — US Stock Dataset (Kaggle)  [PARALLEL REAL-TIME MODE]
======================================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Architecture:
  ┌──────────────────────┐     queue      ┌──────────────────────────┐
  │  DOWNLOAD THREAD     │ ─── (fname,df) ─▶  MAIN THREAD (producer) │
  │  downloads CSV files │               │  converts rows → events  │
  │  flushes to Kafka    │               └──────────────────────────┘
  └──────────────────────┘

Usage:
    python producer.py [--speed FLOAT] [--batch-size INT] [--ticker SYMBOL]

Environment variables (or .env file):
    KAGGLE_USERNAME   – Kaggle account username
    KAGGLE_KEY        – Kaggle API key
    KAFKA_BOOTSTRAP   – Kafka broker(s), default localhost:9092
    KAFKA_TOPIC       – Topic name,       default us-stocks-raw
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
from typing import Generator, Optional

import pandas as pd
import requests
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PRODUCER] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
KAGGLE_DATASET   = "footballjoe789/us-stock-dataset"
KAGGLE_API_BASE  = "https://www.kaggle.com/api/v1"
DEFAULT_TOPIC    = "us-stocks-raw"
DEFAULT_BROKER   = "localhost:9092"
MICRO_BATCH_SIZE = 50
SPEED_FACTOR     = 1.0
QUEUE_MAXSIZE    = 3
_SENTINEL        = object()

# Only stream files whose path (lower-cased) starts with this prefix.
# The dataset also contains Data/Congress/ — those are skipped.
STOCK_PATH_PREFIX = "data/stockhistory"

# Real column names in every StockHistory CSV (after lower-casing):
#   date, open, high, low, close, volume, dividends, stock_splits,
#   stochk_14_3_3, stochd_14_3_3
# Ticker is NOT a column — it comes from the filename (e.g. AAPL.csv → AAPL).
#
# A file is accepted only if it has ALL of these core columns.
REQUIRED_STOCK_COLS = {"date", "open", "high", "low", "close", "volume"}


# ── Dashboard helpers ─────────────────────────────────────────────────────────

class Dashboard:
    """
    Thread-safe live progress dashboard printed to the terminal.
    Rewrites a fixed block of lines so the terminal doesn't scroll.
    """

    BAR_WIDTH = 30

    def __init__(self, total_files: int) -> None:
        self.total_files   = total_files
        self.done_files    = 0
        self.current_file  = "—"
        self.total_rows    = 0        # rows in current file
        self.produced_rows = 0        # rows sent so far (across all files)
        self.total_events  = 0        # running total events produced
        self.skipped_files = 0
        self.start_time    = time.time()
        self._lock         = threading.Lock()
        self._lines        = 0        # how many lines the last render used

    # ── state updates (called from main thread) ────────────────────────────

    def set_file(self, fname: str, nrows: int) -> None:
        with self._lock:
            self.current_file  = os.path.basename(fname)
            self.total_rows    = nrows
            self.produced_rows = 0

    def add_events(self, n: int) -> None:
        with self._lock:
            self.produced_rows += n
            self.total_events  += n

    def file_done(self) -> None:
        with self._lock:
            self.done_files += 1

    def file_skipped(self) -> None:
        with self._lock:
            self.skipped_files += 1

    # ── rendering ─────────────────────────────────────────────────────────

    def _bar(self, frac: float) -> str:
        filled = int(self.BAR_WIDTH * frac)
        return "█" * filled + "░" * (self.BAR_WIDTH - filled)

    def render(self) -> None:
        with self._lock:
            elapsed   = max(time.time() - self.start_time, 1e-9)
            ev_s      = self.total_events / elapsed
            file_pct  = self.done_files / max(self.total_files, 1)
            row_pct   = self.produced_rows / max(self.total_rows, 1)
            remaining = self.total_files - self.done_files

            lines = [
                "",
                "  ╔══════════════════════════════════════════════════════════╗",
                "  ║          📈  US STOCKS KAFKA PRODUCER  📈               ║",
                "  ╠══════════════════════════════════════════════════════════╣",
                f"  ║  Files   : {self.done_files:>4} / {self.total_files:<4} done  "
                f"│  Skipped : {self.skipped_files:<4}  │  Remaining: {remaining:<4}  ║",
                f"  ║  Current : {self.current_file[:42]:<42}         ║",
                f"  ║  File    : [{self._bar(row_pct)}] {row_pct*100:5.1f}%  ║",
                f"  ║  Overall : [{self._bar(file_pct)}] {file_pct*100:5.1f}%  ║",
                f"  ║  Events  : {self.total_events:>12,}  │  Speed: {ev_s:>8,.0f} ev/s  │  "
                f"Elapsed: {int(elapsed//60):02d}:{int(elapsed%60):02d}  ║",
                "  ╚══════════════════════════════════════════════════════════╝",
                "",
            ]

        # Move cursor up to overwrite previous render
        if self._lines:
            sys.stdout.write(f"\033[{self._lines}A")

        output = "\n".join(lines)
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
        self._lines = len(lines)


# ── Kaggle auth ───────────────────────────────────────────────────────────────

def _kaggle_auth() -> tuple[str, str]:
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")

    if not username or not key:
        import json as _json
        cfg = os.path.expanduser("~/.kaggle/kaggle.json")
        if os.path.exists(cfg):
            with open(cfg) as f:
                creds = _json.load(f)
                username = creds.get("username")
                key      = creds.get("key")

    if not username or not key:
        raise EnvironmentError(
            "Kaggle credentials not found. "
            "Set KAGGLE_USERNAME / KAGGLE_KEY env vars, "
            "or place kaggle.json in ~/.kaggle/"
        )
    return username, key


def _kaggle_env() -> dict:
    username, key = _kaggle_auth()
    return {**os.environ, "KAGGLE_USERNAME": username, "KAGGLE_KEY": key}


# ── Kaggle: list files ────────────────────────────────────────────────────────

def list_kaggle_csv_files(dataset: str = KAGGLE_DATASET) -> list[str]:
    log.info("Fetching file list for dataset '%s' …", dataset)

    try:
        result = subprocess.run(
            ["kaggle", "datasets", "files", dataset, "--csv"],
            capture_output=True, text=True,
            env=_kaggle_env(), timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())

        lines = result.stdout.strip().splitlines()
        csv_files = []
        for line in lines[1:]:
            name = line.split(",")[0].strip()
            if name.lower().endswith(".csv"):
                csv_files.append(name)

        csv_files.sort()
        log.info("Found %d CSV files in the dataset", len(csv_files))
        return csv_files

    except (FileNotFoundError, RuntimeError) as exc:
        log.warning("kaggle CLI failed (%s) — falling back to REST API …", exc)
        return _list_csv_files_rest(dataset)


def _list_csv_files_rest(dataset: str) -> list[str]:
    username, key = _kaggle_auth()
    owner, slug   = dataset.split("/", 1)

    endpoints = [
        f"{KAGGLE_API_BASE}/datasets/{owner}/{slug}/files?page=1&pageSize=500",
        f"{KAGGLE_API_BASE}/datasets/{owner}/{slug}?fields=files",
    ]

    for url in endpoints:
        try:
            resp = requests.get(url, auth=(username, key), timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            data = resp.json()

            raw = data if isinstance(data, list) else (
                data.get("datasetFiles") or data.get("files") or []
            )
            csv_files = sorted(
                f.get("name", "") for f in raw
                if f.get("name", "").lower().endswith(".csv")
            )
            if csv_files:
                log.info("Found %d CSV files via REST fallback", len(csv_files))
                return csv_files
        except Exception as exc:
            log.warning("REST endpoint %s failed: %s", url, exc)

    raise RuntimeError(
        f"Could not list files for dataset '{dataset}'.\n"
        "Check KAGGLE_USERNAME / KAGGLE_KEY and that the dataset slug is correct."
    )


# ── Kaggle: download one CSV ──────────────────────────────────────────────────

def _normalise_df(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "ticker" not in df.columns and "symbol" not in df.columns:
        ticker = os.path.splitext(os.path.basename(filename))[0].upper()
        df.insert(0, "ticker", ticker)
    elif "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    return df


def _is_stock_file(df: pd.DataFrame) -> bool:
    """
    Return True only when the DataFrame has ALL required stock columns.
    Columns are already lower-cased by _normalise_df at this point.
    This rejects Congress/transaction files which lack open/high/low/close/volume.
    """
    return REQUIRED_STOCK_COLS.issubset(set(df.columns))


def download_single_csv(filename: str, dataset: str = KAGGLE_DATASET) -> pd.DataFrame:
    # ── Strategy 1: kaggle CLI ────────────────────────────────────────────
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    "kaggle", "datasets", "download",
                    dataset,
                    "--file", filename,
                    "--path", tmpdir,
                    "--unzip",
                    "--quiet",
                ],
                capture_output=True, text=True,
                env=_kaggle_env(), timeout=120,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "kaggle CLI returned non-zero")

            for root, _dirs, files in os.walk(tmpdir):
                for fname in files:
                    if fname.lower().endswith(".csv"):
                        df = pd.read_csv(os.path.join(root, fname), low_memory=False)
                        return _normalise_df(df, filename)

            raise ValueError(f"CLI downloaded nothing recognisable for {filename}")

    except Exception as cli_err:
        log.warning("kaggle CLI download failed for %s (%s) — trying REST …", filename, cli_err)

    # ── Strategy 2: REST download endpoint ───────────────────────────────
    username, key = _kaggle_auth()
    url = f"{KAGGLE_API_BASE}/datasets/download/{dataset}/{filename}"

    resp = requests.get(url, auth=(username, key), stream=True, timeout=120)
    resp.raise_for_status()

    buf = io.BytesIO(resp.content)

    if zipfile.is_zipfile(buf):
        buf.seek(0)
        with zipfile.ZipFile(buf) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside ZIP for {filename}")
            with zf.open(csv_names[0]) as f:
                df = pd.read_csv(f, low_memory=False)
    else:
        buf.seek(0)
        df = pd.read_csv(buf, low_memory=False)

    return _normalise_df(df, filename)


# ── Background download thread ────────────────────────────────────────────────

def _download_worker(
    csv_files: list[str],
    ticker_filter: Optional[str],
    file_queue: "queue.Queue",
    dashboard: Dashboard,
) -> None:
    for fname in csv_files:
        normalised = fname.lower().replace("\\", "/")

        # ── Guard 1: path prefix — only StockHistory files ───────────────
        if not normalised.startswith(STOCK_PATH_PREFIX):
            log.info("[DL] Skipping non-StockHistory file: %s", fname)
            dashboard.file_skipped()
            continue

        # ── Guard 2: ticker filter ────────────────────────────────────────
        ticker_hint = os.path.splitext(os.path.basename(fname))[0].upper()
        if ticker_filter and ticker_hint != ticker_filter.upper():
            dashboard.file_skipped()
            continue

        log.info("[DL] Downloading %s …", fname)
        try:
            df = download_single_csv(fname)
            if df.empty:
                log.warning("[DL] %s is empty — skipping", fname)
                dashboard.file_skipped()
                continue

            # ── Guard 3: required column check ───────────────────────────
            # Rejects any non-stock file regardless of path casing.
            # StockHistory files have: date, open, high, low, close, volume,
            # dividends, stock_splits, stochk_14_3_3, stochd_14_3_3
            # Congress files have completely different columns → rejected here.
            if not _is_stock_file(df):
                log.warning(
                    "[DL] %s missing required stock columns (found: %s) — skipping",
                    fname, list(df.columns),
                )
                dashboard.file_skipped()
                continue

            date_cols = [c for c in df.columns if c in ("date", "timestamp", "time")]
            if date_cols:
                df = df.sort_values(date_cols[0], ascending=True)

            log.info("[DL] ✓ %s  (%d rows) → queued", fname, len(df))
            file_queue.put((fname, df))

        except Exception as exc:
            log.warning("[DL] Skipping %s — %s", fname, exc)
            dashboard.file_skipped()

    file_queue.put(_SENTINEL)
    log.info("[DL] All files downloaded. Thread exiting.")


# ── Event schema ──────────────────────────────────────────────────────────────
#
# Real StockHistory CSV columns (after _normalise_df lower-cases them):
#   date            – trading date  (YYYY-MM-DD)
#   open            – opening price
#   high            – intraday high
#   low             – intraday low
#   close           – closing price
#   volume          – shares traded
#   dividends       – cash dividend paid on this date (0.0 when none)
#   stock_splits    – split ratio on this date        (0.0 when none)
#   stochk_14_3_3   – Stochastic %K (14,3,3)
#   stochd_14_3_3   – Stochastic %D (14,3,3)
#
# Ticker is NOT in the CSV — it is derived from the filename.
# _normalise_df already inserts it as the "ticker" column.

def row_to_event(row: pd.Series, source_file: str, event_id: int) -> dict:
    """
    Convert one DataFrame row (already normalised by _normalise_df) into
    the canonical JSON event that is written to Kafka.

    All numeric fields use _safe() so NaN / None become JSON null rather
    than the string "nan" or raising a serialisation error.
    """

    def _safe(key: str) -> Optional[float]:
        """Return float value or None — never NaN, never raises."""
        val = row.get(key)
        if val is None:
            return None
        # pandas represents missing numerics as float NaN
        try:
            f = float(val)
            return None if pd.isna(f) else f
        except (TypeError, ValueError):
            return None

    # Date column is always "date" after normalisation.
    # Truncate to YYYY-MM-DD in case the raw value includes a time component.
    raw_date = row.get("date")
    date_val: Optional[str] = None
    if raw_date is not None and str(raw_date) not in ("", "nan", "NaT"):
        date_val = str(raw_date)[:10]

    # Ticker was inserted by _normalise_df (from the filename, e.g. AAPL.csv → AAPL)
    ticker = str(row.get("ticker", "UNKNOWN")).upper()

    return {
        # ── Identity ──────────────────────────────────────────────────────
        "event_id":       event_id,
        "ticker":         ticker,
        "date":           date_val,

        # ── Core OHLCV ────────────────────────────────────────────────────
        "open":           _safe("open"),
        "high":           _safe("high"),
        "low":            _safe("low"),
        "close":          _safe("close"),
        "volume":         _safe("volume"),

        # ── Corporate actions ─────────────────────────────────────────────
        "dividends":      _safe("dividends"),      # 0.0 on non-dividend days
        "stock_splits":   _safe("stock_splits"),   # 0.0 on non-split days

        # ── Technical indicators ──────────────────────────────────────────
        "stochk_14_3_3":  _safe("stochk_14_3_3"),
        "stochd_14_3_3":  _safe("stochd_14_3_3"),

        # ── Pipeline metadata ─────────────────────────────────────────────
        "source_file":    source_file,
        "produced_at":    datetime.utcnow().isoformat() + "Z",
    }


# ── Kafka producer helpers ────────────────────────────────────────────────────

def build_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers":            bootstrap_servers,
        "acks":                         "all",
        "retries":                      5,
        "retry.backoff.ms":             500,
        "linger.ms":                    10,
        "batch.size":                   65536,
        "compression.type":             "lz4",
        "enable.idempotence":           True,
        "max.in.flight.requests.per.connection": 5,
    }
    return Producer(conf)


def delivery_callback(err, msg) -> None:
    if err:
        log.error("Delivery FAILED | topic=%s | %s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s | partition=%d | offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="US Stocks Kafka Producer — parallel download + produce"
    )
    p.add_argument("--speed",      type=float, default=SPEED_FACTOR)
    p.add_argument("--batch-size", type=int,   default=MICRO_BATCH_SIZE)
    p.add_argument("--ticker",     type=str,   default=None)
    p.add_argument("--topic",      type=str,   default=None)
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()
    args = parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    log.info("=" * 62)
    log.info("  US Stocks Kafka Producer  [PARALLEL REAL-TIME MODE]")
    log.info("  Broker: %s  |  Topic: %s", bootstrap, topic)
    log.info("  Speed: %.1fx  |  Batch: %d rows", args.speed, args.batch_size)
    log.info("=" * 62)

    csv_files = list_kaggle_csv_files()

    # Count only stock files for the dashboard total
    stock_files = [
        f for f in csv_files
        if f.lower().replace("\\", "/").startswith(STOCK_PATH_PREFIX)
    ]
    dashboard = Dashboard(total_files=len(stock_files))
    log.info("Stock files to stream: %d  (skipping %d non-stock files)",
             len(stock_files), len(csv_files) - len(stock_files))

    file_queue: "queue.Queue" = queue.Queue(maxsize=QUEUE_MAXSIZE)
    dl_thread = threading.Thread(
        target=_download_worker,
        args=(csv_files, args.ticker, file_queue, dashboard),
        daemon=True,
        name="kaggle-downloader",
    )
    dl_thread.start()

    producer    = build_producer(bootstrap)
    delay_per_batch = max(
        0.0,
        (args.batch_size / 252 / 6.5 / 3600) / max(args.speed, 0.01),
    )

    total_events  = 0
    total_batches = 0
    event_id      = 0
    start_wall    = time.time()

    # Initial dashboard render
    dashboard.render()

    try:
        while True:
            item = file_queue.get()

            if item is _SENTINEL:
                log.info("All files consumed — flushing Kafka producer …")
                break

            fname, df = item
            dashboard.set_file(fname, len(df))
            dashboard.render()

            batch: list[dict] = []

            for _, row in df.iterrows():
                event = row_to_event(row, fname, event_id)
                event_id += 1
                batch.append(event)

                if len(batch) >= args.batch_size:
                    for ev in batch:
                        producer.produce(
                            topic=topic,
                            key=ev["ticker"].encode(),
                            value=json.dumps(ev, default=str).encode(),
                            callback=delivery_callback,
                        )
                    producer.poll(0)

                    total_events  += len(batch)
                    total_batches += 1
                    dashboard.add_events(len(batch))
                    batch = []

                    # Refresh dashboard every batch
                    dashboard.render()

                    if delay_per_batch > 0:
                        time.sleep(delay_per_batch)

            # Flush partial last batch
            if batch:
                for ev in batch:
                    producer.produce(
                        topic=topic,
                        key=ev["ticker"].encode(),
                        value=json.dumps(ev, default=str).encode(),
                        callback=delivery_callback,
                    )
                producer.poll(0)
                total_events  += len(batch)
                total_batches += 1
                dashboard.add_events(len(batch))

            dashboard.file_done()
            dashboard.render()
            log.info("  ✓ Finished producing %s", fname)

    except KeyboardInterrupt:
        log.info("Interrupted by user — flushing …")
    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        producer.flush(timeout=30)
        elapsed = time.time() - start_wall

        # Final dashboard
        dashboard.render()

        log.info("=" * 62)
        log.info("  PRODUCER DONE")
        log.info("  Total events  : %d", total_events)
        log.info("  Total batches : %d", total_batches)
        log.info("  Wall time     : %.1f s", elapsed)
        log.info("  Throughput    : %.0f events/s", total_events / max(elapsed, 1))
        log.info("  Topic         : %s", topic)
        log.info("=" * 62)


if __name__ == "__main__":
    main()