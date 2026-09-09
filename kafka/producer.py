"""
producer.py — US Stock Dataset Kafka Producer (Parallel Real-Time Mode)
=======================================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Architecture:
    Download Thread  →  queue(fname, df)  →  Main Thread (producer)
    Downloads CSV files from Kaggle and enqueues them; the main thread
    converts rows to JSON events and publishes them to Kafka.

Usage:
    python producer.py [--speed FLOAT] [--batch-size INT] [--ticker SYMBOL]

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

KAGGLE_DATASET      = "footballjoe789/us-stock-dataset"
KAGGLE_API_BASE     = "https://www.kaggle.com/api/v1"
DEFAULT_TOPIC       = "us-stocks-raw"
DEFAULT_BROKER      = "localhost:9092"
MICRO_BATCH_SIZE    = 50
SPEED_FACTOR        = 1.0
QUEUE_MAXSIZE       = 3

STOCK_PATH_PREFIX   = "data/stockhistory"
REQUIRED_STOCK_COLS = {"date", "open", "high", "low", "close", "volume"}

_SENTINEL = object()


# ---------------------------------------------------------------------------
# Runtime statistics (thread-safe)
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self, total_files: int) -> None:
        self.total_files   = total_files
        self.done_files    = 0
        self.skipped_files = 0
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

    def elapsed_str(self) -> str:
        elapsed = int(time.time() - self.start_time)
        return f"{elapsed // 60:02d}:{elapsed % 60:02d}"

    def ev_per_s(self) -> float:
        elapsed = max(time.time() - self.start_time, 1e-9)
        return self.total_events / elapsed


# ---------------------------------------------------------------------------
# Kaggle authentication
# ---------------------------------------------------------------------------

def _kaggle_auth() -> tuple[str, str]:
    """Return (username, key) from env vars or ~/.kaggle/kaggle.json."""
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
    """Build an os.environ copy that always contains Kaggle credentials."""
    username, key = _kaggle_auth()
    return {**os.environ, "KAGGLE_USERNAME": username, "KAGGLE_KEY": key}


# ---------------------------------------------------------------------------
# Kaggle: list CSV files
# ---------------------------------------------------------------------------

def list_kaggle_csv_files(dataset: str = KAGGLE_DATASET) -> list[str]:
    """Return a sorted list of all CSV file paths in the Kaggle dataset."""
    log.info("Fetching file list for dataset '%s' …", dataset)

    try:
        result = subprocess.run(
            ["kaggle", "datasets", "files", dataset, "--csv"],
            capture_output=True,
            text=True,
            env=_kaggle_env(),
            timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())

        lines     = result.stdout.strip().splitlines()
        csv_files = sorted(
            line.split(",")[0].strip()
            for line in lines[1:]
            if line.split(",")[0].strip().lower().endswith(".csv")
        )
        log.info("Found %d CSV files in the dataset.", len(csv_files))
        return csv_files

    except (FileNotFoundError, RuntimeError) as exc:
        log.warning("kaggle CLI failed (%s) — falling back to REST API …", exc)
        return _list_csv_files_rest(dataset)


def _list_csv_files_rest(dataset: str) -> list[str]:
    """REST-API fallback for listing CSV files when the CLI is unavailable."""
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
            raw  = data if isinstance(data, list) else (
                data.get("datasetFiles") or data.get("files") or []
            )
            csv_files = sorted(
                f.get("name", "")
                for f in raw
                if f.get("name", "").lower().endswith(".csv")
            )
            if csv_files:
                log.info("Found %d CSV files via REST fallback.", len(csv_files))
                return csv_files

        except Exception as exc:
            log.warning("REST endpoint %s failed: %s", url, exc)

    raise RuntimeError(
        f"Could not list files for dataset '{dataset}'. "
        "Check KAGGLE_USERNAME / KAGGLE_KEY and the dataset slug."
    )


# ---------------------------------------------------------------------------
# Kaggle: download one CSV file
# ---------------------------------------------------------------------------

def _normalise_df(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Lowercase and underscore column names; ensure a 'ticker' column exists."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "ticker" not in df.columns and "symbol" not in df.columns:
        ticker = os.path.splitext(os.path.basename(filename))[0].upper()
        df.insert(0, "ticker", ticker)
    elif "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    return df


def _is_stock_file(df: pd.DataFrame) -> bool:
    """Return True if the DataFrame contains all required OHLCV columns."""
    return REQUIRED_STOCK_COLS.issubset(set(df.columns))


def download_single_csv(filename: str, dataset: str = KAGGLE_DATASET) -> pd.DataFrame:
    """Download one CSV from Kaggle (CLI first, REST fallback) and return a DataFrame."""
    # --- CLI path ---
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    "kaggle", "datasets", "download",
                    dataset,
                    "--file",  filename,
                    "--path",  tmpdir,
                    "--unzip",
                    "--quiet",
                ],
                capture_output=True,
                text=True,
                env=_kaggle_env(),
                timeout=120,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "kaggle CLI returned non-zero")

            for root, _dirs, files in os.walk(tmpdir):
                for fname in files:
                    if fname.lower().endswith(".csv"):
                        df = pd.read_csv(os.path.join(root, fname), low_memory=False)
                        return _normalise_df(df, filename)

            raise ValueError(f"CLI downloaded nothing recognisable for {filename}.")

    except Exception as cli_err:
        log.warning("kaggle CLI download failed for %s (%s) — trying REST …", filename, cli_err)

    # --- REST fallback ---
    username, key = _kaggle_auth()
    url  = f"{KAGGLE_API_BASE}/datasets/download/{dataset}/{filename}"
    resp = requests.get(url, auth=(username, key), stream=True, timeout=120)
    resp.raise_for_status()

    buf = io.BytesIO(resp.content)

    if zipfile.is_zipfile(buf):
        buf.seek(0)
        with zipfile.ZipFile(buf) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside ZIP for {filename}.")
            with zf.open(csv_names[0]) as fh:
                df = pd.read_csv(fh, low_memory=False)
    else:
        buf.seek(0)
        df = pd.read_csv(buf, low_memory=False)

    return _normalise_df(df, filename)


# ---------------------------------------------------------------------------
# Background download thread
# ---------------------------------------------------------------------------

def _download_worker(
    csv_files: list[str],
    ticker_filter: Optional[str],
    file_queue: queue.Queue,
    stats: _Stats,
) -> None:
    """Download CSV files one by one and push (filename, DataFrame) onto the queue."""
    for fname in csv_files:
        normalised = fname.lower().replace("\\", "/")

        if not normalised.startswith(STOCK_PATH_PREFIX):
            log.debug("Skipping non-StockHistory file: %s", fname)
            stats.file_skipped()
            continue

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
                log.warning(
                    "[DL] %s missing required stock columns (found: %s) — skipping.",
                    fname, list(df.columns),
                )
                stats.file_skipped()
                continue

            date_cols = [c for c in df.columns if c in ("date", "timestamp", "time")]
            if date_cols:
                df = df.sort_values(date_cols[0], ascending=True)

            log.info("[DL] Ready  file=%-30s  rows=%d", os.path.basename(fname), len(df))
            file_queue.put((fname, df))

        except Exception as exc:
            log.warning("[DL] Skipping %s — %s", fname, exc)
            stats.file_skipped()

    file_queue.put(_SENTINEL)
    log.info("[DL] All files queued. Download thread done.")


# ---------------------------------------------------------------------------
# Event construction
# ---------------------------------------------------------------------------

def row_to_event(row: pd.Series, source_file: str, event_id: int) -> dict:
    """Convert a single DataFrame row to a canonical Kafka event dict."""

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
        "event_id":      event_id,
        "ticker":        str(row.get("ticker", "UNKNOWN")).upper(),
        "date":          date_str,
        "open":          _safe("open"),
        "high":          _safe("high"),
        "low":           _safe("low"),
        "close":         _safe("close"),
        "volume":        _safe("volume"),
        "dividends":     _safe("dividends"),
        "stock_splits":  _safe("stock_splits"),
        "stochk_14_3_3": _safe("stochk_14_3_3"),
        "stochd_14_3_3": _safe("stochd_14_3_3"),
        "source_file":   source_file,
        "produced_at":   datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Kafka producer factory
# ---------------------------------------------------------------------------

def build_producer(bootstrap_servers: str) -> Producer:
    """Return a configured confluent-kafka Producer."""
    return Producer({
        "bootstrap.servers":                      bootstrap_servers,
        "acks":                                   "all",
        "retries":                                5,
        "retry.backoff.ms":                       500,
        "linger.ms":                              10,
        "batch.size":                             65536,
        "compression.type":                       "lz4",
        "enable.idempotence":                     True,
        "max.in.flight.requests.per.connection":  5,
    })


def delivery_callback(err, msg) -> None:
    """Log delivery success or failure for each produced message."""
    if err:
        log.error("Delivery FAILED | topic=%s | %s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s | partition=%d | offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ---------------------------------------------------------------------------
# Produce one batch to Kafka
# ---------------------------------------------------------------------------

def _produce_batch(producer: Producer, topic: str, batch: list[dict]) -> None:
    """Produce all events in *batch* and trigger a non-blocking poll."""
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
        description="US Stocks Kafka Producer — parallel download + produce"
    )
    parser.add_argument("--speed",      type=float, default=SPEED_FACTOR,
                        help="Replay speed multiplier (default: 1.0)")
    parser.add_argument("--batch-size", type=int,   default=MICRO_BATCH_SIZE,
                        help="Rows per Kafka micro-batch (default: 50)")
    parser.add_argument("--ticker",     type=str,   default=None,
                        help="Stream only this ticker symbol")
    parser.add_argument("--topic",      type=str,   default=None,
                        help="Override the target Kafka topic")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()
    args = _parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    log.info("=" * 62)
    log.info("  US Stocks Kafka Producer  [PARALLEL REAL-TIME MODE]")
    log.info("  Broker  : %s", bootstrap)
    log.info("  Topic   : %s", topic)
    log.info("  Speed   : %.1fx  |  Batch: %d rows", args.speed, args.batch_size)
    log.info("=" * 62)

    csv_files   = list_kaggle_csv_files()
    stock_files = [
        f for f in csv_files
        if f.lower().replace("\\", "/").startswith(STOCK_PATH_PREFIX)
    ]
    log.info(
        "Stock files to stream: %d  (skipping %d non-stock files)",
        len(stock_files), len(csv_files) - len(stock_files),
    )

    stats      = _Stats(total_files=len(stock_files))
    file_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

    dl_thread = threading.Thread(
        target=_download_worker,
        args=(csv_files, args.ticker, file_queue, stats),
        daemon=True,
        name="kaggle-downloader",
    )
    dl_thread.start()

    producer        = build_producer(bootstrap)
    delay_per_batch = max(
        0.0,
        (args.batch_size / 252 / 6.5 / 3600) / max(args.speed, 0.01),
    )

    total_events  = 0
    total_batches = 0
    event_id      = 0
    start_wall    = time.time()

    try:
        while True:
            item = file_queue.get()
            if item is _SENTINEL:
                log.info("All files consumed — flushing Kafka producer …")
                break

            fname, df   = item
            basename    = os.path.basename(fname)
            file_rows   = len(df)
            file_events = 0
            file_start  = time.time()

            log.info(
                "START  file=%-12s  rows=%d  overall=%d/%d done  total_events=%d",
                basename, file_rows, stats.done_files, len(stock_files), total_events,
            )

            batch: list[dict] = []

            for _, row in df.iterrows():
                batch.append(row_to_event(row, fname, event_id))
                event_id += 1

                if len(batch) >= args.batch_size:
                    _produce_batch(producer, topic, batch)

                    total_events  += len(batch)
                    total_batches += 1
                    file_events   += len(batch)
                    stats.add_events(len(batch))

                    log.info(
                        "  sent=%5d/%5d (%5.1f%%)  ev/s=%6.0f  elapsed=%s  total=%d",
                        file_events, file_rows,
                        file_events / max(file_rows, 1) * 100,
                        file_events / max(time.time() - file_start, 1e-9),
                        stats.elapsed_str(),
                        total_events,
                    )

                    batch = []
                    if delay_per_batch > 0:
                        time.sleep(delay_per_batch)

            # Flush remaining partial batch
            if batch:
                _produce_batch(producer, topic, batch)
                total_events  += len(batch)
                total_batches += 1
                file_events   += len(batch)
                stats.add_events(len(batch))

            stats.file_done()
            file_elapsed = time.time() - file_start
            log.info(
                "DONE   file=%-12s  events=%d  time=%.1fs  ev/s=%.0f  "
                "overall=%d/%d  grand_total=%d",
                basename, file_events, file_elapsed,
                file_events / max(file_elapsed, 1e-9),
                stats.done_files, len(stock_files), total_events,
            )

    except KeyboardInterrupt:
        log.info("Interrupted by user — flushing …")
    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        producer.flush(timeout=30)
        elapsed = time.time() - start_wall
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