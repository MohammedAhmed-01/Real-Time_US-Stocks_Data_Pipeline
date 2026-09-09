"""
Kafka Producer — US Stock Dataset (Kaggle)  [PARALLEL REAL-TIME MODE]
======================================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Architecture:
  ┌──────────────────────┐     queue      ┌──────────────────────────┐
  │  DOWNLOAD THREAD     │ ─── (fname,df) ─▶  MAIN THREAD (producer) │
  │  downloads CSV files │               │  converts rows → events  │
  │  one after another   │               │  flushes to Kafka now    │
  └──────────────────────┘               └──────────────────────────┘

  • Download of file N+1 starts while file N is still being produced.
  • Memory is bounded — queue holds at most QUEUE_MAXSIZE DataFrames.
  • Events flow into Kafka the moment each row is ready.
  • Consumer can start reading immediately — no full-dataset wait.

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
QUEUE_MAXSIZE    = 3          # max DataFrames buffered between threads
_SENTINEL        = object()   # signals download thread is done

# Only stream files under this folder — the dataset also contains
# Data/Congress/ (congressional trades) which has no OHLCV columns.
STOCK_PATH_PREFIX = "data/stockhistory"   # lower-cased for comparison


# ── Kaggle auth ───────────────────────────────────────────────────────────────

def _kaggle_auth() -> tuple[str, str]:
    """Return (username, key) from env or ~/.kaggle/kaggle.json."""
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


# ── Kaggle: list files ────────────────────────────────────────────────────────

def _kaggle_env() -> dict:
    """Build env dict with KAGGLE_USERNAME/KEY injected for CLI subprocess calls."""
    username, key = _kaggle_auth()
    return {**os.environ, "KAGGLE_USERNAME": username, "KAGGLE_KEY": key}


def list_kaggle_csv_files(dataset: str = KAGGLE_DATASET) -> list[str]:
    """
    Return sorted list of CSV filenames in the Kaggle dataset.

    Uses the kaggle CLI (`kaggle datasets files <dataset> --csv`) which is
    already installed via requirements.txt and works on every kaggle package
    version (1.5, 1.6, 1.7 …).  No SDK import needed.

    Falls back to the v1 REST API if the CLI is not on PATH.
    """
    log.info("Fetching file list for dataset '%s' via kaggle CLI …", dataset)

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

        # CLI output (--csv): header line then  name,size,creationDate
        lines = result.stdout.strip().splitlines()
        csv_files = []
        for line in lines[1:]:          # skip header
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
    """
    REST fallback: GET /api/v1/datasets/{owner}/{slug}/files
    Tries both the v1 JSON envelope and the older bare-list format.
    """
    username, key = _kaggle_auth()
    owner, slug   = dataset.split("/", 1)

    # Kaggle v1 — try the correct endpoint (note: no /files suffix in some versions)
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

            # Handle both {"datasetFiles":[...]} and bare list
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
    """Normalise column names and ensure a ticker column exists."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "ticker" not in df.columns and "symbol" not in df.columns:
        ticker = os.path.splitext(os.path.basename(filename))[0].upper()
        df.insert(0, "ticker", ticker)
    elif "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    return df


def download_single_csv(
    filename: str,
    dataset: str = KAGGLE_DATASET,
) -> pd.DataFrame:
    """
    Download a single CSV from Kaggle and return a parsed DataFrame.

    Strategy (tries in order, stops at first success):
      1. kaggle CLI  → `kaggle datasets download -f <filename> --unzip`
         Written to a temp dir so the container stays stateless.
      2. REST download endpoint  → in-memory, handles ZIP wrapper.

    Using the CLI as primary means we're immune to Kaggle API changes.
    """
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
                capture_output=True,
                text=True,
                env=_kaggle_env(),
                timeout=120,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "kaggle CLI returned non-zero")

            # Walk tmpdir for the downloaded CSV (may be nested)
            for root, _dirs, files in os.walk(tmpdir):
                for fname in files:
                    if fname.lower().endswith(".csv"):
                        df = pd.read_csv(
                            os.path.join(root, fname), low_memory=False
                        )
                        return _normalise_df(df, filename)

            raise ValueError(f"CLI downloaded nothing recognisable for {filename}")

    except Exception as cli_err:
        log.warning(
            "kaggle CLI download failed for %s (%s) — trying REST …",
            filename, cli_err,
        )

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
    file_queue: "queue.Queue[tuple | object]",
) -> None:
    """
    Runs in a background thread.
    Downloads each CSV sequentially and puts (fname, df) into the queue.
    Puts _SENTINEL when done so the main thread knows to stop.

    The queue's maxsize acts as back-pressure: if the producer is slower
    than the downloader, the thread blocks here instead of consuming RAM.
    """
    for fname in csv_files:
        # Only process files inside Data/StockHistory/ — skip everything else
        # (e.g. Data/Congress/houseTransactions.csv, senateTransactions.csv)
        if not fname.lower().replace("\\", "/").startswith(STOCK_PATH_PREFIX):
            log.info("[DL-THREAD] Skipping non-StockHistory file: %s", fname)
            continue

        ticker_hint = os.path.splitext(os.path.basename(fname))[0].upper()
        if ticker_filter and ticker_hint != ticker_filter.upper():
            continue

        log.info("[DL-THREAD] Downloading %s …", fname)
        try:
            df = download_single_csv(fname)
            if df.empty:
                log.warning("[DL-THREAD] %s is empty — skipping", fname)
                continue

            # Sort chronologically before queuing
            date_cols = [c for c in df.columns if c in ("date", "timestamp", "time")]
            if date_cols:
                df = df.sort_values(date_cols[0], ascending=True)

            log.info("[DL-THREAD] ✓ %s  (%d rows) → queued for producer", fname, len(df))
            file_queue.put((fname, df))   # blocks if queue is full (back-pressure)

        except Exception as exc:
            log.warning("[DL-THREAD] Skipping %s — %s", fname, exc)

    # Signal main thread that there are no more files
    file_queue.put(_SENTINEL)
    log.info("[DL-THREAD] All files downloaded. Thread exiting.")


# ── Event schema ──────────────────────────────────────────────────────────────

def row_to_event(row: pd.Series, source_file: str, event_id: int) -> dict:
    """Convert a DataFrame row to the canonical JSON event schema."""
    def _safe(key: str) -> Optional[float]:
        val = row.get(key)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    date_val = None
    for col in ("date", "timestamp", "time", "datetime"):
        if col in row.index and row[col] and str(row[col]) != "nan":
            date_val = str(row[col])[:10]
            break

    return {
        "event_id":    event_id,
        "ticker":      str(row.get("ticker", row.get("symbol", "UNKNOWN"))).upper(),
        "date":        date_val,
        "open":        _safe("open"),
        "high":        _safe("high"),
        "low":         _safe("low"),
        "close":       _safe("close"),
        "adj_close":   _safe("adj_close") or _safe("adjusted_close"),
        "volume":      _safe("volume"),
        "source_file": source_file,
        "produced_at": datetime.utcnow().isoformat() + "Z",
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
    p.add_argument("--speed",      type=float, default=SPEED_FACTOR,
                   help="Speed multiplier (>1 = faster replay)")
    p.add_argument("--batch-size", type=int,   default=MICRO_BATCH_SIZE,
                   help="Rows per micro-batch")
    p.add_argument("--ticker",     type=str,   default=None,
                   help="Stream only this ticker symbol")
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
    log.info("=" * 62)
    log.info("  Broker     : %s", bootstrap)
    log.info("  Topic      : %s", topic)
    log.info("  Speed      : %.1fx  |  Batch size: %d rows", args.speed, args.batch_size)
    log.info("  Queue size : %d files (back-pressure buffer)", QUEUE_MAXSIZE)
    log.info("  Mode       : download thread → queue → produce thread (parallel)")
    log.info("=" * 62)

    # Step 1: Get file list (no data downloaded yet — fast)
    csv_files = list_kaggle_csv_files()

    # Step 2: Start background download thread
    file_queue: "queue.Queue" = queue.Queue(maxsize=QUEUE_MAXSIZE)
    dl_thread = threading.Thread(
        target=_download_worker,
        args=(csv_files, args.ticker, file_queue),
        daemon=True,
        name="kaggle-downloader",
    )
    dl_thread.start()
    log.info("Download thread started — producing starts as soon as first file arrives.")

    # Step 3: Build Kafka producer
    producer = build_producer(bootstrap)

    # How long to sleep between micro-batches to simulate real-time pace
    # (set to 0 for maximum throughput)
    delay_per_batch = max(0.0, (args.batch_size / 252 / 6.5 / 3600) / max(args.speed, 0.01))

    total_events  = 0
    total_batches = 0
    event_id      = 0
    start_wall    = time.time()

    try:
        while True:
            # Block until a file is ready (or done)
            item = file_queue.get()

            if item is _SENTINEL:
                log.info("All files consumed — flushing Kafka producer …")
                break

            fname, df = item
            ticker_hint = os.path.splitext(os.path.basename(fname))[0].upper()
            log.info(
                "Producing  %s  |  %d rows  |  queue depth: %d",
                fname, len(df), file_queue.qsize(),
            )

            batch: list[dict] = []

            for _, row in df.iterrows():
                event = row_to_event(row, fname, event_id)
                event_id += 1
                batch.append(event)

                if len(batch) >= args.batch_size:
                    # Produce the micro-batch to Kafka immediately
                    for ev in batch:
                        producer.produce(
                            topic=topic,
                            key=ev["ticker"].encode(),
                            value=json.dumps(ev, default=str).encode(),
                            callback=delivery_callback,
                        )
                    producer.poll(0)   # trigger delivery callbacks (non-blocking)

                    total_events  += len(batch)
                    total_batches += 1
                    batch = []

                    if delay_per_batch > 0:
                        time.sleep(delay_per_batch)

                    if total_batches % 20 == 0:
                        elapsed = time.time() - start_wall
                        log.info(
                            "  ↳ Progress: %d batches | %d events | %.0f ev/s",
                            total_batches,
                            total_events,
                            total_events / max(elapsed, 1e-9),
                        )

            # Flush remaining rows in the last partial batch
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

            log.info("  ✓ Finished producing %s", fname)

    except KeyboardInterrupt:
        log.info("Interrupted by user — flushing …")
    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        producer.flush(timeout=30)
        elapsed = time.time() - start_wall
        log.info("=" * 62)
        log.info("  DONE")
        log.info("  Total events  : %d", total_events)
        log.info("  Total batches : %d", total_batches)
        log.info("  Wall time     : %.1f s", elapsed)
        log.info("  Throughput    : %.0f events/s", total_events / max(elapsed, 1))
        log.info("  Topic         : %s", topic)
        log.info("=" * 62)


if __name__ == "__main__":
    main()