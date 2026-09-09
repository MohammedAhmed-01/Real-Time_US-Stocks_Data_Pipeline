"""
Kafka Producer — US Stock Dataset (Kaggle)
==========================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Downloads each CSV file INDIVIDUALLY from the Kaggle API so events are
produced immediately as each file arrives — true real-time streaming
instead of buffering the full 600 MB ZIP before publishing anything.

Flow:
    list files → for each CSV: download → parse → produce → next file

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

def list_kaggle_csv_files(dataset: str = KAGGLE_DATASET) -> list[str]:
    """
    Return sorted list of CSV filenames in the dataset using the Kaggle SDK.

    We use the kaggle Python package (already in requirements.txt) to call
    dataset_list_files() which hits the API without downloading any data.

    Args:
        dataset: 'owner/dataset-name'

    Returns:
        Sorted list of member filenames that end in .csv
    """
    from kaggle.api.kaggle_api_extended import KaggleApiExtended

    api = KaggleApiExtended()
    api.authenticate()

    log.info("Fetching file list for dataset '%s' …", dataset)
    response = api.dataset_list_files(dataset)

    csv_files = sorted(
        f.name for f in response.files
        if f.name.lower().endswith(".csv")
    )
    log.info("Found %d CSV files in the dataset", len(csv_files))
    return csv_files


# ── Kaggle: download one CSV ──────────────────────────────────────────────────

def download_single_csv(
    filename: str,
    dataset: str = KAGGLE_DATASET,
) -> pd.DataFrame:
    """
    Download a SINGLE CSV file from the Kaggle dataset into memory and
    return a parsed DataFrame.  The response is either raw CSV or a small
    ZIP containing just that file — both cases are handled.

    This is the core change vs the original producer: instead of buffering
    the entire 600 MB ZIP, we download one ~50–200 KB CSV at a time and
    start producing events immediately.

    Args:
        filename: The member filename returned by list_kaggle_csv_files().
        dataset:  'owner/dataset-name'

    Returns:
        Parsed DataFrame with normalised column names.
    """
    username, key = _kaggle_auth()
    url = f"{KAGGLE_API_BASE}/datasets/download/{dataset}/{filename}"

    resp = requests.get(url, auth=(username, key), stream=True, timeout=60)
    resp.raise_for_status()

    buf = io.BytesIO(resp.content)

    # Kaggle sometimes wraps the single file in a small ZIP
    if zipfile.is_zipfile(buf):
        buf.seek(0)
        with zipfile.ZipFile(buf) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside zip for {filename}")
            with zf.open(csv_names[0]) as f:
                df = pd.read_csv(f, low_memory=False)
    else:
        buf.seek(0)
        df = pd.read_csv(buf, low_memory=False)

    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Ensure ticker column exists
    if "ticker" not in df.columns and "symbol" not in df.columns:
        ticker = os.path.splitext(os.path.basename(filename))[0].upper()
        df.insert(0, "ticker", ticker)
    elif "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    return df


# ── Event schema ──────────────────────────────────────────────────────────────

def row_to_event(row: pd.Series, source_file: str) -> dict:
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


# ── Micro-batch generator ─────────────────────────────────────────────────────

def micro_batch_events(
    csv_files: list[str],
    batch_size: int = MICRO_BATCH_SIZE,
    ticker_filter: Optional[str] = None,
) -> Generator[list[dict], None, None]:
    """
    For each CSV in the list: download it, parse it, yield micro-batches
    of events immediately — no waiting for other files to finish.

    Args:
        csv_files:     Filenames from list_kaggle_csv_files().
        batch_size:    Rows per yielded batch.
        ticker_filter: If set, only emit rows for this ticker symbol.

    Yields:
        List of event dicts (length ≤ batch_size).
    """
    event_id = 0

    for fname in csv_files:
        ticker_hint = os.path.splitext(os.path.basename(fname))[0].upper()
        if ticker_filter and ticker_hint != ticker_filter.upper():
            continue

        log.info("Downloading %s …", fname)
        try:
            df = download_single_csv(fname)
        except Exception as exc:
            log.warning("Skipping %s — %s", fname, exc)
            continue

        if df.empty:
            continue

        # Sort chronologically
        date_cols = [c for c in df.columns if c in ("date", "timestamp", "time")]
        if date_cols:
            df = df.sort_values(date_cols[0], ascending=True)

        log.info("  → %d rows | ticker=%s | producing now …", len(df), ticker_hint)

        batch: list[dict] = []
        for _, row in df.iterrows():
            event = row_to_event(row, fname)
            event["event_id"] = event_id
            event_id += 1
            batch.append(event)

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

        log.info("  ✓ Done with %s", fname)


# ── Kafka producer ────────────────────────────────────────────────────────────

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
    p = argparse.ArgumentParser(description="US Stocks Kafka Producer (streaming per-file)")
    p.add_argument("--speed",      type=float, default=SPEED_FACTOR)
    p.add_argument("--batch-size", type=int,   default=MICRO_BATCH_SIZE)
    p.add_argument("--ticker",     type=str,   default=None)
    p.add_argument("--topic",      type=str,   default=None)
    return p.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    log.info("=== US Stocks Kafka Producer (real-time per-file mode) ===")
    log.info("Broker : %s", bootstrap)
    log.info("Topic  : %s", topic)
    log.info("Speed  : %.1fx  |  Batch size: %d rows", args.speed, args.batch_size)
    log.info("Mode   : download one CSV → produce immediately → next CSV")

    producer = build_producer(bootstrap)

    # Step 1: get file list (fast — no data downloaded yet)
    csv_files = list_kaggle_csv_files()

    total_events   = 0
    total_batches  = 0
    delay_per_batch = (args.batch_size / 252 / 6.5 / 3600) / args.speed

    try:
        # Step 2: for each file: download → parse → produce → next
        for batch in micro_batch_events(csv_files, args.batch_size, args.ticker):
            for event in batch:
                key   = event["ticker"].encode()
                value = json.dumps(event, default=str).encode()
                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=delivery_callback,
                )

            producer.poll(0)

            total_events  += len(batch)
            total_batches += 1

            if total_batches % 20 == 0:
                log.info(
                    "Progress: %d batches | %d events produced",
                    total_batches, total_events,
                )

            if delay_per_batch > 0:
                time.sleep(delay_per_batch)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        log.info("Flushing remaining messages …")
        producer.flush(timeout=30)
        log.info(
            "Done — %d events in %d batches sent to topic '%s'",
            total_events, total_batches, topic,
        )


if __name__ == "__main__":
    main()