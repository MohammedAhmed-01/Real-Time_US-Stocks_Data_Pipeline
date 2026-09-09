"""
Kafka Producer — US Stock Dataset (Kaggle)
==========================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Reads CSV files directly from the Kaggle API (no local download),
serialises each row as a canonical JSON event, and publishes to Kafka.

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
from typing import Generator, Iterator, Optional

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
MICRO_BATCH_SIZE = 50          # rows per simulated "tick"
SPEED_FACTOR     = 1.0         # 1.0 = real-time cadence; >1 = faster


# ── Kaggle helper ─────────────────────────────────────────────────────────────

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


def stream_kaggle_zip(dataset: str = KAGGLE_DATASET) -> zipfile.ZipFile:
    """
    Download the dataset zip from Kaggle API into memory and return a ZipFile.

    Args:
        dataset: Kaggle dataset identifier  owner/dataset-name

    Returns:
        In-memory ZipFile ready for reading.

    Raises:
        requests.HTTPError: on non-200 responses.
    """
    username, key = _kaggle_auth()
    url = f"{KAGGLE_API_BASE}/datasets/download/{dataset}"

    log.info("Connecting to Kaggle API → %s", url)
    resp = requests.get(url, auth=(username, key), stream=True, timeout=120)
    resp.raise_for_status()

    log.info("Buffering dataset into memory …")
    buf = io.BytesIO()
    total = 0
    for chunk in resp.iter_content(chunk_size=1 << 20):   # 1 MB chunks
        buf.write(chunk)
        total += len(chunk)
        if total % (20 << 20) == 0:
            log.info("  … %.0f MB received", total / 1e6)

    buf.seek(0)
    log.info("Download complete — %.1f MB in memory", total / 1e6)
    return zipfile.ZipFile(buf)


def list_csv_files(zf: zipfile.ZipFile) -> list[str]:
    """Return sorted list of .csv members inside the ZipFile."""
    return sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))


def read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> pd.DataFrame:
    """
    Read a single CSV from an open ZipFile into a DataFrame.

    Args:
        zf:   Open ZipFile object.
        name: Member filename.

    Returns:
        Parsed DataFrame with normalised column names.
    """
    with zf.open(name) as f:
        df = pd.read_csv(f, low_memory=False)

    # Normalise column names: strip whitespace, lowercase
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Derive ticker symbol from filename if not in columns
    if "ticker" not in df.columns and "symbol" not in df.columns:
        ticker = os.path.splitext(os.path.basename(name))[0].upper()
        df.insert(0, "ticker", ticker)
    elif "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    return df


# ── Event schema ──────────────────────────────────────────────────────────────

def row_to_event(row: pd.Series, source_file: str) -> dict:
    """
    Convert a DataFrame row to the canonical JSON event schema.

    Canonical fields (all lower-case):
        event_id    – monotonic counter (added by caller)
        ticker      – stock symbol
        date        – trading date  YYYY-MM-DD
        open        – opening price
        high        – daily high
        low         – daily low
        close       – closing price
        adj_close   – adjusted close  (if available)
        volume      – trading volume
        source_file – originating CSV filename
        produced_at – ISO-8601 wall-clock timestamp

    Args:
        row:         Pandas Series (one CSV row).
        source_file: Name of the originating CSV file.

    Returns:
        Dict ready for JSON serialisation.
    """
    def _safe(key: str) -> Optional[float]:
        val = row.get(key)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    # Resolve date column
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
    zf: zipfile.ZipFile,
    batch_size: int = MICRO_BATCH_SIZE,
    ticker_filter: Optional[str] = None,
) -> Generator[list[dict], None, None]:
    """
    Yield micro-batches of canonical events from all CSVs in the zip.

    Args:
        zf:            Open ZipFile with the dataset.
        batch_size:    Rows per yielded batch.
        ticker_filter: If set, only emit rows for this ticker symbol.

    Yields:
        List of event dicts (length ≤ batch_size).
    """
    csv_files = list_csv_files(zf)
    log.info("Found %d CSV files in the dataset", len(csv_files))

    event_id = 0
    for fname in csv_files:
        ticker_hint = os.path.splitext(os.path.basename(fname))[0].upper()
        if ticker_filter and ticker_hint != ticker_filter.upper():
            continue

        log.info("Loading %s …", fname)
        try:
            df = read_csv_from_zip(zf, fname)
        except Exception as exc:
            log.warning("Skipping %s — %s", fname, exc)
            continue

        if df.empty:
            continue

        # Sort by date ascending so replay is chronological
        date_cols = [c for c in df.columns if c in ("date", "timestamp", "time")]
        if date_cols:
            df = df.sort_values(date_cols[0], ascending=True)

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


# ── Kafka producer ────────────────────────────────────────────────────────────

def build_producer(bootstrap_servers: str) -> Producer:
    """
    Build and return a confluent-kafka Producer.

    Args:
        bootstrap_servers: Comma-separated host:port pairs.

    Returns:
        Configured Producer instance.
    """
    conf = {
        "bootstrap.servers":            bootstrap_servers,
        "acks":                         "all",          # strongest guarantee
        "retries":                      5,
        "retry.backoff.ms":             500,
        "linger.ms":                    10,             # small batching window
        "batch.size":                   65536,          # 64 KB
        "compression.type":             "lz4",
        "enable.idempotence":           True,
        "max.in.flight.requests.per.connection": 5,
    }
    return Producer(conf)


def delivery_callback(err, msg) -> None:
    """Confluent-kafka delivery report callback."""
    if err:
        log.error("Delivery FAILED | topic=%s | %s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s | partition=%d | offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="US Stocks Kafka Producer")
    p.add_argument("--speed",      type=float, default=SPEED_FACTOR,
                   help="Replay speed multiplier (default 1.0 = real-time)")
    p.add_argument("--batch-size", type=int,   default=MICRO_BATCH_SIZE,
                   help="Rows per micro-batch (default 50)")
    p.add_argument("--ticker",     type=str,   default=None,
                   help="Only stream a specific ticker symbol, e.g. AAPL")
    p.add_argument("--topic",      type=str,   default=None,
                   help="Override Kafka topic name")
    return p.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    log.info("=== US Stocks Kafka Producer ===")
    log.info("Broker : %s", bootstrap)
    log.info("Topic  : %s", topic)
    log.info("Speed  : %.1fx  |  Batch size: %d rows", args.speed, args.batch_size)

    producer = build_producer(bootstrap)

    # Stream dataset directly from Kaggle
    zf = stream_kaggle_zip()

    total_events   = 0
    total_batches  = 0
    delay_per_batch = (args.batch_size / 252 / 6.5 / 3600) / args.speed  # simulated seconds

    try:
        for batch in micro_batch_events(zf, args.batch_size, args.ticker):
            for event in batch:
                key   = event["ticker"].encode()
                value = json.dumps(event, default=str).encode()
                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=delivery_callback,
                )

            producer.poll(0)            # trigger delivery callbacks

            total_events  += len(batch)
            total_batches += 1

            if total_batches % 20 == 0:
                log.info(
                    "Progress: %d batches | %d events produced",
                    total_batches, total_events,
                )

            # Simulate time passing between micro-batches
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
