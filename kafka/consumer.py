"""
consumer.py — US Stock Dataset Kafka Consumer (Parallel Real-Time Mode)
=======================================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Micro-batch flushing is DUAL-TRIGGERED:
    Size trigger  — flush when buffer reaches --batch-size messages
    Time trigger  — flush every --max-wait-ms milliseconds

Usage:
    python consumer.py [--batch-size INT] [--max-wait-ms INT]
                       [--timeout FLOAT] [--group GROUP] [--live]

Environment variables (or .env):
    KAFKA_BOOTSTRAP   Broker list,     default localhost:9092
    KAFKA_TOPIC       Source topic,    default us-stocks-raw
    KAFKA_GROUP_ID    Consumer group,  default m1-validation-group
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [CONSUMER] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TOPIC    = "us-stocks-raw"
DEFAULT_BROKER   = "localhost:9092"
DEFAULT_GROUP    = "m1-validation-group"
MICRO_BATCH_SIZE = 100
MAX_WAIT_MS      = 2000
POLL_TIMEOUT     = 0.5

TOPIC_RETRY_ATTEMPTS = 60
TOPIC_RETRY_DELAY    = 5

# Fields that must be present and parseable as float (never null)
REQUIRED_NUMERIC_FIELDS: frozenset[str] = frozenset({
    "open", "high", "low", "close", "volume",
})

# Fields that may be null but must be float-parseable when present
NULLABLE_NUMERIC_FIELDS: frozenset[str] = frozenset({
    "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3",
})

REQUIRED_FIELDS: frozenset[str] = frozenset({
    "event_id", "ticker", "date",
    *REQUIRED_NUMERIC_FIELDS,
    *NULLABLE_NUMERIC_FIELDS,
    "source_file", "produced_at",
})


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    valid:    bool
    event_id: Optional[int]
    ticker:   Optional[str]
    errors:   list[str] = field(default_factory=list)


def validate_event(event: dict[str, Any]) -> ValidationResult:
    """Validate one event against the canonical StockHistory schema."""
    errors:   list[str]  = []
    event_id: Optional[int] = event.get("event_id")
    ticker:   Optional[str] = event.get("ticker")

    # 1. Required fields presence
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    # 2. Required numerics — must be non-null and parseable
    for col in REQUIRED_NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            errors.append(f"Null required numeric: {col}")
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric {col}={val!r}")

    # 3. Nullable numerics — must be parseable when present
    for col in NULLABLE_NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric {col}={val!r}")

    # 4. Date format
    date_str = event.get("date")
    if date_str:
        try:
            datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        except ValueError:
            errors.append(f"Unparseable date: {date_str!r}")
    else:
        errors.append("date is null or missing")

    # 5. Price and volume sanity
    try:
        high  = float(event.get("high",  0) or 0)
        low   = float(event.get("low",   0) or 0)
        open_ = float(event.get("open",  0) or 0)
        close = float(event.get("close", 0) or 0)

        if high < low:
            errors.append(f"high ({high}) < low ({low})")

        for name, val in (("open", open_), ("high", high), ("low", low), ("close", close)):
            if val <= 0:
                errors.append(f"{name} must be > 0, got {val}")

    except (TypeError, ValueError):
        pass  # already caught in step 2

    try:
        vol = float(event.get("volume", 0) or 0)
        if vol < 0:
            errors.append(f"volume must be >= 0, got {vol}")
    except (TypeError, ValueError):
        pass

    return ValidationResult(
        valid=len(errors) == 0,
        event_id=event_id,
        ticker=ticker,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Micro-batch statistics
# ---------------------------------------------------------------------------

@dataclass
class BatchStats:
    batch_num:    int
    total_msgs:   int   = 0
    valid_msgs:   int   = 0
    invalid_msgs: int   = 0
    tickers_seen: set   = field(default_factory=set)
    start_time:   float = field(default_factory=time.time)
    trigger:      str   = "?"
    last_ticker:  str   = "—"
    last_date:    str   = "—"
    last_close:   str   = "—"

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def throughput(self) -> float:
        return self.total_msgs / max(self.elapsed, 1e-9)


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_micro_batch(
    messages: list[Message],
    batch_num: int,
    trigger: str,
    global_stats: dict,
    live: bool = False,
) -> BatchStats:
    """Decode, validate, and optionally log every message in the batch."""
    stats = BatchStats(batch_num=batch_num, trigger=trigger)

    for msg in messages:
        stats.total_msgs += 1

        try:
            event: dict = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("batch=%d | Cannot decode message: %s", batch_num, exc)
            stats.invalid_msgs += 1
            global_stats["invalid"] += 1
            continue

        result = validate_event(event)
        ticker = result.ticker or "UNKNOWN"

        stats.tickers_seen.add(ticker)
        stats.last_ticker = ticker
        stats.last_date   = str(event.get("date",  "—"))
        stats.last_close  = str(event.get("close", "—"))

        if result.valid:
            stats.valid_msgs += 1
            global_stats["valid"] += 1

            if live:
                stochk = event.get("stochk_14_3_3")
                stochd = event.get("stochd_14_3_3")
                log.info(
                    "  OK  id=%-8s  ticker=%-6s  date=%s  "
                    "O=%-9s H=%-9s L=%-9s C=%-9s  vol=%-12s  K=%s  D=%s",
                    result.event_id, ticker,
                    event.get("date"),
                    event.get("open"),  event.get("high"),
                    event.get("low"),   event.get("close"),
                    event.get("volume"),
                    f"{stochk:.2f}" if stochk is not None else "n/a",
                    f"{stochd:.2f}" if stochd is not None else "n/a",
                )
        else:
            stats.invalid_msgs += 1
            global_stats["invalid"] += 1
            log.warning(
                "INVALID  event_id=%s  ticker=%s | %s",
                result.event_id, ticker, " | ".join(result.errors),
            )

    global_stats["total"]   += stats.total_msgs
    global_stats["batches"] += 1

    return stats


def log_batch_summary(stats: BatchStats, global_stats: dict) -> None:
    """Log a one-line summary for the completed batch."""
    pct_valid = stats.valid_msgs / max(stats.total_msgs, 1) * 100
    g_total   = global_stats["total"]
    g_valid   = global_stats["valid"]
    g_pct     = g_valid / max(g_total, 1) * 100

    log.info(
        "batch=%4d [%-8s]  msgs=%3d  valid=%3d (%5.1f%%)  invalid=%2d  "
        "tickers=%d  %.2fs  %7.0f msg/s  |  "
        "total=%d  valid=%d (%.1f%%)  invalid=%d  last=%s %s close=%s",
        stats.batch_num, stats.trigger,
        stats.total_msgs, stats.valid_msgs, pct_valid,
        stats.invalid_msgs, len(stats.tickers_seen),
        stats.elapsed, stats.throughput,
        g_total, g_valid, g_pct, global_stats["invalid"],
        stats.last_ticker, stats.last_date, stats.last_close,
    )


def log_global_summary(global_stats: dict) -> None:
    """Log overall statistics when the consumer shuts down."""
    total   = global_stats["total"]
    valid   = global_stats["valid"]
    invalid = global_stats["invalid"]
    pct     = valid / max(total, 1) * 100

    log.info("=" * 62)
    log.info("  GLOBAL SUMMARY")
    log.info("  Total batches : %d", global_stats["batches"])
    log.info("  Total messages: %d", total)
    log.info("  Valid         : %d  (%.1f%%)", valid, pct)
    log.info("  Invalid       : %d  (%.1f%%)", invalid, 100 - pct)
    log.info("=" * 62)


# ---------------------------------------------------------------------------
# Kafka consumer factory
# ---------------------------------------------------------------------------

def build_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    """Return a configured confluent-kafka Consumer subscribed to *topic*."""
    consumer = Consumer({
        "bootstrap.servers":    bootstrap_servers,
        "group.id":             group_id,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms":   30_000,
        "fetch.min.bytes":      1,
        "fetch.wait.max.ms":    500,
    })
    consumer.subscribe([topic])
    log.info("Subscribed to topic '%s'  group='%s'", topic, group_id)
    return consumer


def wait_for_topic(bootstrap: str, topic: str) -> None:
    """Block until *topic* appears in the cluster metadata, or raise on timeout."""
    from confluent_kafka.admin import AdminClient

    deadline = TOPIC_RETRY_ATTEMPTS * TOPIC_RETRY_DELAY
    log.info("Waiting for topic '%s' (up to %d s) …", topic, deadline)

    for attempt in range(1, TOPIC_RETRY_ATTEMPTS + 1):
        admin = AdminClient({"bootstrap.servers": bootstrap})
        try:
            if topic in admin.list_topics(timeout=10).topics:
                log.info("Topic '%s' confirmed on broker.", topic)
                return
        except Exception as exc:
            log.warning("AdminClient error on attempt %d: %s", attempt, exc)

        log.warning(
            "Topic '%s' not found yet (%d/%d) — retrying in %d s …",
            topic, attempt, TOPIC_RETRY_ATTEMPTS, TOPIC_RETRY_DELAY,
        )
        time.sleep(TOPIC_RETRY_DELAY)

    raise RuntimeError(
        f"Topic '{topic}' still not available after {TOPIC_RETRY_ATTEMPTS} attempts."
    )


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_running = True


def _handle_signal(signum, _frame) -> None:
    global _running
    log.info("Signal %d received — shutting down …", signum)
    _running = False


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="US Stocks Kafka Consumer — real-time parallel mode"
    )
    parser.add_argument("--batch-size",  type=int,   default=MICRO_BATCH_SIZE,
                        help="Messages per batch (size trigger, default: 100)")
    parser.add_argument("--max-wait-ms", type=int,   default=MAX_WAIT_MS,
                        help="Max ms before a time-triggered flush (default: 2000)")
    parser.add_argument("--timeout",     type=float, default=POLL_TIMEOUT,
                        help="Kafka poll timeout in seconds (default: 0.5)")
    parser.add_argument("--group",       type=str,   default=None,
                        help="Override the consumer group ID")
    parser.add_argument("--topic",       type=str,   default=None,
                        help="Override the source topic")
    parser.add_argument("--live",        action="store_true",
                        help="Print every valid message as it arrives")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()
    args = _parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC",    DEFAULT_TOPIC)
    group     = args.group or os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP)

    log.info("=" * 62)
    log.info("  US Stocks Kafka Consumer  [PARALLEL REAL-TIME MODE]")
    log.info("  Broker     : %s", bootstrap)
    log.info("  Topic      : %s", topic)
    log.info("  Group      : %s", group)
    log.info("  Batch size : %d  |  Max wait: %d ms", args.batch_size, args.max_wait_ms)
    log.info("=" * 62)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    wait_for_topic(bootstrap, topic)
    consumer = build_consumer(bootstrap, group, topic)

    global_stats: dict = {"total": 0, "valid": 0, "invalid": 0, "batches": 0}
    batch_num          = 0
    buffer: list[Message] = []
    max_wait_s         = args.max_wait_ms / 1000.0
    batch_start        = time.monotonic()

    def _flush_buffer(trigger: str) -> None:
        nonlocal batch_num, buffer, batch_start
        if not buffer:
            batch_start = time.monotonic()
            return
        batch_num += 1
        stats = process_micro_batch(buffer, batch_num, trigger, global_stats, args.live)
        log_batch_summary(stats, global_stats)
        consumer.commit(asynchronous=False)
        buffer      = []
        batch_start = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=args.timeout)

            if time.monotonic() - batch_start >= max_wait_s:
                _flush_buffer(trigger="time")

            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    log.debug(
                        "End of partition %d @ offset %d",
                        msg.partition(), msg.offset(),
                    )
                elif code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topic not found mid-run — waiting 5 s …")
                    time.sleep(5)
                else:
                    raise KafkaException(msg.error())
                continue

            buffer.append(msg)

            if len(buffer) >= args.batch_size:
                _flush_buffer(trigger="size")

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        sys.exit(1)
    finally:
        if buffer:
            _flush_buffer(trigger="shutdown")
        consumer.close()
        log_global_summary(global_stats)
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()