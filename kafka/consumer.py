"""
consumer.py — US Stock Dataset Kafka Consumer (Never-Stop Mode)
===============================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

The consumer runs FOREVER until one of these two conditions is met:
    1. It has received and processed every message in the topic
       (detected by sustained PARTITION_EOF on ALL partitions
        for longer than --idle-timeout seconds), AND
    2. A SIGINT / SIGTERM signal is received.

It will NEVER exit early due to a temporary slow producer or a
brief gap between files. It will keep polling and waiting.

Micro-batch flushing is DUAL-TRIGGERED:
    Size trigger  — flush when buffer reaches --batch-size messages
    Time trigger  — flush every --max-wait-ms milliseconds

Usage:
    python consumer.py [--batch-size INT] [--max-wait-ms INT]
                       [--idle-timeout INT] [--group GROUP] [--live]

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
from confluent_kafka.admin import AdminClient
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

DEFAULT_TOPIC        = "us-stocks-raw"
DEFAULT_BROKER       = "localhost:9092"
DEFAULT_GROUP        = "m1-validation-group"
DEFAULT_BATCH_SIZE   = 100
DEFAULT_MAX_WAIT_MS  = 5000       # flush at least every 5 seconds
DEFAULT_IDLE_TIMEOUT = 120        # seconds of all-partitions EOF before exit
POLL_TIMEOUT         = 1.0        # seconds — longer poll avoids busy-wait

TOPIC_RETRY_ATTEMPTS = 120        # wait up to 10 minutes for topic to appear
TOPIC_RETRY_DELAY    = 5          # seconds between retries

REQUIRED_NUMERIC_FIELDS: frozenset[str] = frozenset({
    "open", "high", "low", "close", "volume",
})
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
    errors:   list[str] = []
    event_id: Optional[int] = event.get("event_id")
    ticker:   Optional[str] = event.get("ticker")

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    for col in REQUIRED_NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            errors.append(f"Null required numeric: {col}")
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric {col}={val!r}")

    for col in NULLABLE_NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric {col}={val!r}")

    date_str = event.get("date")
    if date_str:
        try:
            datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        except ValueError:
            errors.append(f"Unparseable date: {date_str!r}")
    else:
        errors.append("date is null or missing")

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
        pass

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


def log_global_summary(global_stats: dict, elapsed_wall: float) -> None:
    total   = global_stats["total"]
    valid   = global_stats["valid"]
    invalid = global_stats["invalid"]
    pct     = valid / max(total, 1) * 100
    h = int(elapsed_wall) // 3600
    m = (int(elapsed_wall) % 3600) // 60
    s = int(elapsed_wall) % 60

    log.info("=" * 62)
    log.info("  GLOBAL SUMMARY")
    log.info("  Total batches : %d", global_stats["batches"])
    log.info("  Total messages: %d", total)
    log.info("  Valid         : %d  (%.1f%%)", valid, pct)
    log.info("  Invalid       : %d  (%.1f%%)", invalid, 100 - pct)
    log.info("  Wall time     : %02d:%02d:%02d", h, m, s)
    log.info("=" * 62)


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def build_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers":    bootstrap_servers,
        "group.id":             group_id,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        "max.poll.interval.ms": 600_000,   # 10 min — producer can be slow
        "session.timeout.ms":   60_000,    # 60 s
        "fetch.min.bytes":      1,
        "fetch.wait.max.ms":    1000,
    })
    consumer.subscribe([topic])
    log.info("Subscribed to topic '%s'  group='%s'", topic, group_id)
    return consumer


def get_topic_partition_count(bootstrap: str, topic: str) -> int:
    """Return the number of partitions for *topic*."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    meta  = admin.list_topics(topic=topic, timeout=30)
    return len(meta.topics[topic].partitions)


def wait_for_topic(bootstrap: str, topic: str) -> None:
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
# EOF tracker — knows when ALL partitions are at the end
# ---------------------------------------------------------------------------

class _EofTracker:
    """
    Tracks which partitions have reported PARTITION_EOF.
    Only declares "all done" when:
      - every assigned partition has hit EOF, AND
      - that state has been sustained for `idle_timeout` seconds.
    This prevents premature exit when the producer is between files.
    """

    def __init__(self, idle_timeout: int) -> None:
        self.idle_timeout  = idle_timeout
        self._eof_parts:   set[int] = set()
        self._all_eof_since: Optional[float] = None
        self._total_parts  = 0

    def set_total(self, n: int) -> None:
        self._total_parts = n
        log.info("Tracking EOF across %d partition(s).", n)

    def mark_eof(self, partition: int) -> None:
        self._eof_parts.add(partition)
        if len(self._eof_parts) >= self._total_parts and self._all_eof_since is None:
            self._all_eof_since = time.monotonic()
            log.info(
                "All %d partitions at EOF — waiting %d s before declaring done …",
                self._total_parts, self.idle_timeout,
            )

    def mark_message(self, partition: int) -> None:
        """A new message arrived — reset the EOF state for this partition."""
        if partition in self._eof_parts:
            self._eof_parts.discard(partition)
            self._all_eof_since = None

    def is_done(self) -> bool:
        if self._all_eof_since is None:
            return False
        return (time.monotonic() - self._all_eof_since) >= self.idle_timeout


# ---------------------------------------------------------------------------
# Graceful shutdown on signal
# ---------------------------------------------------------------------------

_shutdown_requested = False


def _handle_signal(signum, _frame) -> None:
    global _shutdown_requested
    log.info("Signal %d received — will finish current batch then exit.", signum)
    _shutdown_requested = True


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="US Stocks Kafka Consumer — runs until ALL data is consumed"
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help="Messages per batch — size trigger (default: 100)",
    )
    parser.add_argument(
        "--max-wait-ms", type=int, default=DEFAULT_MAX_WAIT_MS,
        help="Max ms before a time-triggered flush (default: 5000)",
    )
    parser.add_argument(
        "--idle-timeout", type=int, default=DEFAULT_IDLE_TIMEOUT,
        help="Seconds all partitions must stay at EOF before consumer exits "
             "(default: 120). Increase if your producer has long gaps between files.",
    )
    parser.add_argument(
        "--group", type=str, default=None,
        help="Override the consumer group ID",
    )
    parser.add_argument(
        "--topic", type=str, default=None,
        help="Override the source topic",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Print every valid message individually as it arrives",
    )
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
    log.info("  US Stocks Kafka Consumer  [NEVER-STOP MODE]")
    log.info("  Broker       : %s", bootstrap)
    log.info("  Topic        : %s", topic)
    log.info("  Group        : %s", group)
    log.info("  Batch size   : %d  |  Max wait: %d ms", args.batch_size, args.max_wait_ms)
    log.info("  Idle timeout : %d s  (EOF grace period)", args.idle_timeout)
    log.info("=" * 62)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    wait_for_topic(bootstrap, topic)

    # Get partition count so the EOF tracker knows when all are done
    n_partitions = get_topic_partition_count(bootstrap, topic)

    consumer = build_consumer(bootstrap, group, topic)

    global_stats: dict = {"total": 0, "valid": 0, "invalid": 0, "batches": 0}
    batch_num          = 0
    buffer: list[Message] = []
    max_wait_s         = args.max_wait_ms / 1000.0
    batch_start        = time.monotonic()
    start_wall         = time.time()

    eof_tracker = _EofTracker(idle_timeout=args.idle_timeout)
    eof_tracker.set_total(n_partitions)

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

    log.info("Consumer is running — will stop only when ALL data has been consumed.")
    log.info("Send SIGINT (Ctrl+C) to stop early.")

    try:
        while not _shutdown_requested:

            # ── Time-triggered flush ────────────────────────────────────────
            if time.monotonic() - batch_start >= max_wait_s:
                _flush_buffer(trigger="time")

            # ── Check if all partitions have been idle long enough ──────────
            if eof_tracker.is_done():
                log.info(
                    "All partitions have been at EOF for %d s — "
                    "all data consumed. Exiting.",
                    args.idle_timeout,
                )
                break

            # ── Poll ────────────────────────────────────────────────────────
            msg = consumer.poll(timeout=POLL_TIMEOUT)

            if msg is None:
                # No message this poll cycle — normal when producer is slow
                continue

            if msg.error():
                code = msg.error().code()

                if code == KafkaError._PARTITION_EOF:
                    eof_tracker.mark_eof(msg.partition())
                    log.debug(
                        "EOF  partition=%d  offset=%d",
                        msg.partition(), msg.offset(),
                    )
                    continue

                elif code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topic/partition not found — retrying in 5 s …")
                    time.sleep(5)
                    continue

                elif code == KafkaError._ALL_BROKERS_DOWN:
                    log.error("All brokers down — retrying in 10 s …")
                    time.sleep(10)
                    continue

                else:
                    raise KafkaException(msg.error())

            # ── Good message ────────────────────────────────────────────────
            eof_tracker.mark_message(msg.partition())   # reset EOF for this partition
            buffer.append(msg)

            if len(buffer) >= args.batch_size:
                _flush_buffer(trigger="size")

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        sys.exit(1)
    finally:
        # Flush any remaining messages before closing
        if buffer:
            _flush_buffer(trigger="shutdown")
        consumer.close()
        log_global_summary(global_stats, time.time() - start_wall)
        log.info("Consumer closed cleanly.")


if __name__ == "__main__":
    main()