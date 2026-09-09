"""
Kafka Consumer — US Stock Dataset  [PARALLEL REAL-TIME MODE]
============================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

This consumer is designed to run IN PARALLEL with the producer.
It processes messages the moment they arrive — it does NOT wait
for the producer to finish.

Micro-batch flushing is DUAL-TRIGGERED:
  • Size trigger  — flush when buffer reaches --batch-size messages
  • Time trigger  — flush every --max-wait-ms milliseconds even if
                    the buffer is not full (keeps latency low during
                    periods of slow ingest)

This dual strategy gives you true near-real-time processing:
  producer writes row 1 → Kafka → consumer sees it in < max-wait-ms

Architecture:
  Kafka topic ─▶ poll() ─▶ buffer ─▶ [size OR time trigger] ─▶ validate ─▶ stats

Usage:
    python consumer.py [--batch-size INT] [--max-wait-ms INT]
                       [--timeout FLOAT] [--group GROUP] [--live]

    --live        print every individual message to stdout (verbose)

Environment variables (or .env file):
    KAFKA_BOOTSTRAP  – Kafka broker(s), default localhost:9092
    KAFKA_TOPIC      – Topic name,       default us-stocks-raw
    KAFKA_GROUP_ID   – Consumer group,   default m1-validation-group
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

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [CONSUMER] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DEFAULT_TOPIC       = "us-stocks-raw"
DEFAULT_BROKER      = "localhost:9092"
DEFAULT_GROUP       = "m1-validation-group"
MICRO_BATCH_SIZE    = 100       # flush batch when this many messages buffered
MAX_WAIT_MS         = 2000      # flush batch after this many ms even if not full
POLL_TIMEOUT        = 0.5       # seconds per poll() call (kept short for liveness)

TOPIC_RETRY_ATTEMPTS = 60
TOPIC_RETRY_DELAY    = 5

REQUIRED_FIELDS: frozenset[str] = frozenset({
    "ticker", "date", "open", "high", "low", "close", "volume",
    "produced_at", "event_id",
})
NUMERIC_FIELDS: frozenset[str] = frozenset({
    "open", "high", "low", "close", "volume",
})


# ── Schema validation ─────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    valid:    bool
    event_id: Optional[int]
    ticker:   Optional[str]
    errors:   list[str] = field(default_factory=list)


def validate_event(event: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []
    event_id = event.get("event_id")
    ticker   = event.get("ticker")

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    for col in NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            errors.append(f"Null numeric field: {col}")
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric value for {col}: {val!r}")

    date_str = event.get("date")
    if date_str:
        try:
            datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        except ValueError:
            errors.append(f"Unparseable date: {date_str!r}")
    else:
        errors.append("date is null or missing")

    try:
        high = float(event.get("high", 0) or 0)
        low  = float(event.get("low",  0) or 0)
        if high < low:
            errors.append(f"high ({high}) < low ({low})")
    except (TypeError, ValueError):
        pass

    return ValidationResult(
        valid=len(errors) == 0,
        event_id=event_id,
        ticker=ticker,
        errors=errors,
    )


# ── Micro-batch stats ─────────────────────────────────────────────────────────

@dataclass
class BatchStats:
    batch_num:    int
    total_msgs:   int   = 0
    valid_msgs:   int   = 0
    invalid_msgs: int   = 0
    tickers_seen: set   = field(default_factory=set)
    start_time:   float = field(default_factory=time.time)
    trigger:      str   = "?"    # "size" or "time"

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def throughput(self) -> float:
        return self.total_msgs / max(self.elapsed, 1e-9)


def process_micro_batch(
    messages: list[Message],
    batch_num: int,
    trigger: str,
    global_stats: dict,
    live: bool = False,
) -> BatchStats:
    """Deserialise, validate, and tally a micro-batch of Kafka messages."""
    stats = BatchStats(batch_num=batch_num, trigger=trigger)

    for msg in messages:
        stats.total_msgs += 1

        try:
            event: dict = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("Batch %d | Cannot decode message: %s", batch_num, exc)
            stats.invalid_msgs += 1
            global_stats["invalid"] += 1
            continue

        result = validate_event(event)
        ticker = result.ticker or "UNKNOWN"
        stats.tickers_seen.add(ticker)

        if result.valid:
            stats.valid_msgs += 1
            global_stats["valid"] += 1

            # --live mode: print each valid message as it arrives
            if live:
                print(
                    f"  ✅ event_id={result.event_id:<8} "
                    f"ticker={ticker:<6} "
                    f"date={event.get('date')}  "
                    f"close={event.get('close')}  "
                    f"volume={event.get('volume')}"
                )
        else:
            stats.invalid_msgs += 1
            global_stats["invalid"] += 1
            log.warning(
                "INVALID event_id=%s ticker=%s | %s",
                result.event_id, ticker, " | ".join(result.errors),
            )

    global_stats["total"]   += stats.total_msgs
    global_stats["batches"] += 1

    return stats


def log_batch_summary(stats: BatchStats) -> None:
    pct_valid = (stats.valid_msgs / max(stats.total_msgs, 1)) * 100
    log.info(
        "Batch %4d [%s-triggered] | msgs=%d  valid=%d (%.0f%%)  "
        "invalid=%d  tickers=%d  %.2fs  %.0f msg/s",
        stats.batch_num,
        stats.trigger,
        stats.total_msgs,
        stats.valid_msgs,
        pct_valid,
        stats.invalid_msgs,
        len(stats.tickers_seen),
        stats.elapsed,
        stats.throughput,
    )


def log_global_summary(global_stats: dict) -> None:
    total   = global_stats["total"]
    valid   = global_stats["valid"]
    invalid = global_stats["invalid"]
    pct     = (valid / max(total, 1)) * 100

    log.info("=" * 62)
    log.info("  GLOBAL SUMMARY")
    log.info("  Total batches : %d", global_stats["batches"])
    log.info("  Total messages: %d", total)
    log.info("  Valid         : %d  (%.1f%%)", valid, pct)
    log.info("  Invalid       : %d  (%.1f%%)", invalid, 100 - pct)
    log.info("=" * 62)


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def build_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    conf = {
        "bootstrap.servers":    bootstrap_servers,
        "group.id":             group_id,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms":   30_000,
        "fetch.min.bytes":      1,
        "fetch.wait.max.ms":    500,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    log.info("Subscribed to topic '%s'  group='%s'", topic, group_id)
    return consumer


def wait_for_topic(bootstrap: str, topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    log.info(
        "Waiting for topic '%s' to appear (up to %d s) …",
        topic, TOPIC_RETRY_ATTEMPTS * TOPIC_RETRY_DELAY,
    )
    for attempt in range(1, TOPIC_RETRY_ATTEMPTS + 1):
        admin = AdminClient({"bootstrap.servers": bootstrap})
        try:
            cluster_meta = admin.list_topics(timeout=10)
            if topic in cluster_meta.topics:
                log.info("Topic '%s' confirmed on broker. ✓", topic)
                return
        except Exception as exc:
            log.warning("AdminClient error on attempt %d: %s", attempt, exc)

        log.warning(
            "Topic '%s' not found yet (attempt %d/%d) — retrying in %ds …",
            topic, attempt, TOPIC_RETRY_ATTEMPTS, TOPIC_RETRY_DELAY,
        )
        time.sleep(TOPIC_RETRY_DELAY)

    raise RuntimeError(
        f"Topic '{topic}' still not available after {TOPIC_RETRY_ATTEMPTS} attempts."
    )


# ── Graceful shutdown ─────────────────────────────────────────────────────────

_running = True


def _handle_signal(signum, _frame) -> None:
    global _running
    log.info("Signal %d received — shutting down …", signum)
    _running = False


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="US Stocks Kafka Consumer — real-time parallel mode"
    )
    p.add_argument("--batch-size",  type=int,   default=MICRO_BATCH_SIZE,
                   help="Flush batch when this many messages buffered")
    p.add_argument("--max-wait-ms", type=int,   default=MAX_WAIT_MS,
                   help="Flush batch after this many ms even if buffer not full")
    p.add_argument("--timeout",     type=float, default=POLL_TIMEOUT,
                   help="Kafka poll timeout in seconds")
    p.add_argument("--group",       type=str,   default=None)
    p.add_argument("--topic",       type=str,   default=None)
    p.add_argument("--live",        action="store_true",
                   help="Print every valid message as it arrives")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()
    args = parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC",    DEFAULT_TOPIC)
    group     = args.group or os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP)

    log.info("=" * 62)
    log.info("  US Stocks Kafka Consumer  [PARALLEL REAL-TIME MODE]")
    log.info("=" * 62)
    log.info("  Broker       : %s", bootstrap)
    log.info("  Topic        : %s", topic)
    log.info("  Group        : %s", group)
    log.info("  Batch size   : %d messages (size trigger)", args.batch_size)
    log.info("  Max wait     : %d ms       (time trigger)", args.max_wait_ms)
    log.info("  Live mode    : %s", "ON — printing every message" if args.live else "OFF")
    log.info("  Design       : flushes on SIZE or TIME — whichever comes first")
    log.info("=" * 62)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    wait_for_topic(bootstrap, topic)
    consumer = build_consumer(bootstrap, group, topic)

    global_stats: dict = {"total": 0, "valid": 0, "invalid": 0, "batches": 0}
    batch_num   = 0
    buffer:  list[Message] = []
    max_wait_s  = args.max_wait_ms / 1000.0
    batch_start = time.monotonic()   # clock for the time trigger

    def _flush_buffer(trigger: str) -> None:
        nonlocal batch_num, buffer, batch_start
        if not buffer:
            batch_start = time.monotonic()
            return
        batch_num += 1
        stats = process_micro_batch(buffer, batch_num, trigger, global_stats, args.live)
        log_batch_summary(stats)
        consumer.commit(asynchronous=False)
        buffer     = []
        batch_start = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=args.timeout)

            # ── Time trigger: flush regardless of buffer fullness ──────────
            elapsed_since_flush = time.monotonic() - batch_start
            if elapsed_since_flush >= max_wait_s:
                _flush_buffer(trigger="time")

            if msg is None:
                # No new message — time trigger already handled above
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    log.debug("End of partition %d @ offset %d",
                              msg.partition(), msg.offset())
                elif code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topic not found mid-run — waiting 5 s …")
                    time.sleep(5)
                else:
                    raise KafkaException(msg.error())
                continue

            buffer.append(msg)

            # ── Size trigger: flush when buffer is full ───────────────────
            if len(buffer) >= args.batch_size:
                _flush_buffer(trigger="size")

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        sys.exit(1)
    finally:
        # Drain any remaining buffered messages
        if buffer:
            _flush_buffer(trigger="shutdown")

        consumer.close()
        log_global_summary(global_stats)
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()