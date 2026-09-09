"""
Kafka Consumer — US Stock Dataset (validation & micro-batch processing)
=======================================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Consumes canonical JSON events from the Kafka topic, processes them in
configurable micro-batches to simulate real-time ingestion, validates
the schema, and prints a live summary.  This consumer is the handover
validation script that M2 (Spark) will replace with Structured Streaming.

Usage:
    python consumer.py [--batch-size INT] [--timeout FLOAT] [--group GROUP]

Environment variables (or .env file):
    KAFKA_BOOTSTRAP  – Kafka broker(s), default localhost:9092
    KAFKA_TOPIC      – Topic name,       default us-stocks-raw
    KAFKA_GROUP_ID   – Consumer group,   default m1-validation-group
"""

from __future__ import annotations

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
DEFAULT_TOPIC    = "us-stocks-raw"
DEFAULT_BROKER   = "localhost:9092"
DEFAULT_GROUP    = "m1-validation-group"
MICRO_BATCH_SIZE = 100          # messages per processing micro-batch
POLL_TIMEOUT     = 1.0          # seconds to wait for a message

# Required canonical fields that every event must contain
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
    """Outcome of validating a single event."""
    valid:    bool
    event_id: Optional[int]
    ticker:   Optional[str]
    errors:   list[str] = field(default_factory=list)


def validate_event(event: dict[str, Any]) -> ValidationResult:
    """
    Validate a canonical stock event against the agreed schema.

    Rules:
    - All REQUIRED_FIELDS must be present and non-null.
    - NUMERIC_FIELDS must be parseable as float.
    - high >= low  (basic sanity check).
    - date must be a parseable YYYY-MM-DD string.

    Args:
        event: Decoded JSON dictionary from Kafka.

    Returns:
        ValidationResult with valid flag and list of error messages.
    """
    errors: list[str] = []

    event_id = event.get("event_id")
    ticker   = event.get("ticker")

    # 1. Required fields present
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    # 2. Numeric fields are actual numbers
    for col in NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            errors.append(f"Null numeric field: {col}")
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric value for {col}: {val!r}")

    # 3. Date parseable
    date_str = event.get("date")
    if date_str:
        try:
            datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        except ValueError:
            errors.append(f"Unparseable date: {date_str!r}")
    else:
        errors.append("date is null or missing")

    # 4. High >= Low sanity
    try:
        high = float(event.get("high", 0) or 0)
        low  = float(event.get("low",  0) or 0)
        if high < low:
            errors.append(f"high ({high}) < low ({low})")
    except (TypeError, ValueError):
        pass  # already caught above

    return ValidationResult(
        valid=len(errors) == 0,
        event_id=event_id,
        ticker=ticker,
        errors=errors,
    )


# ── Micro-batch processor ─────────────────────────────────────────────────────

@dataclass
class BatchStats:
    """Accumulated statistics for one micro-batch."""
    batch_num:     int
    total_msgs:    int = 0
    valid_msgs:    int = 0
    invalid_msgs:  int = 0
    tickers_seen:  set = field(default_factory=set)
    start_time:    float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def throughput(self) -> float:
        return self.total_msgs / max(self.elapsed, 1e-9)


def process_micro_batch(
    messages: list[Message],
    batch_num: int,
    global_stats: dict,
) -> BatchStats:
    """
    Process a micro-batch of Kafka messages.

    Steps:
        1. Deserialise JSON payload.
        2. Validate against canonical schema.
        3. Accumulate statistics.
        4. Log invalid events for dead-letter handling.

    Args:
        messages:     List of confluent-kafka Message objects.
        batch_num:    Monotonic batch counter (for logging).
        global_stats: Mutable dict updated with cumulative counters.

    Returns:
        BatchStats for this micro-batch.
    """
    stats = BatchStats(batch_num=batch_num)

    for msg in messages:
        stats.total_msgs += 1

        # Deserialise
        try:
            event: dict = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("Batch %d | Cannot decode message: %s", batch_num, exc)
            stats.invalid_msgs += 1
            global_stats["invalid"] += 1
            continue

        # Validate
        result = validate_event(event)
        ticker = result.ticker or "UNKNOWN"
        stats.tickers_seen.add(ticker)

        if result.valid:
            stats.valid_msgs += 1
            global_stats["valid"] += 1
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
    """Print a one-line summary for the completed micro-batch."""
    pct_valid = (stats.valid_msgs / max(stats.total_msgs, 1)) * 100
    log.info(
        "Batch %4d | msgs=%d  valid=%d (%.0f%%)  invalid=%d  "
        "tickers=%d  elapsed=%.2fs  throughput=%.0f msg/s",
        stats.batch_num,
        stats.total_msgs,
        stats.valid_msgs,
        pct_valid,
        stats.invalid_msgs,
        len(stats.tickers_seen),
        stats.elapsed,
        stats.throughput,
    )


def log_global_summary(global_stats: dict) -> None:
    """Print cumulative statistics when the consumer exits."""
    total   = global_stats["total"]
    valid   = global_stats["valid"]
    invalid = global_stats["invalid"]
    pct     = (valid / max(total, 1)) * 100

    log.info("=" * 60)
    log.info("GLOBAL SUMMARY")
    log.info("  Total batches : %d", global_stats["batches"])
    log.info("  Total messages: %d", total)
    log.info("  Valid         : %d  (%.1f%%)", valid, pct)
    log.info("  Invalid       : %d  (%.1f%%)", invalid, 100 - pct)
    log.info("=" * 60)


# ── Kafka consumer ────────────────────────────────────────────────────────────

def build_consumer(
    bootstrap_servers: str,
    group_id: str,
    topic: str,
) -> Consumer:
    """
    Build and return a confluent-kafka Consumer subscribed to topic.

    Args:
        bootstrap_servers: Comma-separated host:port pairs.
        group_id:          Consumer group identifier.
        topic:             Kafka topic to subscribe to.

    Returns:
        Configured, subscribed Consumer instance.
    """
    conf = {
        "bootstrap.servers":        bootstrap_servers,
        "group.id":                 group_id,
        "auto.offset.reset":        "earliest",    # read from the start
        "enable.auto.commit":       False,          # manual commit after batch
        "max.poll.interval.ms":     300_000,
        "session.timeout.ms":       30_000,
        "fetch.min.bytes":          1,
        "fetch.wait.max.ms":        500,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    log.info("Subscribed to topic '%s'  group='%s'", topic, group_id)
    return consumer


# ── Graceful shutdown ─────────────────────────────────────────────────────────

_running = True


def _handle_signal(signum, _frame) -> None:
    global _running
    log.info("Signal %d received — shutting down …", signum)
    _running = False


# ── Main ──────────────────────────────────────────────────────────────────────

import argparse


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="US Stocks Kafka Consumer (validation)")
    p.add_argument("--batch-size", type=int,   default=MICRO_BATCH_SIZE,
                   help="Messages per micro-batch (default 100)")
    p.add_argument("--timeout",    type=float, default=POLL_TIMEOUT,
                   help="Poll timeout in seconds (default 1.0)")
    p.add_argument("--group",      type=str,   default=None,
                   help="Override consumer group ID")
    p.add_argument("--topic",      type=str,   default=None,
                   help="Override Kafka topic name")
    return p.parse_args()


TOPIC_RETRY_ATTEMPTS = 10   # how many times to retry if topic not found yet
TOPIC_RETRY_DELAY    = 5    # seconds between retries


def wait_for_topic(bootstrap: str, topic: str) -> None:
    """
    Block until the topic exists on the broker, retrying on UNKNOWN_TOPIC_OR_PART.
    This handles the race condition where the consumer starts before kafka-init
    has finished creating the topics.

    Args:
        bootstrap: Broker address(es).
        topic:     Topic name to wait for.

    Raises:
        RuntimeError: If the topic never appears after all retries.
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})

    for attempt in range(1, TOPIC_RETRY_ATTEMPTS + 1):
        cluster_meta = admin.list_topics(timeout=10)
        if topic in cluster_meta.topics:
            log.info("Topic '%s' confirmed on broker. ✓", topic)
            return
        log.warning(
            "Topic '%s' not found yet (attempt %d/%d) — retrying in %ds …",
            topic, attempt, TOPIC_RETRY_ATTEMPTS, TOPIC_RETRY_DELAY,
        )
        time.sleep(TOPIC_RETRY_DELAY)

    raise RuntimeError(
        f"Topic '{topic}' still not available after "
        f"{TOPIC_RETRY_ATTEMPTS} attempts. Is kafka-init healthy?"
    )


def main() -> None:
    load_dotenv()
    args = parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BROKER)
    topic     = args.topic or os.getenv("KAFKA_TOPIC",    DEFAULT_TOPIC)
    group     = args.group or os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP)

    log.info("=== US Stocks Kafka Consumer ===")
    log.info("Broker     : %s", bootstrap)
    log.info("Topic      : %s", topic)
    log.info("Group      : %s", group)
    log.info("Batch size : %d messages", args.batch_size)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Wait until the topic exists before subscribing — avoids the
    # UNKNOWN_TOPIC_OR_PART crash when the consumer starts before kafka-init finishes
    wait_for_topic(bootstrap, topic)

    consumer = build_consumer(bootstrap, group, topic)

    global_stats: dict = {"total": 0, "valid": 0, "invalid": 0, "batches": 0}
    batch_num    = 0
    buffer:  list[Message] = []

    try:
        while _running:
            msg = consumer.poll(timeout=args.timeout)

            if msg is None:
                # No new message within timeout — flush partial buffer
                if buffer:
                    batch_num += 1
                    stats = process_micro_batch(buffer, batch_num, global_stats)
                    log_batch_summary(stats)
                    consumer.commit(asynchronous=False)
                    buffer = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug(
                        "End of partition %d @ offset %d",
                        msg.partition(), msg.offset(),
                    )
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Topic disappeared mid-run (e.g. deleted) — wait and retry
                    log.warning("Topic not found mid-run — waiting 5s before retry …")
                    time.sleep(5)
                else:
                    raise KafkaException(msg.error())
                continue

            buffer.append(msg)

            if len(buffer) >= args.batch_size:
                batch_num += 1
                stats = process_micro_batch(buffer, batch_num, global_stats)
                log_batch_summary(stats)
                consumer.commit(asynchronous=False)
                buffer = []

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        sys.exit(1)
    finally:
        # Flush any remaining buffered messages
        if buffer:
            batch_num += 1
            stats = process_micro_batch(buffer, batch_num, global_stats)
            log_batch_summary(stats)
            try:
                consumer.commit(asynchronous=False)
            except Exception:
                pass

        consumer.close()
        log_global_summary(global_stats)
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()