"""
Kafka Consumer — US Stock Dataset  [PARALLEL REAL-TIME MODE]
============================================================
M1 · Data & Kafka  |  Real-Time US Stocks Data Pipeline

Micro-batch flushing is DUAL-TRIGGERED:
  • Size trigger  — flush when buffer reaches --batch-size messages
  • Time trigger  — flush every --max-wait-ms milliseconds

Usage:
    python consumer.py [--batch-size INT] [--max-wait-ms INT]
                       [--timeout FLOAT] [--group GROUP] [--live]

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
DEFAULT_TOPIC        = "us-stocks-raw"
DEFAULT_BROKER       = "localhost:9092"
DEFAULT_GROUP        = "m1-validation-group"
MICRO_BATCH_SIZE     = 100
MAX_WAIT_MS          = 2000
POLL_TIMEOUT         = 0.5

TOPIC_RETRY_ATTEMPTS = 60
TOPIC_RETRY_DELAY    = 5

# Fields that MUST be present in every event (null values are caught separately).
# Matches the exact keys produced by producer.row_to_event().
REQUIRED_FIELDS: frozenset[str] = frozenset({
    "event_id", "ticker", "date",
    "open", "high", "low", "close", "volume",
    "dividends", "stock_splits",
    "stochk_14_3_3", "stochd_14_3_3",
    "source_file", "produced_at",
})

# Fields whose value must be a valid float (NaN/None → validation error for core OHLCV).
# dividends and stock_splits are allowed to be 0.0 on non-event days → not null-checked.
# stochk/stochd can be null for the first few rows before the indicator warms up → nullable.
NUMERIC_FIELDS: frozenset[str] = frozenset({
    "open", "high", "low", "close", "volume",
})

# Nullable numeric fields — present in schema but allowed to be None/null.
NULLABLE_NUMERIC_FIELDS: frozenset[str] = frozenset({
    "dividends", "stock_splits", "stochk_14_3_3", "stochd_14_3_3",
})


# ── Live Dashboard ────────────────────────────────────────────────────────────

class ConsumerDashboard:
    """
    Rewrites a fixed block of terminal lines so the screen doesn't scroll.
    Shows consumed/valid/invalid counts, throughput, and last-seen ticker.
    """

    BAR_WIDTH = 30

    def __init__(self) -> None:
        self.total_consumed = 0
        self.total_valid    = 0
        self.total_invalid  = 0
        self.total_batches  = 0
        self.last_ticker    = "—"
        self.last_date      = "—"
        self.last_close     = "—"
        self.start_time     = time.time()
        self._lines         = 0

    def update(
        self,
        consumed: int,
        valid: int,
        invalid: int,
        last_ticker: str = "—",
        last_date: str   = "—",
        last_close: str  = "—",
    ) -> None:
        self.total_consumed += consumed
        self.total_valid    += valid
        self.total_invalid  += invalid
        self.total_batches  += 1
        self.last_ticker = last_ticker
        self.last_date   = last_date
        self.last_close  = last_close

    def _bar(self, frac: float, ok: bool = True) -> str:
        filled = int(self.BAR_WIDTH * frac)
        ch = "█" if ok else "▓"
        return ch * filled + "░" * (self.BAR_WIDTH - filled)

    def render(self) -> None:
        elapsed    = max(time.time() - self.start_time, 1e-9)
        msg_s      = self.total_consumed / elapsed
        valid_pct  = self.total_valid   / max(self.total_consumed, 1)
        bad_pct    = self.total_invalid / max(self.total_consumed, 1)

        lines = [
            "",
            "  ╔══════════════════════════════════════════════════════════╗",
            "  ║          📥  US STOCKS KAFKA CONSUMER  📥               ║",
            "  ╠══════════════════════════════════════════════════════════╣",
            f"  ║  Consumed : {self.total_consumed:>12,}  │  Batches: {self.total_batches:<6}             ║",
            f"  ║  Valid    : {self.total_valid:>12,}  [{self._bar(valid_pct, ok=True)}] {valid_pct*100:5.1f}%  ║",
            f"  ║  Invalid  : {self.total_invalid:>12,}  [{self._bar(bad_pct,  ok=False)}] {bad_pct*100:5.1f}%  ║",
            f"  ║  Speed    : {msg_s:>10,.0f} msg/s  │  Elapsed: {int(elapsed//60):02d}:{int(elapsed%60):02d}              ║",
            f"  ║  Last     : ticker={self.last_ticker:<6}  date={self.last_date}  close={str(self.last_close):<10}  ║",
            "  ╚══════════════════════════════════════════════════════════╝",
            "",
        ]

        if self._lines:
            sys.stdout.write(f"\033[{self._lines}A")

        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()
        self._lines = len(lines)


# ── Schema validation ─────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    valid:    bool
    event_id: Optional[int]
    ticker:   Optional[str]
    errors:   list[str] = field(default_factory=list)


def validate_event(event: dict[str, Any]) -> ValidationResult:
    """
    Validate one event against the canonical StockHistory schema.

    Rules
    ─────
    1. All REQUIRED_FIELDS keys must be present.
    2. Core OHLCV fields (NUMERIC_FIELDS) must be non-null valid floats.
    3. Nullable indicator fields (NULLABLE_NUMERIC_FIELDS) may be null but,
       when present, must be valid floats (not strings like "nan").
    4. date must parse as YYYY-MM-DD.
    5. high >= low (sanity check).
    6. open, high, low, close must be > 0.
    7. volume must be >= 0.
    """
    errors: list[str] = []
    event_id = event.get("event_id")
    ticker   = event.get("ticker")

    # ── 1. Required keys ──────────────────────────────────────────────────
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    # ── 2. Core OHLCV — must be non-null valid floats ─────────────────────
    for col in NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            errors.append(f"Null required numeric: {col}")
            continue
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric {col}={val!r}")

    # ── 3. Nullable numerics — when present must be valid floats ──────────
    for col in NULLABLE_NUMERIC_FIELDS:
        val = event.get(col)
        if val is None:
            continue   # null is fine for indicator warm-up rows
        try:
            float(val)
        except (TypeError, ValueError):
            errors.append(f"Non-numeric {col}={val!r}")

    # ── 4. Date format ────────────────────────────────────────────────────
    date_str = event.get("date")
    if date_str:
        try:
            datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        except ValueError:
            errors.append(f"Unparseable date: {date_str!r}")
    else:
        errors.append("date is null or missing")

    # ── 5 & 6. Price sanity ───────────────────────────────────────────────
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

    # ── 7. Volume ─────────────────────────────────────────────────────────
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


# ── Micro-batch stats ─────────────────────────────────────────────────────────

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
            log.warning("Batch %d | Cannot decode message: %s", batch_num, exc)
            stats.invalid_msgs += 1
            global_stats["invalid"] += 1
            continue

        result = validate_event(event)
        ticker = result.ticker or "UNKNOWN"
        stats.tickers_seen.add(ticker)
        stats.last_ticker = ticker
        stats.last_date   = str(event.get("date", "—"))
        stats.last_close  = str(event.get("close", "—"))

        if result.valid:
            stats.valid_msgs += 1
            global_stats["valid"] += 1

            if live:
                stochk = event.get("stochk_14_3_3")
                stochd = event.get("stochd_14_3_3")
                print(
                    f"  ✅ [{result.event_id:<8}] "
                    f"{ticker:<6} "
                    f"{event.get('date')}  "
                    f"O={event.get('open'):<9}  "
                    f"H={event.get('high'):<9}  "
                    f"L={event.get('low'):<9}  "
                    f"C={event.get('close'):<9}  "
                    f"Vol={event.get('volume'):<12}  "
                    f"Div={event.get('dividends'):<6}  "
                    f"Split={event.get('stock_splits'):<5}  "
                    f"K={f'{stochk:.2f}' if stochk is not None else 'n/a':<7}  "
                    f"D={f'{stochd:.2f}' if stochd is not None else 'n/a'}"
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
        "Batch %4d [%s] | msgs=%d  valid=%d (%.0f%%)  invalid=%d  "
        "tickers=%d  %.2fs  %.0f msg/s",
        stats.batch_num, stats.trigger,
        stats.total_msgs, stats.valid_msgs, pct_valid,
        stats.invalid_msgs, len(stats.tickers_seen),
        stats.elapsed, stats.throughput,
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
            "Topic '%s' not found yet (%d/%d) — retrying in %ds …",
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
    p.add_argument("--batch-size",  type=int,   default=MICRO_BATCH_SIZE)
    p.add_argument("--max-wait-ms", type=int,   default=MAX_WAIT_MS)
    p.add_argument("--timeout",     type=float, default=POLL_TIMEOUT)
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
    log.info("  Broker: %s  |  Topic: %s  |  Group: %s", bootstrap, topic, group)
    log.info("  Batch size: %d  |  Max wait: %d ms", args.batch_size, args.max_wait_ms)
    log.info("=" * 62)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    wait_for_topic(bootstrap, topic)
    consumer = build_consumer(bootstrap, group, topic)

    dash        = ConsumerDashboard()
    global_stats: dict = {"total": 0, "valid": 0, "invalid": 0, "batches": 0}
    batch_num   = 0
    buffer:  list[Message] = []
    max_wait_s  = args.max_wait_ms / 1000.0
    batch_start = time.monotonic()

    # Initial render
    dash.render()

    def _flush_buffer(trigger: str) -> None:
        nonlocal batch_num, buffer, batch_start
        if not buffer:
            batch_start = time.monotonic()
            return
        batch_num += 1
        stats = process_micro_batch(buffer, batch_num, trigger, global_stats, args.live)
        log_batch_summary(stats)
        consumer.commit(asynchronous=False)

        # Update and re-render the dashboard
        dash.update(
            consumed    = stats.total_msgs,
            valid       = stats.valid_msgs,
            invalid     = stats.invalid_msgs,
            last_ticker = stats.last_ticker,
            last_date   = stats.last_date,
            last_close  = stats.last_close,
        )
        dash.render()

        buffer      = []
        batch_start = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=args.timeout)

            elapsed_since_flush = time.monotonic() - batch_start
            if elapsed_since_flush >= max_wait_s:
                _flush_buffer(trigger="time")

            if msg is None:
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