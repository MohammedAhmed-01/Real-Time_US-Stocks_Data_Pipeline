"""
topic_config.py — Kafka topic definitions for the US Stocks pipeline
====================================================================
Single source of truth for topic names, partition counts, replication
factors, retention configs, and the canonical JSON event schema.

The schema reflects the actual columns in the Kaggle dataset:
    footballjoe789/us-stock-dataset → Data/StockHistory/<TICKER>.csv

Original CSV columns:
    Date, Open, High, Low, Close, Volume,
    Dividends, Stock Splits,
    STOCHk_14_3_3, STOCHd_14_3_3

Note: Ticker is NOT a CSV column — it is derived from the filename.

Run directly to print the M1 → M2 handover summary:
    python topic_config.py
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Topic specification
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TopicSpec:
    """Immutable specification for a single Kafka topic."""
    name:               str
    partitions:         int
    replication_factor: int
    description:        str
    configs:            dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Topic catalogue
# ---------------------------------------------------------------------------

TOPICS: dict[str, TopicSpec] = {

    "raw": TopicSpec(
        name               = "us-stocks-raw",
        partitions         = 6,
        replication_factor = 3,
        description        = (
            "Primary ingest topic. "
            "Producer publishes one JSON event per CSV row, keyed by ticker symbol. "
            "M2 (Spark Structured Streaming) reads from this topic."
        ),
        configs = {
            "retention.ms":        "604800000",   # 7 days
            "compression.type":    "lz4",
            "min.insync.replicas": "2",
            "cleanup.policy":      "delete",
        },
    ),

    "dead_letter": TopicSpec(
        name               = "us-stocks-dead-letter",
        partitions         = 3,
        replication_factor = 3,
        description        = (
            "Dead-letter topic for events that fail schema validation. "
            "Retained for 30 days for investigation."
        ),
        configs = {
            "retention.ms":     "2592000000",  # 30 days
            "compression.type": "lz4",
        },
    ),
}


# ---------------------------------------------------------------------------
# Handover constants for M2 (Spark)
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_INTERNAL = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
KAFKA_BOOTSTRAP_EXTERNAL = "localhost:9092,localhost:9093,localhost:9094"

CONSUMER_GROUP_M1_VALIDATION = "m1-validation-group"
CONSUMER_GROUP_M2_SPARK      = "spark-streaming-group"


# ---------------------------------------------------------------------------
# Canonical JSON event schema
# ---------------------------------------------------------------------------
#
# This is the exact shape of every message on us-stocks-raw.
# Use this to define the Spark StructType / DataFrame schema in M2.
#
# Nullability notes:
#   dividends / stock_splits  — 0.0 on non-event days; never null
#   stochk_14_3_3 / stochd_14_3_3 — null for the first ~14 rows per
#       ticker (indicator warm-up), non-null thereafter

CANONICAL_SCHEMA: dict[str, str] = {
    # Identity
    "event_id":       "LongType",
    "ticker":         "StringType",           # e.g. "AAPL" — from filename
    "date":           "DateType",             # YYYY-MM-DD

    # Core OHLCV — always non-null, always > 0 (volume >= 0)
    "open":           "DoubleType",
    "high":           "DoubleType",
    "low":            "DoubleType",
    "close":          "DoubleType",
    "volume":         "DoubleType",

    # Corporate actions — non-null, 0.0 on non-event days
    "dividends":      "DoubleType",
    "stock_splits":   "DoubleType",

    # Technical indicators — nullable during warm-up
    "stochk_14_3_3":  "DoubleType (nullable)",
    "stochd_14_3_3":  "DoubleType (nullable)",

    # Pipeline metadata
    "source_file":    "StringType",           # e.g. "Data/StockHistory/AAPL.csv"
    "produced_at":    "TimestampType",        # ISO-8601 UTC
}


# ---------------------------------------------------------------------------
# CLI: print handover summary
# ---------------------------------------------------------------------------

def _print_topics(topics: dict[str, TopicSpec]) -> None:
    for key, spec in topics.items():
        print(f"  Topic [{key.upper()}]")
        print(f"    Name               : {spec.name}")
        print(f"    Partitions         : {spec.partitions}")
        print(f"    Replication factor : {spec.replication_factor}")
        print(f"    Description        : {spec.description}")
        print(f"    Extra configs      : {spec.configs}")
        print()


def _print_schema(schema: dict[str, str]) -> None:
    notes = {
        "event_id":      "monotonically increasing",
        "ticker":        "derived from CSV filename",
        "date":          "trading date",
        "open":          "must be > 0",
        "high":          "must be > 0, >= low",
        "low":           "must be > 0",
        "close":         "must be > 0",
        "volume":        "must be >= 0",
        "dividends":     "0.0 on non-dividend days",
        "stock_splits":  "0.0 on non-split days",
        "stochk_14_3_3": "null during warm-up (~14 rows)",
        "stochd_14_3_3": "null during warm-up (~14 rows)",
        "source_file":   "Kaggle path of originating CSV",
        "produced_at":   "UTC wall-clock at produce time",
    }
    print(f"  {'Field':<20} {'Type':<30} Notes")
    print("  " + "-" * 58)
    for col, dtype in schema.items():
        print(f"  {col:<20} {dtype:<30} {notes.get(col, '')}")


def main() -> None:
    print()
    print("=" * 62)
    print("  M1 → M2  HANDOVER PACKAGE  (Kafka)")
    print("=" * 62)
    print(f"  Broker (internal Docker network) : {KAFKA_BOOTSTRAP_INTERNAL}")
    print(f"  Broker (external / host machine) : {KAFKA_BOOTSTRAP_EXTERNAL}")
    print()

    _print_topics(TOPICS)

    print("  Consumer groups")
    print(f"    M1 validation : {CONSUMER_GROUP_M1_VALIDATION}")
    print(f"    M2 Spark      : {CONSUMER_GROUP_M2_SPARK}")
    print()

    print("  Canonical JSON event schema")
    _print_schema(CANONICAL_SCHEMA)
    print("=" * 62)
    print()


if __name__ == "__main__":
    main()