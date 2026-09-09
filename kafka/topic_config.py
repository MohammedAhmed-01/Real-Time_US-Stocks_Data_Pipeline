"""
topic_config.py — Kafka topic definitions for the US Stocks pipeline
=====================================================================
Single source-of-truth for topic names, partition counts, replication
factors, and configs.  Import this in both the producer and consumer,
and reference it from Airflow DAGs and Spark jobs.

This module can also be run directly to print the handover summary
that M1 passes to M2:

    python config/topic_config.py
"""

from __future__ import annotations
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TopicSpec:
    """Immutable specification for a single Kafka topic."""
    name:               str
    partitions:         int
    replication_factor: int
    description:        str
    configs:            dict[str, str] = field(default_factory=dict)


# ── Topic catalogue ───────────────────────────────────────────────────────────

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
        configs={
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
        configs={
            "retention.ms":     "2592000000",  # 30 days
            "compression.type": "lz4",
        },
    ),
}

# ── Handover constants for M2 ─────────────────────────────────────────────────

KAFKA_BOOTSTRAP_INTERNAL = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
KAFKA_BOOTSTRAP_EXTERNAL = "localhost:9092,localhost:9093,localhost:9094"

CONSUMER_GROUP_M1_VALIDATION = "m1-validation-group"
CONSUMER_GROUP_M2_SPARK      = "spark-streaming-group"

# Canonical JSON event schema (for M2 Spark schema enforcement)
CANONICAL_SCHEMA: dict[str, str] = {
    "event_id":    "LongType",
    "ticker":      "StringType",
    "date":        "DateType",          # YYYY-MM-DD
    "open":        "DoubleType",
    "high":        "DoubleType",
    "low":         "DoubleType",
    "close":       "DoubleType",
    "adj_close":   "DoubleType",        # nullable
    "volume":      "DoubleType",
    "source_file": "StringType",
    "produced_at": "TimestampType",     # ISO-8601 UTC
}

# ── CLI summary ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("=" * 62)
    print("  M1 → M2  HANDOVER PACKAGE  (Kafka)")
    print("=" * 62)
    print(f"  Broker (internal Docker network) : {KAFKA_BOOTSTRAP_INTERNAL}")
    print(f"  Broker (external / host machine) : {KAFKA_BOOTSTRAP_EXTERNAL}")
    print()

    for key, spec in TOPICS.items():
        print(f"  Topic [{key.upper()}]")
        print(f"    Name               : {spec.name}")
        print(f"    Partitions         : {spec.partitions}")
        print(f"    Replication factor : {spec.replication_factor}")
        print(f"    Description        : {spec.description}")
        print(f"    Extra configs      : {spec.configs}")
        print()

    print("  Consumer groups")
    print(f"    M1 validation : {CONSUMER_GROUP_M1_VALIDATION}")
    print(f"    M2 Spark      : {CONSUMER_GROUP_M2_SPARK}")
    print()

    print("  Canonical JSON schema")
    for col, dtype in CANONICAL_SCHEMA.items():
        nullable = " (nullable)" if col == "adj_close" else ""
        print(f"    {col:<14} {dtype}{nullable}")

    print("=" * 62)
    print()
