"""
Central configuration for the Real-Time Retail Analytics Platform.
Linux / DuckDB edition – all SQL Server settings removed.

Values can be overridden via environment variables so no source files
need to be edited between environments.
"""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC",             "retail_events")
KAFKA_DEAD_LETTER_TOPIC  = os.getenv("KAFKA_DEAD_LETTER_TOPIC", "retail_events_dead_letter")

# ── DuckDB ────────────────────────────────────────────────────────────────────
_default_duckdb = str(PROJECT_ROOT / "warehouse" / "RetailDW.duckdb")
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", _default_duckdb))

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR",
    str(PROJECT_ROOT / "checkpoints" / "retail_events"),
)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR           = PROJECT_ROOT / "logs"
PRODUCER_LOG_FILE = LOG_DIR / "producer.log"
SPARK_LOG_FILE    = LOG_DIR / "spark_stream.log"

# ── Producer tuning ───────────────────────────────────────────────────────────
DEFAULT_EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "20"))
DEFAULT_BAD_RECORD_RATE   = float(os.getenv("BAD_RECORD_RATE",   "0.08"))
DEFAULT_DUPLICATE_RATE    = float(os.getenv("DUPLICATE_RATE",    "0.02"))

# ── Domain constants ──────────────────────────────────────────────────────────
VALID_YEARS = [2023, 2024, 2025, 2026]

VALID_EVENT_TYPES = [
    "product_view", "add_to_cart", "checkout",
    "purchase", "return", "review",
]

VALID_CATEGORIES = [
    "Electronics", "Fashion", "Home", "Beauty",
    "Sports", "Books", "Toys", "Grocery",
]
