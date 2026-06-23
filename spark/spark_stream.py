"""
Spark Structured Streaming job – Linux / DuckDB edition.
Kafka → schema validation → cleaning/transformation → DuckDB star schema.

Run from the project root:

    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        spark/spark_stream.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    LOG_DIR,
    SPARK_CHECKPOINT_DIR,
    SPARK_LOG_FILE,
)
from spark.schema import raw_event_schema  # noqa: E402
from spark.transformations import (  # noqa: E402
    enrich_valid_events,
    invalid_events,
    normalize_and_validate,
)
from warehouse.duckdb_writer import write_microbatch_to_duckdb  # noqa: E402


def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(SPARK_LOG_FILE, encoding="utf-8"),
        ],
    )
    return logging.getLogger("retail_spark_stream")


logger = configure_logging()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RetailAnalyticsStructuredStreaming-DuckDB")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("parsed", from_json(col("raw_json"), raw_event_schema))
        .select("kafka_key", "raw_json", "kafka_timestamp", "parsed.*")
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    def foreach_batch(batch_df, batch_id: int) -> None:
        normalized    = normalize_and_validate(batch_df)
        valid_batch   = enrich_valid_events(normalized)
        invalid_batch = invalid_events(normalized)
        write_microbatch_to_duckdb(valid_batch, invalid_batch, batch_id)

    query = (
        parsed.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming from Kafka topic '%s'. Press Ctrl+C to stop.", KAFKA_TOPIC)
    query.awaitTermination()


if __name__ == "__main__":
    main()
