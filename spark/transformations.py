"""Validation, cleaning, and enrichment logic for retail event streams."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    concat_ws,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    lower,
    round as spark_round,
    to_date,
    to_timestamp,
    trim,
    weekofyear,
    when,
    year,
    month,
    hour,
)

from config.config import VALID_CATEGORIES, VALID_EVENT_TYPES, VALID_YEARS


def normalize_and_validate(events: DataFrame) -> DataFrame:
    """Return events with cleaned fields, derived columns, and reject reasons."""
    normalized = (
        events.withColumn("event_timestamp_ts", to_timestamp(col("event_timestamp")))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("product_category", trim(col("product_category")))
        .withColumn("customer_name", coalesce(trim(col("customer_name")), lit("Unknown Customer")))
        .withColumn("product_name", coalesce(trim(col("product_name")), lit("Unknown Product")))
        .withColumn("brand", coalesce(trim(col("brand")), lit("Unknown Brand")))
        .withColumn("supplier", coalesce(trim(col("supplier")), lit("Unknown Supplier")))
        .withColumn("payment_method", coalesce(trim(col("payment_method")), lit("not_applicable")))
        .withColumn("discount", coalesce(col("discount"), lit(0.0)))
        .withColumn("price", when(col("price") < 0, spark_abs(col("price"))).otherwise(col("price")))
    )

    with_reasons = (
        normalized.withColumn(
            "reject_reason",
            concat_ws(
                "; ",
                when(col("event_id").isNull(), lit("missing event_id")),
                when(col("event_timestamp_ts").isNull(), lit("bad timestamp")),
                when(~year(col("event_timestamp_ts")).isin(VALID_YEARS), lit("event year outside supported range")),
                when(col("customer_id").isNull(), lit("missing customer_id")),
                when(col("product_id").isNull(), lit("missing product_id")),
                when(col("store_id").isNull(), lit("missing store_id")),
                when(col("event_type").isNull() | (~col("event_type").isin(VALID_EVENT_TYPES)), lit("invalid event_type")),
                when(col("product_category").isNull() | (~col("product_category").isin(VALID_CATEGORIES)), lit("invalid product_category")),
                when(col("customer_age").isNull() | (col("customer_age") < 13) | (col("customer_age") > 100), lit("invalid customer_age")),
                when(col("price").isNull() | (col("price") <= 0), lit("invalid price")),
                when(col("quantity").isNull() | (col("quantity") <= 0), lit("invalid quantity")),
                when((col("discount") < 0) | (col("discount") >= 1), lit("invalid discount")),
                when((col("event_type") == "review") & (~col("rating").between(1, 5)), lit("invalid review rating")),
            ),
        )
    )
    return with_reasons.withColumn("is_valid", col("reject_reason") == "")


def enrich_valid_events(events: DataFrame) -> DataFrame:
    """Create analytics-ready fields for valid events."""
    final_price = spark_round(col("price") * (lit(1.0) - col("discount")), 2)
    gross_revenue = final_price * col("quantity")
    signed_revenue = (
        when(col("event_type") == "purchase", gross_revenue)
        .when(col("event_type") == "return", -gross_revenue)
        .otherwise(lit(0.0))
    )

    return (
        events.filter(col("is_valid"))
        .dropDuplicates(["event_id"])
        .withColumn("event_timestamp", col("event_timestamp_ts"))
        .withColumn("final_price", final_price)
        .withColumn("revenue", spark_round(signed_revenue, 2))
        .withColumn("profit_estimate", spark_round(signed_revenue * lit(0.28), 2))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_month", month(col("event_timestamp")))
        .withColumn("event_year", year(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("week_number", weekofyear(col("event_timestamp")))
        .withColumn("is_weekend", dayofweek(col("event_timestamp")).isin([1, 7]))
        .withColumn("date_key", date_format(col("event_timestamp"), "yyyyMMdd").cast("int"))
        .withColumn("full_date", to_date(col("event_timestamp")))
        .withColumn("quarter", ((month(col("event_timestamp")) - 1) / 3 + 1).cast("int"))
        .withColumn("month_name", date_format(col("event_timestamp"), "MMMM"))
        .withColumn("day_of_month", dayofmonth(col("event_timestamp")))
        .withColumn("day_name", date_format(col("event_timestamp"), "EEEE"))
    )


def invalid_events(events: DataFrame) -> DataFrame:
    """Select records rejected by validation for dead-letter storage."""
    return events.filter(~col("is_valid"))
