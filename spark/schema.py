"""Spark schemas for raw retail events and dead-letter records."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


raw_event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_age", IntegerType(), True),
        StructField("customer_gender", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("supplier", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("store_country", StringType(), True),
        StructField("store_city", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("payment_method", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("marketing_channel", StringType(), True),
    ]
)

