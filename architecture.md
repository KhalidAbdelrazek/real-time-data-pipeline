# Architecture

## Pipeline Flow

```text
RetailEventGenerator
  -> Kafka producer with acknowledgements and retries
  -> Kafka topic: retail_events
  -> PySpark Structured Streaming
  -> schema validation and data quality checks
  -> cleaning, deduplication, and derived columns
  -> foreachBatch SQL Server writer
  -> RetailDW star schema
  -> SSMS analytics
```

## Components

`producer/data_generator.py` creates realistic customers, products, stores, sessions, and events across 2023, 2024, 2025, and 2026. It intentionally injects bad data including null IDs, missing categories, invalid categories, duplicate events, negative prices, negative quantities, and malformed timestamps.

`producer/producer.py` continuously publishes generated events to Kafka. It uses `acks="all"`, retries, controlled in-flight requests, logging, and graceful shutdown.

`spark/schema.py` defines the raw event schema used by `from_json`.

`spark/transformations.py` normalizes text, parses timestamps, repairs safe sign errors such as negative prices, rejects unsafe records, removes duplicate event IDs, and creates analytics columns such as `final_price`, `revenue`, `profit_estimate`, `event_date`, `event_month`, `event_year`, `event_hour`, `week_number`, and `is_weekend`.

`spark/spark_stream.py` reads Kafka with Structured Streaming and uses checkpointing for recovery. Each micro-batch is sent to the SQL Server warehouse writer.

`warehouse/sql_server_writer.py` loads dimensions first with SQL Server `MERGE` statements, appends facts idempotently by `event_id`, retries failed batches, wraps each micro-batch in a transaction, and writes rejected records to `dead_letter_events`.

`warehouse/create_database.sql`, `warehouse/create_dimensions.sql`, and `warehouse/create_fact_table.sql` create the `RetailDW` database, star-schema tables, primary keys, foreign keys, constraints, and indexes.

## Star Schema

The warehouse separates descriptive attributes into dimensions and measurable events into a fact table.

`dim_customer` stores customer demographics and location.

`dim_product` stores product name, category, brand, and supplier.

`dim_store` stores store location details.

`dim_date` stores full calendar attributes keyed by `yyyyMMdd`.

`dim_channel` stores marketing channel values.

`fact_sales` stores every valid event. Purchases add positive revenue, returns subtract revenue, reviews provide ratings, and checkout/view/cart events support conversion metrics.

## Reliability

Kafka handles producer retries and acknowledgements. Spark checkpointing stores streaming progress. SQL Server writes are transaction-protected per micro-batch. Duplicate facts are ignored using unique `event_id`. Rejected records are kept in `dead_letter_events` for audit and debugging.
