"""
SQL Server warehouse writer used by Spark foreachBatch.

Each Spark micro-batch is written in one SQL Server transaction. Dimensions are
loaded first with SCD Type 1 MERGE statements, then fact rows are appended
idempotently by event_id. Rejected rows are stored in dead_letter_events.
"""

from __future__ import annotations

import logging
import math
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyodbc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.config import SQL_SERVER_ODBC_CONNECTION_STRING  # noqa: E402


logger = logging.getLogger("sql_server_writer")

MAX_RETRIES = 3
RETRY_SECONDS = 5


def write_microbatch_to_sql_server(valid_df, invalid_df, batch_id: int) -> None:
    """Write one Spark micro-batch to SQL Server with retries."""
    valid_rows = _clean_records(valid_df.toPandas().to_dict("records"))
    invalid_rows = _clean_records(invalid_df.toPandas().to_dict("records"))

    if not valid_rows and not invalid_rows:
        logger.info("Batch %s: no rows to write.", batch_id)
        return

    for attempt in range(1, MAX_RETRIES + 1):
        conn = None
        try:
            conn = pyodbc.connect(SQL_SERVER_ODBC_CONNECTION_STRING, autocommit=False)
            conn.timeout = 60
            with conn.cursor() as cur:
                cur.fast_executemany = True
                if invalid_rows:
                    _insert_dead_letters(cur, invalid_rows, batch_id)
                if valid_rows:
                    _write_valid_rows(cur, valid_rows)
            conn.commit()
            logger.info(
                "Batch %s committed to SQL Server: %,d facts and %,d dead-letter rows.",
                batch_id,
                len(valid_rows),
                len(invalid_rows),
            )
            return
        except Exception:
            if conn is not None:
                conn.rollback()
            logger.exception("Batch %s attempt %s/%s failed.", batch_id, attempt, MAX_RETRIES)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_SECONDS * attempt)
        finally:
            if conn is not None:
                conn.close()


def _write_valid_rows(cur, rows: list[dict[str, Any]]) -> None:
    customer_map = _merge_customers(cur, rows)
    product_map = _merge_products(cur, rows)
    store_map = _merge_stores(cur, rows)
    channel_map = _merge_channels(cur, rows)
    date_map = _merge_dates(cur, rows)
    _insert_facts(cur, rows, customer_map, product_map, store_map, channel_map, date_map)


def _merge_customers(cur, rows: list[dict[str, Any]]) -> dict[str, int]:
    values = _dedupe_by_key(
        rows,
        "customer_id",
        lambda r: (
            r["customer_id"],
            r["customer_name"],
            r["customer_age"],
            r["customer_gender"],
            r["customer_country"],
            r["customer_city"],
        ),
    )
    cur.execute(
        """
        CREATE TABLE #customer_stage (
            customer_id NVARCHAR(30) NOT NULL PRIMARY KEY,
            customer_name NVARCHAR(150) NOT NULL,
            customer_age INT NOT NULL,
            customer_gender NVARCHAR(30) NULL,
            customer_country NVARCHAR(80) NULL,
            customer_city NVARCHAR(80) NULL
        );
        """
    )
    cur.executemany("INSERT INTO #customer_stage VALUES (?, ?, ?, ?, ?, ?);", values)
    cur.execute(
        """
        MERGE dbo.dim_customer AS target
        USING #customer_stage AS source
            ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
            customer_name = source.customer_name,
            customer_age = source.customer_age,
            customer_gender = source.customer_gender,
            customer_country = source.customer_country,
            customer_city = source.customer_city,
            updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            customer_id, customer_name, customer_age, customer_gender,
            customer_country, customer_city
        )
        VALUES (
            source.customer_id, source.customer_name, source.customer_age,
            source.customer_gender, source.customer_country, source.customer_city
        );
        """
    )
    cur.execute(
        """
        SELECT d.customer_id, d.customer_key
        FROM dbo.dim_customer d
        JOIN #customer_stage s ON s.customer_id = d.customer_id;
        """
    )
    return dict(cur.fetchall())


def _merge_products(cur, rows: list[dict[str, Any]]) -> dict[str, int]:
    values = _dedupe_by_key(
        rows,
        "product_id",
        lambda r: (
            r["product_id"],
            r["product_name"],
            r["product_category"],
            r["brand"],
            r["supplier"],
        ),
    )
    cur.execute(
        """
        CREATE TABLE #product_stage (
            product_id NVARCHAR(30) NOT NULL PRIMARY KEY,
            product_name NVARCHAR(200) NOT NULL,
            product_category NVARCHAR(80) NOT NULL,
            brand NVARCHAR(100) NULL,
            supplier NVARCHAR(150) NULL
        );
        """
    )
    cur.executemany("INSERT INTO #product_stage VALUES (?, ?, ?, ?, ?);", values)
    cur.execute(
        """
        MERGE dbo.dim_product AS target
        USING #product_stage AS source
            ON target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET
            product_name = source.product_name,
            product_category = source.product_category,
            brand = source.brand,
            supplier = source.supplier,
            updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            product_id, product_name, product_category, brand, supplier
        )
        VALUES (
            source.product_id, source.product_name, source.product_category,
            source.brand, source.supplier
        );
        """
    )
    cur.execute(
        """
        SELECT d.product_id, d.product_key
        FROM dbo.dim_product d
        JOIN #product_stage s ON s.product_id = d.product_id;
        """
    )
    return dict(cur.fetchall())


def _merge_stores(cur, rows: list[dict[str, Any]]) -> dict[str, int]:
    values = _dedupe_by_key(
        rows,
        "store_id",
        lambda r: (
            r["store_id"],
            r["store_name"],
            r["store_country"],
            r["store_city"],
        ),
    )
    cur.execute(
        """
        CREATE TABLE #store_stage (
            store_id NVARCHAR(30) NOT NULL PRIMARY KEY,
            store_name NVARCHAR(150) NOT NULL,
            store_country NVARCHAR(80) NOT NULL,
            store_city NVARCHAR(80) NOT NULL
        );
        """
    )
    cur.executemany("INSERT INTO #store_stage VALUES (?, ?, ?, ?);", values)
    cur.execute(
        """
        MERGE dbo.dim_store AS target
        USING #store_stage AS source
            ON target.store_id = source.store_id
        WHEN MATCHED THEN UPDATE SET
            store_name = source.store_name,
            store_country = source.store_country,
            store_city = source.store_city,
            updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (store_id, store_name, store_country, store_city)
        VALUES (source.store_id, source.store_name, source.store_country, source.store_city);
        """
    )
    cur.execute(
        """
        SELECT d.store_id, d.store_key
        FROM dbo.dim_store d
        JOIN #store_stage s ON s.store_id = d.store_id;
        """
    )
    return dict(cur.fetchall())


def _merge_channels(cur, rows: list[dict[str, Any]]) -> dict[str, int]:
    values = _dedupe_by_key(
        rows,
        "marketing_channel",
        lambda r: (r["marketing_channel"],),
    )
    cur.execute(
        """
        CREATE TABLE #channel_stage (
            marketing_channel NVARCHAR(60) NOT NULL PRIMARY KEY
        );
        """
    )
    cur.executemany("INSERT INTO #channel_stage VALUES (?);", values)
    cur.execute(
        """
        MERGE dbo.dim_channel AS target
        USING #channel_stage AS source
            ON target.marketing_channel = source.marketing_channel
        WHEN MATCHED THEN UPDATE SET
            marketing_channel = source.marketing_channel
        WHEN NOT MATCHED THEN INSERT (marketing_channel)
        VALUES (source.marketing_channel);
        """
    )
    cur.execute(
        """
        SELECT d.marketing_channel, d.channel_key
        FROM dbo.dim_channel d
        JOIN #channel_stage s ON s.marketing_channel = d.marketing_channel;
        """
    )
    return dict(cur.fetchall())


def _merge_dates(cur, rows: list[dict[str, Any]]) -> dict[int, int]:
    values = _dedupe_by_key(
        rows,
        "date_key",
        lambda r: (
            r["date_key"],
            _as_date(r["full_date"]),
            r["event_year"],
            r["quarter"],
            r["event_month"],
            r["month_name"],
            r["week_number"],
            r["day_of_month"],
            r["day_name"],
            bool(r["is_weekend"]),
        ),
    )
    cur.execute(
        """
        CREATE TABLE #date_stage (
            date_key INT NOT NULL PRIMARY KEY,
            full_date DATE NOT NULL,
            [year] INT NOT NULL,
            [quarter] INT NOT NULL,
            [month] INT NOT NULL,
            month_name NVARCHAR(20) NOT NULL,
            week_number INT NOT NULL,
            day_of_month INT NOT NULL,
            day_name NVARCHAR(20) NOT NULL,
            is_weekend BIT NOT NULL
        );
        """
    )
    cur.executemany("INSERT INTO #date_stage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", values)
    cur.execute(
        """
        MERGE dbo.dim_date AS target
        USING #date_stage AS source
            ON target.date_key = source.date_key
        WHEN MATCHED THEN UPDATE SET
            full_date = source.full_date,
            [year] = source.[year],
            [quarter] = source.[quarter],
            [month] = source.[month],
            month_name = source.month_name,
            week_number = source.week_number,
            day_of_month = source.day_of_month,
            day_name = source.day_name,
            is_weekend = source.is_weekend
        WHEN NOT MATCHED THEN INSERT (
            date_key, full_date, [year], [quarter], [month], month_name,
            week_number, day_of_month, day_name, is_weekend
        )
        VALUES (
            source.date_key, source.full_date, source.[year], source.[quarter],
            source.[month], source.month_name, source.week_number,
            source.day_of_month, source.day_name, source.is_weekend
        );
        """
    )
    cur.execute(
        """
        SELECT d.date_key, d.date_key
        FROM dbo.dim_date d
        JOIN #date_stage s ON s.date_key = d.date_key;
        """
    )
    return dict(cur.fetchall())


def _insert_facts(
    cur,
    rows: list[dict[str, Any]],
    customer_map: dict[str, int],
    product_map: dict[str, int],
    store_map: dict[str, int],
    channel_map: dict[str, int],
    date_map: dict[int, int],
) -> None:
    fact_values = []
    for row in rows:
        fact_values.append(
            (
                row["event_id"],
                customer_map[row["customer_id"]],
                product_map[row["product_id"]],
                store_map[row["store_id"]],
                date_map[row["date_key"]],
                channel_map[row["marketing_channel"]],
                row["event_type"],
                row["quantity"],
                row["price"],
                row["discount"],
                row["final_price"],
                row["revenue"],
                row["profit_estimate"],
                _as_datetime(row["event_timestamp"]),
                row["event_hour"],
                row.get("rating"),
                row.get("review_text"),
                row.get("session_id"),
                row.get("payment_method"),
            )
        )

    cur.execute(
        """
        CREATE TABLE #fact_stage (
            event_id NVARCHAR(80) NOT NULL PRIMARY KEY,
            customer_key BIGINT NOT NULL,
            product_key BIGINT NOT NULL,
            store_key BIGINT NOT NULL,
            date_key INT NOT NULL,
            channel_key INT NOT NULL,
            event_type NVARCHAR(30) NOT NULL,
            quantity INT NOT NULL,
            price DECIMAL(12, 2) NOT NULL,
            discount DECIMAL(5, 2) NOT NULL,
            final_price DECIMAL(12, 2) NOT NULL,
            revenue DECIMAL(14, 2) NOT NULL,
            profit_estimate DECIMAL(14, 2) NOT NULL,
            event_timestamp DATETIME2(3) NOT NULL,
            event_hour INT NOT NULL,
            rating INT NULL,
            review_text NVARCHAR(1000) NULL,
            session_id NVARCHAR(80) NULL,
            payment_method NVARCHAR(40) NULL
        );
        """
    )
    cur.executemany(
        """
        INSERT INTO #fact_stage VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
        """,
        fact_values,
    )
    cur.execute(
        """
        INSERT INTO dbo.fact_sales (
            event_id, customer_key, product_key, store_key, date_key, channel_key,
            event_type, quantity, price, discount, final_price, revenue,
            profit_estimate, event_timestamp, event_hour, rating, review_text,
            session_id, payment_method
        )
        SELECT
            s.event_id, s.customer_key, s.product_key, s.store_key, s.date_key,
            s.channel_key, s.event_type, s.quantity, s.price, s.discount,
            s.final_price, s.revenue, s.profit_estimate, s.event_timestamp,
            s.event_hour, s.rating, s.review_text, s.session_id, s.payment_method
        FROM #fact_stage s
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.fact_sales f WITH (UPDLOCK, HOLDLOCK)
            WHERE f.event_id = s.event_id
        );
        """
    )


def _insert_dead_letters(cur, rows: list[dict[str, Any]], batch_id: int) -> None:
    values = [
        (
            row.get("event_id"),
            row.get("raw_json"),
            row.get("reject_reason") or "schema parse failure",
            batch_id,
        )
        for row in rows
    ]
    cur.executemany(
        """
        INSERT INTO dbo.dead_letter_events (event_id, raw_payload, reject_reason, batch_id)
        VALUES (?, ?, ?, ?);
        """,
        values,
    )


def _dedupe_by_key(rows: list[dict[str, Any]], key: str, builder) -> list[tuple[Any, ...]]:
    deduped: dict[Any, tuple[Any, ...]] = {}
    for row in rows:
        deduped[row[key]] = builder(row)
    return list(deduped.values())


def _clean_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: _clean_value(value) for key, value in row.items()} for row in rows]


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except TypeError:
        pass
    if str(type(value)).startswith("<class 'pandas.") and hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
