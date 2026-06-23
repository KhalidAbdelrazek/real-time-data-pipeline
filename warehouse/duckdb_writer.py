"""
DuckDB warehouse writer used by Spark foreachBatch.

Each micro-batch runs inside a single DuckDB transaction:
  1. Dimensions are upserted (SCD Type-1) via temp staging tables.
  2. Facts are inserted idempotently on event_id (skip duplicates).
  3. Invalid rows land in dead_letter_events.

Thread-safety: a threading.Lock serialises writes within one process,
which is safe for single-node spark-submit.
"""

from __future__ import annotations

import logging
import math
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.config import DUCKDB_PATH  # noqa: E402

logger       = logging.getLogger("duckdb_writer")
MAX_RETRIES  = 3
RETRY_SECONDS = 5
_WRITE_LOCK  = threading.Lock()


# ── Public entry-point ────────────────────────────────────────────────────────

def write_microbatch_to_duckdb(valid_df, invalid_df, batch_id: int) -> None:
    """Write one Spark micro-batch to DuckDB with retries."""
    valid_rows   = _clean_records(valid_df.toPandas().to_dict("records"))
    invalid_rows = _clean_records(invalid_df.toPandas().to_dict("records"))

    if not valid_rows and not invalid_rows:
        logger.info("Batch %s: no rows – skipping.", batch_id)
        return

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _WRITE_LOCK:
                _commit_batch(valid_rows, invalid_rows, batch_id)
            logger.info(
                "Batch %s done: %d facts, %d dead-letters.",
                batch_id, len(valid_rows), len(invalid_rows),
            )
            return
        except Exception:
            logger.exception("Batch %s attempt %s/%s failed.", batch_id, attempt, MAX_RETRIES)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_SECONDS * attempt)


# ── Transaction wrapper ───────────────────────────────────────────────────────

def _ensure_schema(con) -> None:
    """Ensure the database schema exists by bootstrapping it if missing."""
    r = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_sales'"
    ).fetchone()
    if not r or r[0] == 0:
        logger.info("Database schema not found. Bootstrapping schema from DDL file...")
        schema_path = Path(__file__).parent / "create_tables.sql"
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                con.execute(f.read())
            logger.info("Database schema initialized successfully.")
        else:
            logger.error("Schema DDL file not found at %s", schema_path)


def _commit_batch(valid_rows, invalid_rows, batch_id):
    con = duckdb.connect(str(DUCKDB_PATH), read_only=False)
    _ensure_schema(con)
    # Temporarily drop indexes on non-PK columns being updated to bypass DuckDB issue #20246.
    # This must be done outside the transaction block because DuckDB constraint checking
    # inside a transaction does not register transactional index drops immediately.
    if valid_rows:
        con.execute("DROP INDEX IF EXISTS ix_dim_cust_country")
        con.execute("DROP INDEX IF EXISTS ix_dim_prod_category")
        con.execute("DROP INDEX IF EXISTS ix_dim_store_country")
    try:
        con.execute("BEGIN TRANSACTION")
        if invalid_rows:
            _insert_dead_letters(con, invalid_rows, batch_id)
        if valid_rows:
            _write_valid_rows(con, valid_rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        # Recreate the exact same indexes outside the transaction so schema definition at-rest remains identical
        if valid_rows:
            try:
                con.execute("CREATE INDEX IF NOT EXISTS ix_dim_cust_country ON dim_customer (customer_country, customer_city)")
                con.execute("CREATE INDEX IF NOT EXISTS ix_dim_prod_category ON dim_product (product_category)")
                con.execute("CREATE INDEX IF NOT EXISTS ix_dim_store_country ON dim_store (store_country, store_city)")
            except Exception:
                pass
        con.close()


def _write_valid_rows(con, rows):
    cmap = _upsert_customers(con, rows)
    pmap = _upsert_products(con, rows)
    smap = _upsert_stores(con, rows)
    chmap = _upsert_channels(con, rows)
    dmap = _upsert_dates(con, rows)
    _insert_facts(con, rows, cmap, pmap, smap, chmap, dmap)


# ── Surrogate-key helper ──────────────────────────────────────────────────────

def _next_key(con, table: str, pk_col: str) -> int:
    row = con.execute(f"SELECT coalesce(max({pk_col}), 0) FROM {table}").fetchone()
    return row[0] if row else 0


# ── Dimension upserts ─────────────────────────────────────────────────────────

def _upsert_customers(con, rows) -> dict[str, int]:
    records = _dedupe_by_key(rows, "customer_id", lambda r: (
        r["customer_id"], r["customer_name"], r["customer_age"],
        r["customer_gender"], r["customer_country"], r["customer_city"],
    ))
    
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _sc (
            customer_id VARCHAR PRIMARY KEY, customer_name VARCHAR NOT NULL,
            customer_age INTEGER NOT NULL, customer_gender VARCHAR,
            customer_country VARCHAR, customer_city VARCHAR
        )""")
    con.execute("DELETE FROM _sc")
    con.executemany("INSERT INTO _sc VALUES (?,?,?,?,?,?)", records)

    base = _next_key(con, "dim_customer", "customer_key")
    # Step 1: insert only rows whose customer_id is not yet in dim_customer.
    # now() is used instead of the bare current_timestamp keyword because
    # DuckDB's binder can misread the bare keyword as a column reference in
    # some statement contexts.
    con.execute(f"""
        INSERT INTO dim_customer (
            customer_key, customer_id, customer_name, customer_age,
            customer_gender, customer_country, customer_city, updated_at
        )
        SELECT {base} + row_number() OVER (),
               s.customer_id, s.customer_name, s.customer_age,
               s.customer_gender, s.customer_country, s.customer_city,
               now()
        FROM _sc s
        WHERE NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id)
    """)
    # Step 2: SCD-1 — overwrite attributes for existing customers only if changed.
    con.execute("""
        UPDATE dim_customer AS d
        SET customer_name    = s.customer_name,
            customer_age     = s.customer_age,
            customer_gender  = s.customer_gender,
            customer_country = s.customer_country,
            customer_city    = s.customer_city,
            updated_at       = now()
        FROM _sc s
        WHERE d.customer_id = s.customer_id
          AND (d.customer_name != s.customer_name
            OR d.customer_age != s.customer_age
            OR d.customer_gender IS DISTINCT FROM s.customer_gender
            OR d.customer_country IS DISTINCT FROM s.customer_country
            OR d.customer_city IS DISTINCT FROM s.customer_city)
    """)
    return dict(con.execute(
        "SELECT d.customer_id, d.customer_key FROM dim_customer d JOIN _sc s USING (customer_id)"
    ).fetchall())


def _upsert_products(con, rows) -> dict[str, int]:
    records = _dedupe_by_key(rows, "product_id", lambda r: (
        r["product_id"], r["product_name"], r["product_category"],
        r["brand"], r["supplier"],
    ))
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _sp (
            product_id VARCHAR PRIMARY KEY, product_name VARCHAR NOT NULL,
            product_category VARCHAR NOT NULL, brand VARCHAR, supplier VARCHAR
        )""")
    con.execute("DELETE FROM _sp")
    con.executemany("INSERT INTO _sp VALUES (?,?,?,?,?)", records)

    base = _next_key(con, "dim_product", "product_key")
    con.execute(f"""
        INSERT INTO dim_product
            (product_key, product_id, product_name, product_category, brand, supplier, updated_at)
        SELECT {base} + row_number() OVER (),
               s.product_id, s.product_name, s.product_category,
               s.brand, s.supplier, now()
        FROM _sp s
        WHERE NOT EXISTS (SELECT 1 FROM dim_product d WHERE d.product_id = s.product_id)
    """)
    con.execute("""
        UPDATE dim_product AS d
        SET product_name=s.product_name, product_category=s.product_category,
            brand=s.brand, supplier=s.supplier, updated_at=now()
        FROM _sp s
        WHERE d.product_id = s.product_id
          AND (d.product_name != s.product_name
            OR d.product_category != s.product_category
            OR d.brand IS DISTINCT FROM s.brand
            OR d.supplier IS DISTINCT FROM s.supplier)
    """)
    return dict(con.execute(
        "SELECT d.product_id, d.product_key FROM dim_product d JOIN _sp s USING (product_id)"
    ).fetchall())


def _upsert_stores(con, rows) -> dict[str, int]:
    records = _dedupe_by_key(rows, "store_id", lambda r: (
        r["store_id"], r["store_name"], r["store_country"], r["store_city"],
    ))
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _ss (
            store_id VARCHAR PRIMARY KEY, store_name VARCHAR NOT NULL,
            store_country VARCHAR NOT NULL, store_city VARCHAR NOT NULL
        )""")
    con.execute("DELETE FROM _ss")
    con.executemany("INSERT INTO _ss VALUES (?,?,?,?)", records)

    base = _next_key(con, "dim_store", "store_key")
    con.execute(f"""
        INSERT INTO dim_store (store_key, store_id, store_name, store_country, store_city, updated_at)
        SELECT {base} + row_number() OVER (),
               s.store_id, s.store_name, s.store_country, s.store_city, now()
        FROM _ss s
        WHERE NOT EXISTS (SELECT 1 FROM dim_store d WHERE d.store_id = s.store_id)
    """)
    con.execute("""
        UPDATE dim_store AS d
        SET store_name=s.store_name, store_country=s.store_country,
            store_city=s.store_city, updated_at=now()
        FROM _ss s
        WHERE d.store_id = s.store_id
          AND (d.store_name != s.store_name
            OR d.store_country != s.store_country
            OR d.store_city != s.store_city)
    """)
    return dict(con.execute(
        "SELECT d.store_id, d.store_key FROM dim_store d JOIN _ss s USING (store_id)"
    ).fetchall())


def _upsert_channels(con, rows) -> dict[str, int]:
    records = _dedupe_by_key(rows, "marketing_channel", lambda r: (r["marketing_channel"],))
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _sch (marketing_channel VARCHAR PRIMARY KEY)""")
    con.execute("DELETE FROM _sch")
    con.executemany("INSERT INTO _sch VALUES (?)", records)

    base = _next_key(con, "dim_channel", "channel_key")
    con.execute(f"""
        INSERT INTO dim_channel (channel_key, marketing_channel)
        SELECT {base} + row_number() OVER (), s.marketing_channel
        FROM _sch s
        WHERE NOT EXISTS (SELECT 1 FROM dim_channel d WHERE d.marketing_channel = s.marketing_channel)
    """)
    return dict(con.execute(
        "SELECT d.marketing_channel, d.channel_key FROM dim_channel d JOIN _sch s USING (marketing_channel)"
    ).fetchall())


def _upsert_dates(con, rows) -> dict[int, int]:
    records = _dedupe_by_key(rows, "date_key", lambda r: (
        r["date_key"], _as_date(r["full_date"]), r["event_year"],
        r["quarter"], r["event_month"], r["month_name"],
        r["week_number"], r["day_of_month"], r["day_name"], bool(r["is_weekend"]),
    ))
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _sd (
            date_key INTEGER PRIMARY KEY, full_date DATE NOT NULL,
            year INTEGER NOT NULL, quarter INTEGER NOT NULL, month INTEGER NOT NULL,
            month_name VARCHAR NOT NULL, week_number INTEGER NOT NULL,
            day_of_month INTEGER NOT NULL, day_name VARCHAR NOT NULL, is_weekend BOOLEAN NOT NULL
        )""")
    con.execute("DELETE FROM _sd")
    con.executemany("INSERT INTO _sd VALUES (?,?,?,?,?,?,?,?,?,?)", records)

    con.execute("""
        INSERT INTO dim_date
            (date_key, full_date, year, quarter, month, month_name,
             week_number, day_of_month, day_name, is_weekend)
        SELECT s.date_key, s.full_date, s.year, s.quarter, s.month, s.month_name,
               s.week_number, s.day_of_month, s.day_name, s.is_weekend
        FROM _sd s
        WHERE NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = s.date_key)
    """)
    return dict(con.execute(
        "SELECT date_key, date_key FROM dim_date JOIN _sd USING (date_key)"
    ).fetchall())


# ── Fact insert ───────────────────────────────────────────────────────────────

def _insert_facts(con, rows, cmap, pmap, smap, chmap, dmap):
    base = _next_key(con, "fact_sales", "sales_key")
    values = []
    for i, row in enumerate(rows, 1):
        ck = cmap.get(row["customer_id"])
        pk = pmap.get(row["product_id"])
        sk = smap.get(row["store_id"])
        chk = chmap.get(row["marketing_channel"])
        dk = dmap.get(row["date_key"])
        if None in (ck, pk, sk, chk, dk):
            logger.warning("Missing FK for event_id=%s – skipping fact.", row.get("event_id"))
            continue
        values.append((
            base + i, row["event_id"], ck, pk, sk, dk, chk,
            row["event_type"], row["quantity"],
            float(row["price"]), float(row["discount"]),
            float(row["final_price"]), float(row["revenue"]),
            float(row["profit_estimate"]),
            _as_datetime(row["event_timestamp"]), row["event_hour"],
            row.get("rating"), row.get("review_text"),
            row.get("session_id"), row.get("payment_method"),
        ))

    if not values:
        return

    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _sf (
            sales_key BIGINT PRIMARY KEY, event_id VARCHAR NOT NULL,
            customer_key BIGINT NOT NULL, product_key BIGINT NOT NULL,
            store_key BIGINT NOT NULL, date_key INTEGER NOT NULL,
            channel_key INTEGER NOT NULL, event_type VARCHAR NOT NULL,
            quantity INTEGER NOT NULL, price DOUBLE NOT NULL,
            discount DOUBLE NOT NULL, final_price DOUBLE NOT NULL,
            revenue DOUBLE NOT NULL, profit_estimate DOUBLE NOT NULL,
            event_timestamp TIMESTAMP NOT NULL, event_hour INTEGER NOT NULL,
            rating INTEGER, review_text VARCHAR,
            session_id VARCHAR, payment_method VARCHAR
        )""")
    con.execute("DELETE FROM _sf")
    con.executemany("INSERT INTO _sf VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)

    con.execute("""
        INSERT INTO fact_sales (
            sales_key, event_id, customer_key, product_key, store_key, date_key, channel_key,
            event_type, quantity, price, discount, final_price, revenue, profit_estimate,
            event_timestamp, event_hour, rating, review_text, session_id, payment_method,
            loaded_at
        )
        SELECT
            sf.sales_key, sf.event_id, sf.customer_key, sf.product_key, sf.store_key, sf.date_key, sf.channel_key,
            sf.event_type, sf.quantity, sf.price, sf.discount, sf.final_price, sf.revenue, sf.profit_estimate,
            sf.event_timestamp, sf.event_hour, sf.rating, sf.review_text, sf.session_id, sf.payment_method,
            now()
        FROM _sf sf
        WHERE NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.event_id = sf.event_id)
    """)


# ── Dead-letter ───────────────────────────────────────────────────────────────

def _insert_dead_letters(con, rows, batch_id):
    base = _next_key(con, "dead_letter_events", "dead_letter_key")
    values = []
    for i, row in enumerate(rows, 1):
        # Gracefully fall back if Spark/Pandas used 'raw_json' or 'raw_payload'
        payload = row.get("raw_json") or row.get("raw_payload") or ""
        reason = row.get("reject_reason") or "schema parse failure"
        
        values.append((
            base + i,
            row.get("event_id"),
            payload,
            reason,
            batch_id
        ))
        
    con.executemany(
        """
        INSERT INTO dead_letter_events (
            dead_letter_key, event_id, raw_payload, reject_reason, batch_id
        ) VALUES (?, ?, ?, ?, ?)
        """,
        values,
    )


# ── Utilities ─────────────────────────────────────────────────────────────────

def _dedupe_by_key(rows, key, builder):
    seen = {}
    for row in rows:
        seen[row[key]] = builder(row)
    return list(seen.values())


def _clean_records(rows):
    return [{k: _clean_value(v) for k, v in row.items()} for row in rows]


def _clean_value(value):
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


def _as_date(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def _as_datetime(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))