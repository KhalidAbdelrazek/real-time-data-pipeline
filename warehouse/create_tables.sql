-- =============================================================================
-- DuckDB DDL for RetailDW Star Schema
-- Replaces: create_database.sql, create_dimensions.sql, create_fact_table.sql
-- Run with: duckdb warehouse/RetailDW.duckdb < warehouse/create_tables.sql
-- =============================================================================

-- ── Dimensions ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key  BIGINT      PRIMARY KEY,  -- assigned by writer (rowid sequence)
    customer_id   VARCHAR(30) NOT NULL UNIQUE,
    customer_name VARCHAR(150) NOT NULL,
    customer_age  INTEGER     NOT NULL CHECK (customer_age BETWEEN 13 AND 100),
    customer_gender   VARCHAR(30),
    customer_country  VARCHAR(80),
    customer_city     VARCHAR(80),
    created_at    TIMESTAMP   NOT NULL DEFAULT current_timestamp,
    updated_at    TIMESTAMP   NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key      BIGINT      PRIMARY KEY,
    product_id       VARCHAR(30) NOT NULL UNIQUE,
    product_name     VARCHAR(200) NOT NULL,
    product_category VARCHAR(80)  NOT NULL
        CHECK (product_category IN (
            'Electronics','Fashion','Home','Beauty',
            'Sports','Books','Toys','Grocery'
        )),
    brand    VARCHAR(100),
    supplier VARCHAR(150),
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key     BIGINT      PRIMARY KEY,
    store_id      VARCHAR(30) NOT NULL UNIQUE,
    store_name    VARCHAR(150) NOT NULL,
    store_country VARCHAR(80)  NOT NULL,
    store_city    VARCHAR(80)  NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY
        CHECK (date_key BETWEEN 20230101 AND 20261231),
    full_date    DATE    NOT NULL UNIQUE,
    year         INTEGER NOT NULL CHECK (year    BETWEEN 2023 AND 2026),
    quarter      INTEGER NOT NULL CHECK (quarter BETWEEN 1    AND 4),
    month        INTEGER NOT NULL CHECK (month   BETWEEN 1    AND 12),
    month_name   VARCHAR(20) NOT NULL,
    week_number  INTEGER NOT NULL CHECK (week_number  BETWEEN 1 AND 53),
    day_of_month INTEGER NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_name     VARCHAR(20) NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key       INTEGER     PRIMARY KEY,
    marketing_channel VARCHAR(60) NOT NULL UNIQUE
        CHECK (marketing_channel IN (
            'organic','paid_search','social','email','affiliate','direct'
        ))
);

-- ── Fact table ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key       BIGINT    PRIMARY KEY,   -- assigned by writer
    event_id        VARCHAR(80) NOT NULL UNIQUE,
    customer_key    BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    product_key     BIGINT NOT NULL REFERENCES dim_product(product_key),
    store_key       BIGINT NOT NULL REFERENCES dim_store(store_key),
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    channel_key     INTEGER NOT NULL REFERENCES dim_channel(channel_key),
    event_type      VARCHAR(30) NOT NULL
        CHECK (event_type IN (
            'product_view','add_to_cart','checkout',
            'purchase','return','review'
        )),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    price           DOUBLE  NOT NULL CHECK (price > 0),
    discount        DOUBLE  NOT NULL CHECK (discount >= 0 AND discount < 1),
    final_price     DOUBLE  NOT NULL CHECK (final_price >= 0),
    revenue         DOUBLE  NOT NULL,
    profit_estimate DOUBLE  NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_hour      INTEGER   NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    rating          INTEGER   CHECK (rating IS NULL OR rating BETWEEN 1 AND 5),
    review_text     VARCHAR(1000),
    session_id      VARCHAR(80),
    payment_method  VARCHAR(40),
    loaded_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- ── Dead-letter / quarantine ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dead_letter_events (
    dead_letter_key BIGINT    PRIMARY KEY,   -- assigned by writer
    event_id        VARCHAR(80),
    raw_payload     VARCHAR,
    reject_reason   VARCHAR(1000) NOT NULL,
    batch_id        BIGINT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- ── Indexes (DuckDB uses ART indexes automatically on PK/UNIQUE;
--    extra indexes below speed up typical analytical queries) ─────────────────

CREATE INDEX IF NOT EXISTS ix_fact_event_ts      ON fact_sales (event_timestamp);
CREATE INDEX IF NOT EXISTS ix_fact_date_key      ON fact_sales (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_customer_key  ON fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_product_key   ON fact_sales (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_store_key     ON fact_sales (store_key);
CREATE INDEX IF NOT EXISTS ix_fact_channel_key   ON fact_sales (channel_key);
CREATE INDEX IF NOT EXISTS ix_fact_event_type    ON fact_sales (event_type);
CREATE INDEX IF NOT EXISTS ix_fact_loaded_at     ON fact_sales (loaded_at);
CREATE INDEX IF NOT EXISTS ix_dim_cust_country   ON dim_customer (customer_country, customer_city);
CREATE INDEX IF NOT EXISTS ix_dim_prod_category  ON dim_product  (product_category);
CREATE INDEX IF NOT EXISTS ix_dim_store_country  ON dim_store    (store_country, store_city);
CREATE INDEX IF NOT EXISTS ix_dim_date_ym        ON dim_date     (year, month);
CREATE INDEX IF NOT EXISTS ix_dl_created_at      ON dead_letter_events (created_at);
