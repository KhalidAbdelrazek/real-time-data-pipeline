-- SQL Server fact and dead-letter tables for RetailDW.
-- Run after create_dimensions.sql.

USE RetailDW;
GO

IF OBJECT_ID(N'dbo.fact_sales', N'U') IS NOT NULL DROP TABLE dbo.fact_sales;
IF OBJECT_ID(N'dbo.dead_letter_events', N'U') IS NOT NULL DROP TABLE dbo.dead_letter_events;
GO

CREATE TABLE dbo.fact_sales (
    sales_key BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_fact_sales PRIMARY KEY,
    event_id NVARCHAR(80) NOT NULL,
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
    payment_method NVARCHAR(40) NULL,
    loaded_at DATETIME2(3) NOT NULL CONSTRAINT DF_fact_sales_loaded_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_fact_sales_event_id UNIQUE (event_id),
    CONSTRAINT FK_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES dbo.dim_customer(customer_key),
    CONSTRAINT FK_fact_sales_product FOREIGN KEY (product_key) REFERENCES dbo.dim_product(product_key),
    CONSTRAINT FK_fact_sales_store FOREIGN KEY (store_key) REFERENCES dbo.dim_store(store_key),
    CONSTRAINT FK_fact_sales_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key),
    CONSTRAINT FK_fact_sales_channel FOREIGN KEY (channel_key) REFERENCES dbo.dim_channel(channel_key),
    CONSTRAINT CK_fact_sales_event_type CHECK (
        event_type IN (N'product_view', N'add_to_cart', N'checkout', N'purchase', N'return', N'review')
    ),
    CONSTRAINT CK_fact_sales_quantity CHECK (quantity > 0),
    CONSTRAINT CK_fact_sales_price CHECK (price > 0),
    CONSTRAINT CK_fact_sales_discount CHECK (discount >= 0 AND discount < 1),
    CONSTRAINT CK_fact_sales_final_price CHECK (final_price >= 0),
    CONSTRAINT CK_fact_sales_event_hour CHECK (event_hour BETWEEN 0 AND 23),
    CONSTRAINT CK_fact_sales_rating CHECK (rating IS NULL OR rating BETWEEN 1 AND 5)
);

CREATE TABLE dbo.dead_letter_events (
    dead_letter_key BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dead_letter_events PRIMARY KEY,
    event_id NVARCHAR(80) NULL,
    raw_payload NVARCHAR(MAX) NULL,
    reject_reason NVARCHAR(1000) NOT NULL,
    batch_id BIGINT NOT NULL,
    created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dead_letter_events_created_at DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX IX_fact_sales_event_timestamp ON dbo.fact_sales (event_timestamp);
CREATE INDEX IX_fact_sales_date_key ON dbo.fact_sales (date_key);
CREATE INDEX IX_fact_sales_customer_key ON dbo.fact_sales (customer_key);
CREATE INDEX IX_fact_sales_product_key ON dbo.fact_sales (product_key);
CREATE INDEX IX_fact_sales_store_key ON dbo.fact_sales (store_key);
CREATE INDEX IX_fact_sales_channel_key ON dbo.fact_sales (channel_key);
CREATE INDEX IX_fact_sales_event_type ON dbo.fact_sales (event_type);
CREATE INDEX IX_fact_sales_loaded_at ON dbo.fact_sales (loaded_at);
CREATE INDEX IX_dead_letter_events_created_at ON dbo.dead_letter_events (created_at);
GO
