-- SQL Server dimension tables for RetailDW.
-- Run after create_database.sql.

USE RetailDW;
GO

IF OBJECT_ID(N'dbo.fact_sales', N'U') IS NOT NULL DROP TABLE dbo.fact_sales;
IF OBJECT_ID(N'dbo.dead_letter_events', N'U') IS NOT NULL DROP TABLE dbo.dead_letter_events;
IF OBJECT_ID(N'dbo.dim_channel', N'U') IS NOT NULL DROP TABLE dbo.dim_channel;
IF OBJECT_ID(N'dbo.dim_date', N'U') IS NOT NULL DROP TABLE dbo.dim_date;
IF OBJECT_ID(N'dbo.dim_store', N'U') IS NOT NULL DROP TABLE dbo.dim_store;
IF OBJECT_ID(N'dbo.dim_product', N'U') IS NOT NULL DROP TABLE dbo.dim_product;
IF OBJECT_ID(N'dbo.dim_customer', N'U') IS NOT NULL DROP TABLE dbo.dim_customer;
GO

CREATE TABLE dbo.dim_customer (
    customer_key BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_customer PRIMARY KEY,
    customer_id NVARCHAR(30) NOT NULL,
    customer_name NVARCHAR(150) NOT NULL,
    customer_age INT NOT NULL,
    customer_gender NVARCHAR(30) NULL,
    customer_country NVARCHAR(80) NULL,
    customer_city NVARCHAR(80) NULL,
    created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_customer_created_at DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_customer_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_dim_customer_customer_id UNIQUE (customer_id),
    CONSTRAINT CK_dim_customer_age CHECK (customer_age BETWEEN 13 AND 100)
);

CREATE TABLE dbo.dim_product (
    product_key BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_product PRIMARY KEY,
    product_id NVARCHAR(30) NOT NULL,
    product_name NVARCHAR(200) NOT NULL,
    product_category NVARCHAR(80) NOT NULL,
    brand NVARCHAR(100) NULL,
    supplier NVARCHAR(150) NULL,
    created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_product_created_at DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_product_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_dim_product_product_id UNIQUE (product_id),
    CONSTRAINT CK_dim_product_category CHECK (
        product_category IN (N'Electronics', N'Fashion', N'Home', N'Beauty', N'Sports', N'Books', N'Toys', N'Grocery')
    )
);

CREATE TABLE dbo.dim_store (
    store_key BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_store PRIMARY KEY,
    store_id NVARCHAR(30) NOT NULL,
    store_name NVARCHAR(150) NOT NULL,
    store_country NVARCHAR(80) NOT NULL,
    store_city NVARCHAR(80) NOT NULL,
    created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_store_created_at DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_store_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_dim_store_store_id UNIQUE (store_id)
);

CREATE TABLE dbo.dim_date (
    date_key INT NOT NULL CONSTRAINT PK_dim_date PRIMARY KEY,
    full_date DATE NOT NULL,
    [year] INT NOT NULL,
    [quarter] INT NOT NULL,
    [month] INT NOT NULL,
    month_name NVARCHAR(20) NOT NULL,
    week_number INT NOT NULL,
    day_of_month INT NOT NULL,
    day_name NVARCHAR(20) NOT NULL,
    is_weekend BIT NOT NULL,
    CONSTRAINT UQ_dim_date_full_date UNIQUE (full_date),
    CONSTRAINT CK_dim_date_year CHECK ([year] BETWEEN 2023 AND 2026),
    CONSTRAINT CK_dim_date_quarter CHECK ([quarter] BETWEEN 1 AND 4),
    CONSTRAINT CK_dim_date_month CHECK ([month] BETWEEN 1 AND 12),
    CONSTRAINT CK_dim_date_week CHECK (week_number BETWEEN 1 AND 53),
    CONSTRAINT CK_dim_date_day CHECK (day_of_month BETWEEN 1 AND 31)
);

CREATE TABLE dbo.dim_channel (
    channel_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_channel PRIMARY KEY,
    marketing_channel NVARCHAR(60) NOT NULL,
    CONSTRAINT UQ_dim_channel_marketing_channel UNIQUE (marketing_channel),
    CONSTRAINT CK_dim_channel_marketing_channel CHECK (
        marketing_channel IN (N'organic', N'paid_search', N'social', N'email', N'affiliate', N'direct')
    )
);
GO

CREATE INDEX IX_dim_customer_country_city ON dbo.dim_customer (customer_country, customer_city);
CREATE INDEX IX_dim_product_category ON dbo.dim_product (product_category);
CREATE INDEX IX_dim_store_country_city ON dbo.dim_store (store_country, store_city);
CREATE INDEX IX_dim_date_year_month ON dbo.dim_date ([year], [month]);
GO
