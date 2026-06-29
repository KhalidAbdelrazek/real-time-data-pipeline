-- Create the SQL Server database for the local retail data warehouse.
-- Run from SSMS while connected to the master database.

IF DB_ID(N'RetailDW') IS NULL
BEGIN
    CREATE DATABASE RetailDW;
END;
GO

ALTER DATABASE RetailDW SET RECOVERY SIMPLE;
GO

USE RetailDW;
GO

IF SCHEMA_ID(N'dbo') IS NULL
    EXEC(N'CREATE SCHEMA dbo');
GO
