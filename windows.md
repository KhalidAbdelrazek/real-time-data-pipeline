# Real-Time Retail Data Warehouse

A complete local Windows data engineering project using Python, Apache Kafka, PySpark Structured Streaming, SQL Server, SSMS, Faker, Pandas, and SQL. The pipeline streams realistic retail activity, cleans and enriches it in Spark, and loads a SQL Server star schema named `RetailDW`.

## Project Overview

Retail events are generated continuously and sent to Kafka. Spark Structured Streaming reads the Kafka topic, validates the schema, standardizes fields, removes duplicate events, fixes safe timestamp and numeric issues, filters invalid records, enriches valid rows, and writes each micro-batch to SQL Server inside a transaction.

The warehouse is designed for SSMS analytics with dimensions for customer, product, store, date, and marketing channel, plus a `fact_sales` table for event-level behavior and revenue metrics.

## Architecture Diagram

```text
Python data generator
  -> Kafka topic: retail_events
  -> PySpark Structured Streaming
  -> validation, cleaning, deduplication, enrichment
  -> SQL Server writer with batch loads, retries, transactions
  -> RetailDW star schema
  -> SSMS analytics
```

## Folder Structure

```text
project/
|-- producer/
|   |-- data_generator.py
|   |-- producer.py
|-- spark/
|   |-- schema.py
|   |-- transformations.py
|   |-- spark_stream.py
|-- warehouse/
|   |-- sql_server_writer.py
|   |-- create_database.sql
|   |-- create_dimensions.sql
|   |-- create_fact_table.sql
|   |-- analytics_queries.sql
|-- config/
|   |-- config.py
|-- checkpoints/
|-- logs/
|-- requirements.txt
|-- architecture.md
|-- README.md
```

## Star Schema

`dim_customer` stores customer profile, demographics, country, and city.

`dim_product` stores product name, category, brand, and supplier.

`dim_store` stores store identity and location.

`dim_date` stores `date_key`, full date, year, quarter, month, month name, week number, day of month, day name, and weekend flag.

`dim_channel` stores the marketing channel.

`fact_sales` stores event-level facts: event ID, surrogate keys, event type, quantity, price, discount, final price, revenue, profit estimate, timestamp, and supporting review/session attributes used by analytics.

## Generated Data

The producer streams continuously and creates events across 2023, 2024, 2025, and 2026 while still running in real time. Supported event types are `product_view`, `add_to_cart`, `checkout`, `purchase`, `return`, and `review`.

The generator intentionally creates null customer IDs, null product IDs, missing categories, invalid categories, malformed timestamps, duplicate records, negative prices, and negative quantities so the Spark job can demonstrate data quality handling.

## Spark Transformations

Spark performs schema validation, null handling, duplicate removal by `event_id`, standardization, timestamp parsing, invalid record filtering, and enrichment.

Derived columns include `final_price`, `revenue`, `profit_estimate`, `event_date`, `event_year`, `event_month`, `event_hour`, `week_number`, and `is_weekend`.

Business rules:

- `purchase`: revenue equals `final_price * quantity`
- `return`: revenue is negative
- `review`: rating is preserved for product rating analytics
- `checkout`: events are retained for conversion tracking

Invalid records are written to `dbo.dead_letter_events`.

## SQL Server Setup

Install SQL Server Developer Edition or SQL Server Express locally. Enable TCP/IP in SQL Server Configuration Manager and confirm SQL Server listens on port `1433`.

Install SQL Server Management Studio and connect to:

```text
Server name: localhost
Authentication: Windows Authentication
Database: RetailDW
```

If you use SQL authentication, create a login with access to `RetailDW` and set these environment variables before running Spark:

```powershell
$env:SQL_SERVER_TRUSTED_CONNECTION="no"
$env:SQL_SERVER_USER="your_login"
$env:SQL_SERVER_PASSWORD="your_password"
```

## JDBC And ODBC Driver Setup

Spark downloads the SQL Server JDBC driver when you run `spark-submit` with:

```text
com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11
```

The Python writer uses `pyodbc`, so install Microsoft ODBC Driver 18 for SQL Server. The default driver name is `ODBC Driver 18 for SQL Server`. Override it if needed:

```powershell
$env:SQL_SERVER_DRIVER="ODBC Driver 17 for SQL Server"
```

The configured JDBC URL is:

```text
jdbc:sqlserver://localhost:1433;databaseName=RetailDW;encrypt=false;trustServerCertificate=true;
```

## Create Database And Tables

In SSMS, run these files in order:

```text
warehouse/create_database.sql
warehouse/create_dimensions.sql
warehouse/create_fact_table.sql
```

Verify the tables:

```sql
USE RetailDW;
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;
```

## Kafka Setup

Download Kafka 3.x and extract it to `C:\kafka`.

Update `C:\kafka\config\zookeeper.properties`:

```text
dataDir=C:/kafka/zookeeper-data
```

Update `C:\kafka\config\server.properties`:

```text
log.dirs=C:/kafka/kafka-logs
```

Start ZooKeeper:

```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

Start Kafka in a second terminal:

```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Create topics:

```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --create --topic retail_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --topic retail_events_dead_letter --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

## Spark Setup

Install Java JDK 17, Apache Spark 3.5.x pre-built for Hadoop 3, and `winutils.exe`.

Recommended Windows paths:

```text
C:\spark
C:\hadoop\bin\winutils.exe
```

Set environment variables, then open a new terminal:

```powershell
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17"
setx SPARK_HOME "C:\spark"
setx HADOOP_HOME "C:\hadoop"
setx PATH "%PATH%;%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin"
```

Verify:

```powershell
java -version
spark-submit --version
```

## Python Environment

From the project folder:

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

Optional overrides:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_TOPIC="retail_events"
$env:SQL_SERVER_HOST="localhost"
$env:SQL_SERVER_PORT="1433"
$env:SQL_SERVER_DATABASE="RetailDW"
```

## End-To-End Run Order

Use separate terminals.

1. Start ZooKeeper.

```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

2. Start Kafka.

```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

3. Create Kafka topics if they do not already exist.

```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

4. Create SQL Server database and tables in SSMS using the three warehouse scripts.

5. Start the producer.

```powershell
cd "E:\Data Engineer\DEPI Graduation Project\project"
.\venv\Scripts\activate
python producer\producer.py --rate 50
```

6. Start Spark Structured Streaming.

```powershell
cd "E:\Data Engineer\DEPI Graduation Project\project"
.\venv\Scripts\activate
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 spark\spark_stream.py
```

7. Let the stream run for a few minutes, then verify in SSMS.

## Verify Data In SSMS

```sql
USE RetailDW;

SELECT COUNT(*) AS fact_rows FROM dbo.fact_sales;
SELECT COUNT(*) AS rejected_rows FROM dbo.dead_letter_events;

SELECT TOP (10) *
FROM dbo.fact_sales
ORDER BY loaded_at DESC;

SELECT TOP (10) *
FROM dbo.dead_letter_events
ORDER BY created_at DESC;

SELECT COUNT(*) AS customers FROM dbo.dim_customer;
SELECT COUNT(*) AS products FROM dbo.dim_product;
SELECT COUNT(*) AS stores FROM dbo.dim_store;
SELECT COUNT(*) AS dates FROM dbo.dim_date;
SELECT COUNT(*) AS channels FROM dbo.dim_channel;
```

## Example Analytics Queries

Open `warehouse/analytics_queries.sql` in SSMS. It includes total revenue, revenue by country, revenue by category, revenue by store, monthly revenue, yearly revenue, top customers, top products, return rate, conversion rate, customer lifetime value, average product rating, and top marketing channels.

Example:

```sql
SELECT CAST(ROUND(SUM(revenue), 2) AS DECIMAL(14, 2)) AS total_revenue
FROM dbo.fact_sales
WHERE event_type IN (N'purchase', N'return');
```

## Expected Results

After a few minutes at 50 events per second, expect thousands of fact rows, hundreds of rejected rows in `dead_letter_events`, complete product/store/channel dimensions, and a growing customer/date dimension. Exact values differ because the generator is random.

## Troubleshooting

`spark-submit` not found: open a new terminal after setting `SPARK_HOME`, and confirm `%SPARK_HOME%\bin` is on `PATH`.

`winutils.exe` error: confirm `C:\hadoop\bin\winutils.exe` exists and `HADOOP_HOME=C:\hadoop`.

Kafka connection failed: confirm ZooKeeper, Kafka broker, and the topic are running.

SQL Server connection failed: confirm SQL Server is running, TCP/IP is enabled, port `1433` is open, and `RetailDW` exists.

ODBC driver not found: install Microsoft ODBC Driver 18 for SQL Server or set `SQL_SERVER_DRIVER` to the installed driver name.

Login failed: use Windows Authentication with `SQL_SERVER_TRUSTED_CONNECTION=yes`, or set `SQL_SERVER_TRUSTED_CONNECTION=no`, `SQL_SERVER_USER`, and `SQL_SERVER_PASSWORD`.

No rows in SSMS: confirm the producer is sending events, Spark is running, the warehouse scripts were executed, and the Spark terminal is not showing SQL Server writer errors.

Duplicate data after restarting: the writer ignores duplicate `event_id` values in `fact_sales`. To reprocess from scratch, stop Spark, clear `checkpoints/retail_events`, truncate warehouse tables, then restart in the documented order.



1-
java -Xmx512M -Xms512M "-Dlog4j.configuration=file:C:\kafka\config\log4j.properties" "-Dkafka.logs.dir=C:\kafka\logs" -cp "C:\kafka\libs\*" org.apache.zookeeper.server.quorum.QuorumPeerMain C:\kafka\config\zookeeper.properties

2-
java -Xmx1G -Xms1G "-Dlog4j.configuration=file:C:\kafka\config\log4j.properties" "-Dkafka.logs.dir=C:\kafka\logs" -cp "C:\kafka\libs\*" kafka.Kafka C:\kafka\config\server.properties


3-
java -cp "C:\kafka\libs\*" kafka.tools.ConsoleConsumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning