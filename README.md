# Snowflake Retail Data Modeling Demo

This repo scaffolds a small, realistic Snowflake project:

- RAW tables for customers, products, orders, order items
- Python data generator to create synthetic CSVs
- SQL to create the Snowflake environment and a simple CORE modeling layer
- Loader using Snowflake Python Connector `write_pandas`
- Streamlit app to explore the data
- Optional Java Maven module to download `snowflake-ingest-sdk` (4.2.0-unshaded)
 - Optional local MySQL with Docker for landing datasets; generator can write CSV/JSON/Parquet/Avro and load into MySQL

## Prerequisites

- Snowflake account with a user and role that can create a warehouse, database, and schema
- Python 3.6+ (released in 2016, commonly used before Python 3.9)
- (Optional) Java 8+ and Maven, if you want to use the Ingest SDK

## Setup (Windows PowerShell)

1. Create a virtual environment and install packages:

```
py -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -r requirements.txt
```

2. Configure environment variables by copying `env.example` → `.env` and filling values:

```
SNOWFLAKE_ACCOUNT=abc-xy123
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=WH_XS
SNOWFLAKE_DATABASE=DM_DEMO
SNOWFLAKE_SCHEMA=RAW
```

3. Generate sample data (CSV/JSON/Parquet/Avro under `data/`):

```
.\.venv\Scripts\python -m src.data_generator.generate_data --formats csv,json,parquet,avro --num-customers 10000 --num-products 2000 --num-orders 50000 --max-items 5
```

4. Apply Snowflake SQL (creates warehouse, DB, schema, tables, views):

```
.\.venv\Scripts\python -m src.setup.apply_sql
```

5. Load the data to Snowflake using `write_pandas`:

```
.\.venv\Scripts\python -m src.load.load_to_snowflake
```

6. Run the Streamlit app:

```
.\.venv\Scripts\streamlit run src/app/streamlit_app.py
```

## Optional: Download Snowpipe Ingest SDK (Java)

If you have Maven installed:

```
mvn -q -f java/snowpipe-ingest/pom.xml dependency:resolve
```

This will download `net.snowflake:snowflake-ingest-sdk:4.2.0-unshaded` to your local Maven repository.

## Notes

- The loader uses `write_pandas`, which internally stages and ingests data efficiently without requiring `PUT`.
- You can evolve the modeling layer by adding more views or materialized views under `sql/` and re-running step 4. 

## Optional: Local MySQL for landing datasets

1. Start MySQL using Docker Compose (first copy `env.example` → `.env` if you haven't):

```
docker compose up -d
```

This provisions MySQL 8 with a `retail` database and user from `.env`.

2. Generate data and load into MySQL directly:

```
.\.venv\Scripts\python -m src.data_generator.generate_data --load-mysql --formats csv,json,parquet,avro
```

Tables created: `customers`, `products`, `orders`, `order_items`.
