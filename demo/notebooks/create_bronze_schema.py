# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Schema and Tables
# MAGIC
# MAGIC This notebook creates bronze layer schemas and tables for raw data ingestion.
# COMMAND ----------

# Setup path to common utils
import os
import sys

cwd = os.getcwd()
p = os.path.join(cwd, '..', 'src')
if os.path.isdir(p) and p not in sys.path:
    sys.path.insert(0, p)

# COMMAND ----------

from pyspark.sql import SparkSession

# Get catalog from widget or default
dbutils.widgets.text("catalog", "demo_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

print(f"Creating bronze schema in catalog: {catalog}")

# COMMAND ----------

# Create bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"USE {catalog}.bronze")

print(f"Bronze schema created: {catalog}.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables

# COMMAND ----------

# Create orders_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.orders_raw (
    order_id STRING NOT NULL,
    customer_id STRING,
    order_timestamp STRING,
    status STRING,
    currency_code STRING,
    total_amount STRING,
    channel STRING,
    _ingest_time TIMESTAMP NOT NULL,
    _file_name STRING,
    _source STRING
)
USING DELTA
COMMENT 'Raw orders data from source systems'
""")

print("✓ Created table: orders_raw")

# COMMAND ----------

# Create order_items_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.order_items_raw (
    order_item_id STRING NOT NULL,
    order_id STRING NOT NULL,
    product_id STRING,
    quantity STRING,
    unit_price STRING,
    tax_amount STRING,
    discount_amount STRING,
    _ingest_time TIMESTAMP NOT NULL,
    _file_name STRING,
    _source STRING
)
USING DELTA
COMMENT 'Raw order items data'
""")

print("✓ Created table: order_items_raw")

# COMMAND ----------

# Create customers_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.customers_raw (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country_code STRING,
    created_at STRING,
    updated_at STRING,
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING
)
USING DELTA
COMMENT 'Raw customer data'
""")

print("✓ Created table: customers_raw")

# COMMAND ----------

# Create products_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.products_raw (
    product_id STRING NOT NULL,
    name STRING,
    category STRING,
    list_price STRING,
    currency_code STRING,
    is_active STRING,
    _ingest_time TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Raw product master data'
""")

print("✓ Created table: products_raw")

# COMMAND ----------

# Create web_events_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.web_events_raw (
    event_id STRING NOT NULL,
    customer_id STRING,
    session_id STRING,
    event_type STRING,
    event_timestamp STRING,
    source STRING,
    _ingest_time TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Raw web event tracking data'
""")

print("✓ Created table: web_events_raw")

# COMMAND ----------

# Create payments_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.payments_raw (
    payment_id STRING NOT NULL,
    order_id STRING,
    amount STRING,
    currency_code STRING,
    status STRING,
    paid_at STRING,
    _ingest_time TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Raw payment transaction data'
""")

print("✓ Created table: payments_raw")

# COMMAND ----------

# Create inventory_raw table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.inventory_raw (
    product_id STRING NOT NULL,
    location_id STRING NOT NULL,
    quantity_on_hand STRING,
    updated_at STRING,
    _ingest_time TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Raw inventory levels'
""")

print("✓ Created table: inventory_raw")

# COMMAND ----------

# Display all bronze tables
display(spark.sql(f"SHOW TABLES IN {catalog}.bronze"))

# COMMAND ----------

print("✅ Bronze schema and tables created successfully!")





