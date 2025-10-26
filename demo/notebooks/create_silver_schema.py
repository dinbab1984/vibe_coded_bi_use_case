# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Schema and Tables
# MAGIC
# MAGIC This notebook creates silver layer schemas and tables for cleansed and conformed data.

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

print(f"Creating silver schema in catalog: {catalog}")

# COMMAND ----------

# Create silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"USE {catalog}.silver")

print(f"Silver schema created: {catalog}.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Tables

# COMMAND ----------

# Create dim_customer table (SCD Type 2)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_customer (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country_code STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'Customer dimension with SCD Type 2'
""")

print("✓ Created table: dim_customer")

# COMMAND ----------

# Create dim_product table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_product (
    product_id STRING NOT NULL,
    name STRING,
    category STRING,
    list_price DECIMAL(12,2),
    currency_code STRING,
    is_active BOOLEAN,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Product dimension'
""")

print("✓ Created table: dim_product")

# COMMAND ----------

# Create fact_order table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_order (
    order_id STRING NOT NULL,
    customer_id STRING,
    order_date DATE,
    order_timestamp TIMESTAMP,
    status STRING,
    total_amount DECIMAL(12,2),
    currency_code STRING,
    channel STRING,
    payment_status STRING
)
USING DELTA
COMMENT 'Order fact table'
""")

print("✓ Created table: fact_order")

# COMMAND ----------

# Create fact_order_item table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_order_item (
    order_item_id STRING NOT NULL,
    order_id STRING NOT NULL,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(12,2),
    extended_amount DECIMAL(12,2),
    tax_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2)
)
USING DELTA
COMMENT 'Order item fact table'
""")

print("✓ Created table: fact_order_item")

# COMMAND ----------

# Create fact_web_event table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_web_event (
    event_id STRING NOT NULL,
    customer_id STRING,
    session_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    source STRING
)
USING DELTA
COMMENT 'Web event fact table'
""")

print("✓ Created table: fact_web_event")

# COMMAND ----------

# Create dim_time table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_time (
    date_key INT NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    week_of_year INT NOT NULL,
    day_of_week INT NOT NULL
)
USING DELTA
COMMENT 'Time dimension table'
""")

print("✓ Created table: dim_time")

# COMMAND ----------

# Display all silver tables
display(spark.sql(f"SHOW TABLES IN {catalog}.silver"))

# COMMAND ----------

print("✅ Silver schema and tables created successfully!")





