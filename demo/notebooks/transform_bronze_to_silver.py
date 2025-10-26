# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Bronze to Silver
# MAGIC
# MAGIC This notebook transforms raw bronze data into cleansed silver layer with data quality checks.

# COMMAND ----------

# Install dependencies
%pip install databricks-labs-dqx==0.9.3
dbutils.library.restartPython()

# COMMAND ----------

# Setup path to common utils
import os
import sys

cwd = os.getcwd()
p = os.path.join(cwd, '..', 'src')
if os.path.isdir(p) and p not in sys.path:
    sys.path.insert(0, p)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.sdk import WorkspaceClient
from demo.common_utils import safe_cast_column, deduplicate_by_key, normalize_string_column, create_date_dimension

# Get catalog from widget
dbutils.widgets.text("catalog", "demo_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

print(f"Transforming bronze to silver in catalog: {catalog}")

# COMMAND ----------

# Initialize DQ Engine
dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform dim_product

# COMMAND ----------

# Load products_raw
products_df = spark.table(f"{catalog}.bronze.products_raw")

# Deduplicate by product_id
products_df = deduplicate_by_key(products_df, ["product_id"])

# Transform and cleanse
products_df = products_df.select(
    F.col("product_id"),
    F.trim(F.col("name")).alias("name"),
    F.upper(F.trim(F.col("category"))).alias("category"),
    F.col("list_price"),
    F.col("currency_code"),
    F.col("is_active"),
    F.col("_ingest_time")
)

# Safe cast columns
products_df = safe_cast_column(products_df, "list_price", "DECIMAL(12,2)")
products_df = safe_cast_column(products_df, "is_active", "BOOLEAN")
products_df = normalize_string_column(products_df, "currency_code", uppercase=True)

# Rename _ingest_time to updated_at
products_df = products_df.withColumnRenamed("_ingest_time", "updated_at")

# Data quality checks for dim_product
product_dq_checks = [
    {
        "name": "valid_price",
        "criticality": "error",
        "check": {
            "function": "is_not_less_than",
            "arguments": {
                "column": "list_price",
                "limit": 0
            }
        }
    }
]

# Apply DQ checks and split valid/invalid
valid_products, invalid_products = dq_engine.apply_checks_by_metadata_and_split(products_df, product_dq_checks)

# Select only target table columns in correct order
valid_products_final = valid_products.select(
    "product_id", "name", "category", "list_price",
    "currency_code", "is_active", "updated_at"
)

# Write valid records to silver
valid_products_final.write.mode("overwrite").saveAsTable(f"{catalog}.silver.dim_product")

print(f"✓ Loaded {valid_products.count()} products into silver.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform fact_order

# COMMAND ----------

# Load orders_raw
orders_df = spark.table(f"{catalog}.bronze.orders_raw")

# Deduplicate orders
orders_df = deduplicate_by_key(orders_df, ["order_id"])

# Load payments for enrichment
payments_df = spark.table(f"{catalog}.bronze.payments_raw")
payments_df = deduplicate_by_key(payments_df, ["order_id"])
payments_df = payments_df.select(
    F.col("order_id"),
    F.upper(F.trim(F.col("status"))).alias("payment_status")
)

# Transform orders
orders_df = orders_df.select(
    F.col("order_id"),
    F.col("customer_id"),
    F.col("order_timestamp"),
    F.col("status"),
    F.col("total_amount"),
    F.col("currency_code"),
    F.col("channel")
)

# Safe cast and parse columns
orders_df = orders_df.withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
orders_df = orders_df.withColumn("order_date", F.to_date(F.col("order_timestamp")))
orders_df = safe_cast_column(orders_df, "total_amount", "DECIMAL(12,2)")
orders_df = normalize_string_column(orders_df, "currency_code", uppercase=True)
orders_df = normalize_string_column(orders_df, "channel", uppercase=True)

# Normalize status
orders_df = orders_df.withColumn(
    "status",
    F.when(F.upper(F.col("status")) == "SHIPPED", "SHIPPED")
     .when(F.upper(F.col("status")) == "DELIVERED", "DELIVERED")
     .when(F.upper(F.col("status")) == "PENDING", "PENDING")
     .when(F.upper(F.col("status")) == "CANCELLED", "CANCELLED")
     .otherwise("OTHER")
)

# Join with payment status
orders_df = orders_df.join(payments_df, "order_id", "left")

# Data quality checks for fact_order
order_dq_checks = [
    {
        "name": "valid_order_id",
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {
                "column": "order_id"
            }
        }
    },
    {
        "name": "non_negative_amount",
        "criticality": "error",
        "check": {
            "function": "is_not_less_than",
            "arguments": {
                "column": "total_amount",
                "limit": 0
            }
        }
    },
    {
        "name": "valid_currency",
        "criticality": "warn",
        "check": {
            "function": "is_in_list",
            "arguments": {
                "column": "currency_code",
                "allowed": ["USD", "EUR", "GBP", "INR", "JPY"]
            }
        }
    }
]

# Apply DQ checks
valid_orders, invalid_orders = dq_engine.apply_checks_by_metadata_and_split(orders_df, order_dq_checks)

# Select only target table columns in correct order
valid_orders_final = valid_orders.select(
    "order_id", "customer_id", "order_date", "order_timestamp",
    "status", "total_amount", "currency_code", "channel", "payment_status"
)

# Write to silver
valid_orders_final.write.mode("overwrite").saveAsTable(f"{catalog}.silver.fact_order")

print(f"✓ Loaded {valid_orders.count()} orders into silver.fact_order")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform fact_order_item

# COMMAND ----------

# Load order_items_raw
order_items_df = spark.table(f"{catalog}.bronze.order_items_raw")

# Deduplicate
order_items_df = deduplicate_by_key(order_items_df, ["order_item_id"])

# Transform
order_items_df = order_items_df.select(
    F.col("order_item_id"),
    F.col("order_id"),
    F.col("product_id"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("tax_amount"),
    F.col("discount_amount")
)

# Safe cast columns
order_items_df = safe_cast_column(order_items_df, "quantity", "INT")
order_items_df = safe_cast_column(order_items_df, "unit_price", "DECIMAL(12,2)")
order_items_df = safe_cast_column(order_items_df, "tax_amount", "DECIMAL(12,2)")
order_items_df = safe_cast_column(order_items_df, "discount_amount", "DECIMAL(12,2)")

# Derive extended_amount and cast to proper type
order_items_df = order_items_df.withColumn(
    "extended_amount",
    F.coalesce(F.col("quantity") * F.col("unit_price"), F.lit(0)).cast("decimal(12,2)")
)

# Data quality checks
order_item_dq_checks = [
    {
        "name": "valid_quantity",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {
                "column": "quantity",
                "min_limit": 1,
                "max_limit": 9999
            }
        }
    }
]

# Apply DQ checks
valid_items, invalid_items = dq_engine.apply_checks_by_metadata_and_split(order_items_df, order_item_dq_checks)

# Select only the target table columns in correct order before writing
valid_items_final = valid_items.select(
    "order_item_id", "order_id", "product_id", "quantity",
    "unit_price", "extended_amount", "tax_amount", "discount_amount"
)

# Write to silver
valid_items_final.write.mode("overwrite").saveAsTable(f"{catalog}.silver.fact_order_item")

print(f"✓ Loaded {valid_items.count()} order items into silver.fact_order_item")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform dim_customer

# COMMAND ----------

# Load customers_raw
customers_df = spark.table(f"{catalog}.bronze.customers_raw")

# Deduplicate
customers_df = deduplicate_by_key(customers_df, ["customer_id"])

# Transform
customers_df = customers_df.select(
    F.col("customer_id"),
    F.trim(F.col("first_name")).alias("first_name"),
    F.trim(F.col("last_name")).alias("last_name"),
    F.trim(F.lower(F.col("email"))).alias("email"),
    F.col("phone"),
    F.col("country_code"),
    F.col("created_at"),
    F.col("updated_at")
)

# Parse timestamps
customers_df = customers_df.withColumn("created_at", F.to_timestamp(F.col("created_at")))
customers_df = customers_df.withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
customers_df = normalize_string_column(customers_df, "country_code", uppercase=True)

# Add SCD2 columns for initial load
customers_df = customers_df.withColumn("effective_from", F.current_timestamp())
customers_df = customers_df.withColumn("effective_to", F.lit(None).cast("timestamp"))
customers_df = customers_df.withColumn("is_current", F.lit(True))

# Data quality checks
customer_dq_checks = [
    {
        "name": "valid_email_format",
        "criticality": "warn",
        "check": {
            "function": "regex_match",
            "arguments": {
                "column": "email",
                "regex": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
            }
        }
    }
]

# Apply DQ checks
valid_customers, invalid_customers = dq_engine.apply_checks_by_metadata_and_split(customers_df, customer_dq_checks)

# Select only target table columns in correct order
valid_customers_final = valid_customers.select(
    "customer_id", "first_name", "last_name", "email", "phone",
    "country_code", "created_at", "updated_at",
    "effective_from", "effective_to", "is_current"
)

# Write to silver
valid_customers_final.write.mode("overwrite").saveAsTable(f"{catalog}.silver.dim_customer")

print(f"✓ Loaded {valid_customers.count()} customers into silver.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform fact_web_event

# COMMAND ----------

# Load web_events_raw
events_df = spark.table(f"{catalog}.bronze.web_events_raw")

# Transform
events_df = events_df.select(
    F.col("event_id"),
    F.col("customer_id"),
    F.col("session_id"),
    F.col("event_type"),
    F.col("event_timestamp"),
    F.col("source")
)

# Parse timestamp
events_df = events_df.withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))

# Normalize event_type and source
events_df = events_df.withColumn(
    "event_type",
    F.when(F.upper(F.col("event_type")) == "VIEW", "VIEW")
     .when(F.upper(F.col("event_type")) == "ADD_TO_CART", "ADD_TO_CART")
     .when(F.upper(F.col("event_type")) == "PURCHASE", "PURCHASE")
     .otherwise("OTHER")
)

events_df = normalize_string_column(events_df, "source", uppercase=True)

# Select only target table columns in correct order
events_df_final = events_df.select(
    "event_id", "customer_id", "session_id",
    "event_type", "event_timestamp", "source"
)

# Write to silver (no DQ checks for events in spec)
events_df_final.write.mode("overwrite").saveAsTable(f"{catalog}.silver.fact_web_event")

print(f"✓ Loaded {events_df.count()} web events into silver.fact_web_event")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dim_time

# COMMAND ----------

# Generate date dimension for 3 years (past to future)
from datetime import datetime, timedelta

start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
end_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')

dim_time_df = create_date_dimension(spark, start_date, end_date)

# Write to silver
dim_time_df.write.mode("overwrite").saveAsTable(f"{catalog}.silver.dim_time")

print(f"✓ Created dim_time with {dim_time_df.count()} date records")

# COMMAND ----------

print("✅ Bronze to Silver transformation completed successfully!")

