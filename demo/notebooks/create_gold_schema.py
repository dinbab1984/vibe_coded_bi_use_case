# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Schema and Tables
# MAGIC
# MAGIC This notebook creates gold layer schemas and tables for business-ready aggregated data.


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

print(f"Creating gold schema in catalog: {catalog}")

# COMMAND ----------

# Create gold schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"USE {catalog}.gold")

print(f"Gold schema created: {catalog}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Tables

# COMMAND ----------

# Create sales_dashboard_metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sales_dashboard_metrics (
    metric_date DATE NOT NULL,
    total_orders BIGINT NOT NULL,
    total_sales DECIMAL(18,2) NOT NULL,
    avg_order_value DECIMAL(12,2),
    orders_web BIGINT,
    orders_store BIGINT
)
USING DELTA
COMMENT 'Daily sales metrics for dashboards'
""")

print("✓ Created table: sales_dashboard_metrics")

# COMMAND ----------

# Create product_sales_by_month table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.product_sales_by_month (
    product_id STRING NOT NULL,
    month DATE NOT NULL,
    units_sold BIGINT,
    revenue DECIMAL(18,2),
    returns_rate DECIMAL(5,2),
    revenue_share DECIMAL(5,2)
)
USING DELTA
COMMENT 'Monthly product sales aggregations'
""")

print("✓ Created table: product_sales_by_month")

# COMMAND ----------

# Create customer_lifetime_value_features table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.customer_lifetime_value_features (
    customer_id STRING NOT NULL,
    total_spent DECIMAL(18,2),
    order_count BIGINT,
    avg_days_between_orders DECIMAL(8,2),
    recency_days INT,
    churn_risk_score DECIMAL(5,2)
)
USING DELTA
COMMENT 'Customer lifetime value and churn features'
""")

print("✓ Created table: customer_lifetime_value_features")

# COMMAND ----------

# Create cohort_retention table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.cohort_retention (
    cohort_month DATE NOT NULL,
    month_offset INT NOT NULL,
    active_customers BIGINT,
    retention_rate DECIMAL(5,2)
)
USING DELTA
COMMENT 'Cohort-based customer retention analysis'
""")

print("✓ Created table: cohort_retention")

# COMMAND ----------

# Display all gold tables
display(spark.sql(f"SHOW TABLES IN {catalog}.gold"))

# COMMAND ----------

print("✅ Gold schema and tables created successfully!")





