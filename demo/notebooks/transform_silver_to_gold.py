# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Silver to Gold
# MAGIC
# MAGIC This notebook creates business-ready aggregations in the gold layer with data quality checks.

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
from demo.common_utils import safe_cast_column

# Get catalog from widget
dbutils.widgets.text("catalog", "demo_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

print(f"Transforming silver to gold in catalog: {catalog}")

# COMMAND ----------

# Initialize DQ Engine
dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create sales_dashboard_metrics

# COMMAND ----------

# Load fact_order
orders_df = spark.table(f"{catalog}.silver.fact_order")

# Aggregate by date and channel
sales_metrics_df = orders_df.groupBy("order_date").agg(
    F.count("order_id").alias("total_orders"),
    F.sum("total_amount").alias("total_sales"),
    (F.sum("total_amount") / F.count("order_id")).alias("avg_order_value"),
    F.sum(F.when(F.col("channel") == "WEB", 1).otherwise(0)).alias("orders_web"),
    F.sum(F.when(F.col("channel") == "STORE", 1).otherwise(0)).alias("orders_store")
)

# Rename order_date to metric_date
sales_metrics_df = sales_metrics_df.withColumnRenamed("order_date", "metric_date")

# Cast to proper types
sales_metrics_df = safe_cast_column(sales_metrics_df, "total_orders", "BIGINT")
sales_metrics_df = safe_cast_column(sales_metrics_df, "total_sales", "DECIMAL(18,2)")
sales_metrics_df = safe_cast_column(sales_metrics_df, "avg_order_value", "DECIMAL(12,2)")
sales_metrics_df = safe_cast_column(sales_metrics_df, "orders_web", "BIGINT")
sales_metrics_df = safe_cast_column(sales_metrics_df, "orders_store", "BIGINT")

# Write to gold
sales_metrics_df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.sales_dashboard_metrics")

print(f"✓ Created sales_dashboard_metrics with {sales_metrics_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create product_sales_by_month

# COMMAND ----------

# Load order items and products
order_items_df = spark.table(f"{catalog}.silver.fact_order_item")
orders_df = spark.table(f"{catalog}.silver.fact_order")
products_df = spark.table(f"{catalog}.silver.dim_product")

# Join to get order dates
order_items_with_date = order_items_df.join(
    orders_df.select("order_id", "order_date"),
    "order_id",
    "inner"
)

# Calculate month
order_items_with_date = order_items_with_date.withColumn(
    "month",
    F.trunc(F.col("order_date"), "month")
)

# Aggregate by product and month
product_sales_df = order_items_with_date.groupBy("product_id", "month").agg(
    F.sum("quantity").alias("units_sold"),
    F.sum("extended_amount").alias("revenue")
)

# Calculate returns_rate (placeholder - would need returns data)
product_sales_df = product_sales_df.withColumn("returns_rate", F.lit(0.0).cast("decimal(5,2)"))

# Calculate revenue share within each month
window_month = Window.partitionBy("month")
product_sales_df = product_sales_df.withColumn(
    "revenue_share",
    (F.col("revenue") / F.sum("revenue").over(window_month) * 100).cast("decimal(5,2)")
)

# Cast to proper types
product_sales_df = safe_cast_column(product_sales_df, "units_sold", "BIGINT")
product_sales_df = safe_cast_column(product_sales_df, "revenue", "DECIMAL(18,2)")

# Data quality checks
product_sales_dq_checks = [
    {
        "name": "valid_monthly_units",
        "criticality": "error",
        "check": {
            "function": "is_not_less_than",
            "arguments": {
                "column": "units_sold",
                "limit": 0
            }
        }
    }
]

# Apply DQ checks
valid_product_sales, invalid_product_sales = dq_engine.apply_checks_by_metadata_and_split(
    product_sales_df, product_sales_dq_checks
)

# Write to gold
valid_product_sales.write.mode("overwrite").saveAsTable(f"{catalog}.gold.product_sales_by_month")

print(f"✓ Created product_sales_by_month with {valid_product_sales.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create customer_lifetime_value_features

# COMMAND ----------

# Load orders and customers
orders_df = spark.table(f"{catalog}.silver.fact_order")
customers_df = spark.table(f"{catalog}.silver.dim_customer").filter(F.col("is_current") == True)

# Calculate CLV features per customer
clv_features_df = orders_df.groupBy("customer_id").agg(
    F.sum("total_amount").alias("total_spent"),
    F.count("order_id").alias("order_count"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date")
)

# Calculate avg_days_between_orders
clv_features_df = clv_features_df.withColumn(
    "avg_days_between_orders",
    F.when(
        F.col("order_count") > 1,
        F.datediff(F.col("last_order_date"), F.col("first_order_date")) / (F.col("order_count") - 1)
    ).otherwise(F.lit(None))
)

# Calculate recency_days
clv_features_df = clv_features_df.withColumn(
    "recency_days",
    F.datediff(F.current_date(), F.col("last_order_date"))
)

# Calculate churn_risk_score (simple heuristic: higher recency = higher risk)
clv_features_df = clv_features_df.withColumn(
    "churn_risk_score",
    F.when(F.col("recency_days") > 180, 80.0)
     .when(F.col("recency_days") > 90, 50.0)
     .when(F.col("recency_days") > 30, 20.0)
     .otherwise(5.0)
)

# Select final columns
clv_features_df = clv_features_df.select(
    "customer_id",
    "total_spent",
    "order_count",
    "avg_days_between_orders",
    "recency_days",
    "churn_risk_score"
)

# Cast to proper types
clv_features_df = safe_cast_column(clv_features_df, "total_spent", "DECIMAL(18,2)")
clv_features_df = safe_cast_column(clv_features_df, "order_count", "BIGINT")
clv_features_df = safe_cast_column(clv_features_df, "avg_days_between_orders", "DECIMAL(8,2)")
clv_features_df = safe_cast_column(clv_features_df, "recency_days", "INT")
clv_features_df = safe_cast_column(clv_features_df, "churn_risk_score", "DECIMAL(5,2)")

# Data quality checks
clv_dq_checks = [
    {
        "name": "referential_integrity",
        "criticality": "error",
        "check": {
            "function": "sql_expression",
            "arguments": {
                "expression": f"customer_id IN (SELECT customer_id FROM {catalog}.silver.dim_customer WHERE is_current = true)"
            }
        }
    }
]

# Apply DQ checks
valid_clv, invalid_clv = dq_engine.apply_checks_by_metadata_and_split(clv_features_df, clv_dq_checks)

# Write to gold
valid_clv.write.mode("overwrite").saveAsTable(f"{catalog}.gold.customer_lifetime_value_features")

print(f"✓ Created customer_lifetime_value_features with {valid_clv.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create cohort_retention

# COMMAND ----------

# Get first order date per customer (cohort month)
customer_cohorts = orders_df.groupBy("customer_id").agg(
    F.trunc(F.min("order_date"), "month").alias("cohort_month")
)

# Join with all orders
orders_with_cohort = orders_df.join(customer_cohorts, "customer_id", "inner")

# Calculate month offset from cohort
orders_with_cohort = orders_with_cohort.withColumn(
    "order_month",
    F.trunc(F.col("order_date"), "month")
)

orders_with_cohort = orders_with_cohort.withColumn(
    "month_offset",
    F.months_between(F.col("order_month"), F.col("cohort_month")).cast("int")
)

# Count active customers per cohort and month offset
cohort_retention_df = orders_with_cohort.groupBy("cohort_month", "month_offset").agg(
    F.countDistinct("customer_id").alias("active_customers")
)

# Calculate retention rate (% of cohort month 0)
window_cohort = Window.partitionBy("cohort_month")
cohort_retention_df = cohort_retention_df.withColumn(
    "cohort_size",
    F.first(F.when(F.col("month_offset") == 0, F.col("active_customers"))).over(
        window_cohort.orderBy("month_offset")
    )
)

cohort_retention_df = cohort_retention_df.withColumn(
    "retention_rate",
    (F.col("active_customers") / F.col("cohort_size") * 100).cast("decimal(5,2)")
)

# Select final columns
cohort_retention_df = cohort_retention_df.select(
    "cohort_month",
    "month_offset",
    "active_customers",
    "retention_rate"
)

# Cast to proper types
cohort_retention_df = safe_cast_column(cohort_retention_df, "active_customers", "BIGINT")

# Data quality checks
cohort_dq_checks = [
    {
        "name": "valid_retention_bounds",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {
                "column": "retention_rate",
                "min_limit": 0,
                "max_limit": 100
            }
        }
    }
]

# Apply DQ checks
valid_cohort, invalid_cohort = dq_engine.apply_checks_by_metadata_and_split(cohort_retention_df, cohort_dq_checks)

# Write to gold
valid_cohort.write.mode("overwrite").saveAsTable(f"{catalog}.gold.cohort_retention")

print(f"✓ Created cohort_retention with {valid_cohort.count()} records")

# COMMAND ----------

print("✅ Silver to Gold transformation completed successfully!")





