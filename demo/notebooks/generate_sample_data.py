# Databricks notebook source
# MAGIC %md
# MAGIC # Generate and Load Sample Data
# MAGIC
# MAGIC This notebook generates synthetic sample data and loads it into bronze tables.

# COMMAND ----------

# Install dependencies
%pip install databricks-labs-dqx==0.9.3 faker
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
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
from demo.common_utils import add_audit_columns

# Get catalog from widget
dbutils.widgets.text("catalog", "demo_dev", "Catalog Name")
dbutils.widgets.text("num_records", "1000", "Number of Sample Records")

catalog = dbutils.widgets.get("catalog")
num_records = int(dbutils.widgets.get("num_records"))

print(f"Generating {num_records} sample records for catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data

# COMMAND ----------

# Generate sample products
product_categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports"]
product_names = {
    "Electronics": ["Laptop", "Phone", "Tablet", "Headphones", "Camera"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Shoes", "Hat"],
    "Books": ["Fiction Novel", "Textbook", "Magazine", "Comic Book", "Biography"],
    "Home & Garden": ["Chair", "Table", "Lamp", "Rug", "Plant"],
    "Sports": ["Basketball", "Soccer Ball", "Tennis Racket", "Yoga Mat", "Dumbbell"]
}

products_data = []
for i in range(100):
    category = random.choice(product_categories)
    name = random.choice(product_names[category])
    products_data.append({
        "product_id": f"PROD-{i+1:04d}",
        "name": f"{name} {i+1}",
        "category": category,
        "list_price": str(round(random.uniform(10, 500), 2)),
        "currency_code": "USD",
        "is_active": random.choice(["true", "false", "1", "0", "True", "False"])
    })

products_df = spark.createDataFrame(products_data)
products_df = add_audit_columns(products_df, source="sample_generator")

print(f"Generated {products_df.count()} products")

# COMMAND ----------

# Generate sample customers
countries = ["US", "GB", "DE", "FR", "IN", "JP"]
first_names = ["John", "Jane", "Mike", "Sarah", "David", "Emma", "Chris", "Lisa"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]

customers_data = []
for i in range(min(500, num_records)):
    created_date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d %H:%M:%S')
    customers_data.append({
        "customer_id": f"CUST-{i+1:06d}",
        "first_name": random.choice(first_names),
        "last_name": random.choice(last_names),
        "email": f"customer{i+1}@example.com",
        "phone": f"+1-555-{random.randint(1000, 9999)}",
        "country_code": random.choice(countries),
        "created_at": created_date,
        "updated_at": created_date
    })

customers_df = spark.createDataFrame(customers_data)
customers_df = add_audit_columns(customers_df, source="sample_generator")

print(f"Generated {customers_df.count()} customers")

# COMMAND ----------

# Generate sample orders
channels = ["WEB", "STORE", "APP", "web", "store", "app"]
statuses = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "pending", "shipped"]
currencies = ["USD", "EUR", "GBP"]

orders_data = []
for i in range(num_records):
    order_date = datetime.now() - timedelta(days=random.randint(0, 180))
    orders_data.append({
        "order_id": f"ORD-{i+1:08d}",
        "customer_id": f"CUST-{random.randint(1, min(500, num_records)):06d}",
        "order_timestamp": order_date.strftime('%Y-%m-%d %H:%M:%S'),
        "status": random.choice(statuses),
        "currency_code": random.choice(currencies),
        "total_amount": str(round(random.uniform(20, 1000), 2)),
        "channel": random.choice(channels)
    })

orders_df = spark.createDataFrame(orders_data)
orders_df = add_audit_columns(orders_df, source="sample_generator")

print(f"Generated {orders_df.count()} orders")

# COMMAND ----------

# Generate sample order items
order_items_data = []
item_counter = 1

for i in range(num_records):
    num_items = random.randint(1, 5)
    for j in range(num_items):
        order_items_data.append({
            "order_item_id": f"ITEM-{item_counter:08d}",
            "order_id": f"ORD-{i+1:08d}",
            "product_id": f"PROD-{random.randint(1, 100):04d}",
            "quantity": str(random.randint(1, 10)),
            "unit_price": str(round(random.uniform(10, 200), 2)),
            "tax_amount": str(round(random.uniform(1, 20), 2)),
            "discount_amount": str(round(random.uniform(0, 30), 2))
        })
        item_counter += 1

order_items_df = spark.createDataFrame(order_items_data)
order_items_df = add_audit_columns(order_items_df, source="sample_generator")

print(f"Generated {order_items_df.count()} order items")

# COMMAND ----------

# Generate sample payments
payment_statuses = ["COMPLETED", "PENDING", "FAILED", "completed", "pending"]

payments_data = []
for i in range(num_records):
    paid_date = datetime.now() - timedelta(days=random.randint(0, 180))
    payments_data.append({
        "payment_id": f"PAY-{i+1:08d}",
        "order_id": f"ORD-{i+1:08d}",
        "amount": str(round(random.uniform(20, 1000), 2)),
        "currency_code": random.choice(currencies),
        "status": random.choice(payment_statuses),
        "paid_at": paid_date.strftime('%Y-%m-%d %H:%M:%S')
    })

payments_df = spark.createDataFrame(payments_data)
payments_df = add_audit_columns(payments_df, source="sample_generator")

print(f"Generated {payments_df.count()} payments")

# COMMAND ----------

# Generate sample web events
event_types = ["VIEW", "ADD_TO_CART", "PURCHASE", "view", "add_to_cart"]
sources = ["WEB", "APP", "web", "app"]

events_data = []
for i in range(num_records * 3):
    event_date = datetime.now() - timedelta(days=random.randint(0, 90))
    events_data.append({
        "event_id": f"EVT-{i+1:08d}",
        "customer_id": f"CUST-{random.randint(1, min(500, num_records)):06d}" if random.random() > 0.2 else None,
        "session_id": f"SESS-{random.randint(1, num_records):06d}",
        "event_type": random.choice(event_types),
        "event_timestamp": event_date.strftime('%Y-%m-%d %H:%M:%S'),
        "source": random.choice(sources)
    })

events_df = spark.createDataFrame(events_data)
events_df = add_audit_columns(events_df, source="sample_generator")

print(f"Generated {events_df.count()} web events")

# COMMAND ----------

# Generate sample inventory
locations = ["WH-01", "WH-02", "WH-03", "STORE-01", "STORE-02"]

inventory_data = []
for product_id in range(1, 101):
    for location in locations:
        inventory_data.append({
            "product_id": f"PROD-{product_id:04d}",
            "location_id": location,
            "quantity_on_hand": str(random.randint(0, 500)),
            "updated_at": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S')
        })

inventory_df = spark.createDataFrame(inventory_data)
inventory_df = add_audit_columns(inventory_df, source="sample_generator")

print(f"Generated {inventory_df.count()} inventory records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Sample Data into Bronze Tables

# COMMAND ----------

# Load products - select columns in exact table order
products_df_ordered = products_df.select(
    "product_id", "name", "category", "list_price", 
    "currency_code", "is_active", "_ingest_time"
)
products_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.products_raw")
print(f"✓ Loaded products into {catalog}.bronze.products_raw")

# COMMAND ----------

# Load customers - select columns in exact table order
customers_df_ordered = customers_df.select(
    "customer_id", "first_name", "last_name", "email", "phone",
    "country_code", "created_at", "updated_at", "_ingest_time", "_source"
)
customers_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.customers_raw")
print(f"✓ Loaded customers into {catalog}.bronze.customers_raw")

# COMMAND ----------

# Load orders - select columns in exact table order
orders_df_ordered = orders_df.select(
    "order_id", "customer_id", "order_timestamp", "status",
    "currency_code", "total_amount", "channel", 
    "_ingest_time", "_file_name", "_source"
)
orders_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.orders_raw")
print(f"✓ Loaded orders into {catalog}.bronze.orders_raw")

# COMMAND ----------

# Load order items - select columns in exact table order
order_items_df_ordered = order_items_df.select(
    "order_item_id", "order_id", "product_id", "quantity",
    "unit_price", "tax_amount", "discount_amount",
    "_ingest_time", "_file_name", "_source"
)
order_items_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.order_items_raw")
print(f"✓ Loaded order items into {catalog}.bronze.order_items_raw")

# COMMAND ----------

# Load payments - select columns in exact table order
payments_df_ordered = payments_df.select(
    "payment_id", "order_id", "amount", "currency_code",
    "status", "paid_at", "_ingest_time"
)
payments_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.payments_raw")
print(f"✓ Loaded payments into {catalog}.bronze.payments_raw")

# COMMAND ----------

# Load web events - select columns in exact table order
events_df_ordered = events_df.select(
    "event_id", "customer_id", "session_id", "event_type",
    "event_timestamp", "source", "_ingest_time"
)
events_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.web_events_raw")
print(f"✓ Loaded web events into {catalog}.bronze.web_events_raw")

# COMMAND ----------

# Load inventory - select columns in exact table order
inventory_df_ordered = inventory_df.select(
    "product_id", "location_id", "quantity_on_hand",
    "updated_at", "_ingest_time"
)
inventory_df_ordered.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.inventory_raw")
print(f"✓ Loaded inventory into {catalog}.bronze.inventory_raw")

# COMMAND ----------

# Display summary
print("\n" + "="*80)
print("SAMPLE DATA LOAD SUMMARY")
print("="*80)
print(f"Products: {spark.table(f'{catalog}.bronze.products_raw').count()}")
print(f"Customers: {spark.table(f'{catalog}.bronze.customers_raw').count()}")
print(f"Orders: {spark.table(f'{catalog}.bronze.orders_raw').count()}")
print(f"Order Items: {spark.table(f'{catalog}.bronze.order_items_raw').count()}")
print(f"Payments: {spark.table(f'{catalog}.bronze.payments_raw').count()}")
print(f"Web Events: {spark.table(f'{catalog}.bronze.web_events_raw').count()}")
print(f"Inventory: {spark.table(f'{catalog}.bronze.inventory_raw').count()}")
print("="*80)

# COMMAND ----------

print("✅ Sample data generation and load completed successfully!")

