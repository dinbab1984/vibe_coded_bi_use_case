#!/bin/bash
# Deploy dashboards using Databricks CLI

WAREHOUSE_ID="c0f912968139b56f"
PARENT_PATH="/Workspace/Shared/Dashboards"

echo "Deploying dashboards..."

# Create Sales Overview Dashboard
echo "Creating Sales Overview Dashboard..."
databricks dashboards create \
  --display-name "Sales Overview - dev" \
  --warehouse-id "$WAREHOUSE_ID" \
  --serialized-dashboard "$(cat resources/dashboards/sales_overview_dashboard.json)" \
  --parent-path "$PARENT_PATH"

# Create Product Performance Dashboard
echo "Creating Product Performance Dashboard..."
databricks dashboards create \
  --display-name "Product Performance - dev" \
  --warehouse-id "$WAREHOUSE_ID" \
  --serialized-dashboard "$(cat resources/dashboards/product_performance_dashboard.json)" \
  --parent-path "$PARENT_PATH"

# Create Customer 360 Dashboard
echo "Creating Customer 360 Dashboard..."
databricks dashboards create \
  --display-name "Customer 360 & AI Features - dev" \
  --warehouse-id "$WAREHOUSE_ID" \
  --serialized-dashboard "$(cat resources/dashboards/customer_360_dashboard.json)" \
  --parent-path "$PARENT_PATH"

# Create Cohort Retention Dashboard
echo "Creating Cohort Retention Dashboard..."
databricks dashboards create \
  --display-name "Cohort Retention - dev" \
  --warehouse-id "$WAREHOUSE_ID" \
  --serialized-dashboard "$(cat resources/dashboards/cohort_retention_dashboard.json)" \
  --parent-path "$PARENT_PATH"

echo "✅ All dashboards deployed!"





