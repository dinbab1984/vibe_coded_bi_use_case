"""Dashboard JSON generator using Databricks SDK"""

import json
from typing import Dict, List


def create_sales_overview_dashboard(catalog: str, warehouse_id: str = "c0f912968139b56f") -> Dict:
    """Create Sales Overview dashboard JSON"""
    
    dashboard = {
        "displayName": "Sales Overview Dashboard",
        "warehouseId": warehouse_id,
        "datasets": [
            {
                "name": "sales_metrics_ds",
                "displayName": "Sales Metrics",
                "query": f"SELECT * FROM {catalog}.gold.sales_dashboard_metrics"
            }
        ],
        "pages": [
            {
                "name": "sales_overview_page",
                "displayName": "Sales Overview",
                "layout": [
                    # Date Range Filter
                    {
                        "widget": {
                            "name": "filter_date_range",
                            "textbox_spec": "metric_date"
                        },
                        "position": {"x": 0, "y": 0, "width": 2, "height": 1}
                    },
                    # Total Sales Counter
                    {
                        "widget": {
                            "name": "counter_total_sales",
                            "queries": [
                                {
                                    "name": "main_query",
                        "query": {
                            "datasetName": "sales_metrics_ds",
                            "fields": [
                                {"name": "sum(total_sales)", "expression": "SUM(`total_sales`)"}
                            ],
                            "disaggregated": false
                        }
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "counter",
                                "encodings": {
                                    "value": {
                                        "fieldName": "sum(total_sales)",
                                        "displayName": "Total Sales"
                                    }
                                },
                                "frame": {"showTitle": true, "title": "Total Sales"}
                            }
                        },
                        "position": {"x": 2, "y": 0, "width": 2, "height": 1}
                    },
                    # Total Orders Counter
                    {
                        "widget": {
                            "name": "counter_total_orders",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "sales_metrics_ds",
                                        "fields": [
                                            {"name": "sum(total_orders)", "expression": "SUM(`total_orders`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widget_type": "counter",
                                "encodings": {
                                    "value": {
                                        "field_name": "sum(total_orders)",
                                        "display_name": "Total Orders"
                                    }
                                },
                                "frame": {"show_title": True, "title": "Total Orders"}
                            }
                        },
                        "position": {"x": 4, "y": 0, "width": 2, "height": 1}
                    },
                    # AOV Trend Line Chart
                    {
                        "widget": {
                            "name": "line_aov_trend",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "sales_metrics_ds",
                                        "fields": [
                                            {"name": "metric_date", "expression": "`metric_date`"},
                                            {"name": "avg_order_value", "expression": "AVG(`avg_order_value`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "line",
                                "encodings": {
                                    "x": {
                                        "field_name": "metric_date",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Date"}
                                    },
                                    "y": {
                                        "field_name": "avg_order_value",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "AOV"}
                                    }
                                },
                                "frame": {"show_title": True, "title": "Average Order Value Trend"}
                            }
                        },
                        "position": {"x": 0, "y": 1, "width": 6, "height": 3}
                    },
                    # Orders by Channel Line Chart
                    {
                        "widget": {
                            "name": "line_orders_by_channel",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "sales_metrics_ds",
                                        "fields": [
                                            {"name": "metric_date", "expression": "`metric_date`"},
                                            {"name": "orders_web", "expression": "SUM(`orders_web`)"},
                                            {"name": "orders_store", "expression": "SUM(`orders_store`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "line",
                                "frame": {"show_title": True, "title": "Orders by Channel"}
                            }
                        },
                        "position": {"x": 0, "y": 4, "width": 6, "height": 3}
                    }
                ]
            }
        ]
    }
    
    return dashboard


def create_product_performance_dashboard(catalog: str, warehouse_id: str = "c0f912968139b56f") -> Dict:
    """Create Product Performance dashboard JSON"""
    
    dashboard = {
        "display_name": "Product Performance Dashboard",
        "warehouse_id": warehouse_id,
        "datasets": [
            {
                "name": "product_sales_ds",
                "display_name": "Product Sales",
                "query": f"""
                    SELECT 
                        p.product_id,
                        p.name as product_name,
                        p.category,
                        ps.month,
                        ps.units_sold,
                        ps.revenue,
                        ps.returns_rate
                    FROM {catalog}.gold.product_sales_by_month ps
                    JOIN {catalog}.silver.dim_product p ON ps.product_id = p.product_id
                """
            }
        ],
        "pages": [
            {
                "name": "product_page",
                "display_name": "Product Performance",
                "layout": [
                    # Top Products by Revenue Bar Chart
                    {
                        "widget": {
                            "name": "bar_top_products",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "product_sales_ds",
                                        "fields": [
                                            {"name": "product_name", "expression": "`product_name`"},
                                            {"name": "sum(revenue)", "expression": "SUM(`revenue`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "bar",
                                "encodings": {
                                    "x": {
                                        "field_name": "sum(revenue)",
                                        "scale": {"type": "quantitative"}
                                    },
                                    "y": {
                                        "field_name": "product_name",
                                        "scale": {"type": "categorical"}
                                    }
                                },
                                "frame": {"show_title": True, "title": "Top Products by Revenue"}
                            }
                        },
                        "position": {"x": 0, "y": 0, "width": 6, "height": 4}
                    },
                    # Returns Rate Heatmap
                    {
                        "widget": {
                            "name": "heatmap_returns",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "product_sales_ds",
                                        "fields": [
                                            {"name": "month", "expression": "`month`"},
                                            {"name": "product_name", "expression": "`product_name`"},
                                            {"name": "avg(returns_rate)", "expression": "AVG(`returns_rate`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "heatmap",
                                "frame": {"show_title": True, "title": "Returns Rate by Product and Month"}
                            }
                        },
                        "position": {"x": 0, "y": 4, "width": 6, "height": 4}
                    }
                ]
            }
        ]
    }
    
    return dashboard


def create_customer_360_dashboard(catalog: str, warehouse_id: str = "c0f912968139b56f") -> Dict:
    """Create Customer 360 dashboard JSON"""
    
    dashboard = {
        "display_name": "Customer 360 & AI Features Dashboard",
        "warehouse_id": warehouse_id,
        "datasets": [
            {
                "name": "clv_features_ds",
                "display_name": "CLV Features",
                "query": f"""
                    SELECT 
                        clv.*,
                        c.country_code
                    FROM {catalog}.gold.customer_lifetime_value_features clv
                    JOIN {catalog}.silver.dim_customer c 
                        ON clv.customer_id = c.customer_id 
                        AND c.is_current = true
                """
            }
        ],
        "pages": [
            {
                "name": "customer_page",
                "display_name": "Customer 360",
                "layout": [
                    # Churn Risk Scatter Plot
                    {
                        "widget": {
                            "name": "scatter_churn_risk",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "clv_features_ds",
                                        "fields": [
                                            {"name": "recency_days", "expression": "`recency_days`"},
                                            {"name": "total_spent", "expression": "`total_spent`"},
                                            {"name": "churn_risk_score", "expression": "`churn_risk_score`"}
                                        ],
                                        "disaggregated": True
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "scatter",
                                "encodings": {
                                    "x": {
                                        "field_name": "recency_days",
                                        "scale": {"type": "quantitative"}
                                    },
                                    "y": {
                                        "field_name": "total_spent",
                                        "scale": {"type": "quantitative"}
                                    },
                                    "color": {
                                        "field_name": "churn_risk_score",
                                        "scale": {"type": "quantitative"}
                                    }
                                },
                                "frame": {"show_title": True, "title": "Churn Risk Analysis"}
                            }
                        },
                        "position": {"x": 0, "y": 0, "width": 6, "height": 4}
                    },
                    # Recency Histogram
                    {
                        "widget": {
                            "name": "histogram_recency",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "clv_features_ds",
                                        "fields": [
                                            {"name": "recency_days", "expression": "`recency_days`"}
                                        ],
                                        "disaggregated": True
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "histogram",
                                "encodings": {
                                    "x": {
                                        "field_name": "recency_days",
                                        "scale": {"type": "quantitative"},
                                        "bin": {"max_bins": 30}
                                    }
                                },
                                "frame": {"show_title": True, "title": "Customer Recency Distribution"}
                            }
                        },
                        "position": {"x": 0, "y": 4, "width": 6, "height": 3}
                    }
                ]
            }
        ]
    }
    
    return dashboard


def create_cohort_retention_dashboard(catalog: str, warehouse_id: str = "c0f912968139b56f") -> Dict:
    """Create Cohort Retention dashboard JSON"""
    
    dashboard = {
        "display_name": "Cohort Retention Dashboard",
        "warehouse_id": warehouse_id,
        "datasets": [
            {
                "name": "cohort_retention_ds",
                "display_name": "Cohort Retention",
                "query": f"SELECT * FROM {catalog}.gold.cohort_retention"
            }
        ],
        "pages": [
            {
                "name": "cohort_page",
                "display_name": "Cohort Retention",
                "layout": [
                    # Cohort Retention Heatmap
                    {
                        "widget": {
                            "name": "heatmap_cohort_retention",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "dataset_name": "cohort_retention_ds",
                                        "fields": [
                                            {"name": "cohort_month", "expression": "`cohort_month`"},
                                            {"name": "month_offset", "expression": "`month_offset`"},
                                            {"name": "retention_rate", "expression": "`retention_rate`"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widget_type": "heatmap",
                                "encodings": {
                                    "x": {
                                        "field_name": "month_offset",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Months Since First Order"}
                                    },
                                    "y": {
                                        "field_name": "cohort_month",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Cohort Month"}
                                    },
                                    "color": {
                                        "field_name": "retention_rate",
                                        "scale": {"type": "quantitative"},
                                        "legend": {"title": "Retention %"}
                                    }
                                },
                                "frame": {"show_title": True, "title": "Cohort Retention Matrix"}
                            }
                        },
                        "position": {"x": 0, "y": 0, "width": 6, "height": 6}
                    }
                ]
            }
        ]
    }
    
    return dashboard


def generate_all_dashboards(catalog: str = "demo_dev") -> None:
    """Generate all dashboard JSON files"""
    
    dashboards = {
        "sales_overview": create_sales_overview_dashboard(catalog),
        "product_performance": create_product_performance_dashboard(catalog),
        "customer_360": create_customer_360_dashboard(catalog),
        "cohort_retention": create_cohort_retention_dashboard(catalog)
    }
    
    import os
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "resources", "dashboards")
    os.makedirs(output_dir, exist_ok=True)
    
    for name, dashboard_def in dashboards.items():
        output_file = os.path.join(output_dir, f"{name}_dashboard.json")
        with open(output_file, 'w') as f:
            json.dump(dashboard_def, f, indent=2)
        print(f"Generated: {output_file}")


if __name__ == "__main__":
    generate_all_dashboards()

