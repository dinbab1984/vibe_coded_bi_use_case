"""Generate Databricks Dashboard JSONs following the official format"""

import json
import os

CATALOG = "demo_dev"

def create_sales_overview_dashboard():
    """Create Sales Overview Dashboard JSON"""
    return {
        "datasets": [
            {
                "name": "sales_metrics",
                "displayName": "Sales Metrics",
                "queryLines": [
                    f"SELECT\n",
                    f"  metric_date,\n",
                    f"  total_sales,\n",
                    f"  total_orders,\n",
                    f"  avg_order_value,\n",
                    f"  orders_web,\n",
                    f"  orders_store\n",
                    f"FROM {CATALOG}.gold.sales_dashboard_metrics\n",
                    f"ORDER BY metric_date DESC"
                ]
            }
        ],
        "pages": [
            {
                "name": "sales_page",
                "displayName": "Sales Overview",
                "layout": [
                    # Total Sales Counter
                    {
                        "position": {"x": 0, "y": 0, "width": 2, "height": 2},
                        "widget": {
                            "name": "wCounterTotalSales",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "sales_metrics",
                                        "fields": [
                                            {"name": "sum(total_sales)", "expression": "SUM(`total_sales`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "counter",
                                "encodings": {
                                    "value": {
                                        "fieldName": "sum(total_sales)",
                                        "displayName": "Total Sales",
                                        "format": {
                                            "type": "number-currency",
                                            "currencyCode": "USD",
                                            "abbreviation": "compact",
                                            "decimalPlaces": {"type": "max", "places": 2}
                                        },
                                        "style": {"color": "#00A972", "bold": True}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Total Sales"}
                            }
                        }
                    },
                    # Total Orders Counter
                    {
                        "position": {"x": 2, "y": 0, "width": 2, "height": 2},
                        "widget": {
                            "name": "wCounterTotalOrders",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "sales_metrics",
                                        "fields": [
                                            {"name": "sum(total_orders)", "expression": "SUM(`total_orders`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "counter",
                                "encodings": {
                                    "value": {
                                        "fieldName": "sum(total_orders)",
                                        "displayName": "Total Orders",
                                        "format": {
                                            "type": "number",
                                            "decimalPlaces": {"type": "max", "places": 0}
                                        },
                                        "style": {"color": "#077A9D", "bold": True}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Total Orders"}
                            }
                        }
                    },
                    # AOV Trend Line Chart
                    {
                        "position": {"x": 0, "y": 2, "width": 6, "height": 4},
                        "widget": {
                            "name": "wLineAOV",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "sales_metrics",
                                        "fields": [
                                            {"name": "metric_date", "expression": "`metric_date`"},
                                            {"name": "avg(avg_order_value)", "expression": "AVG(`avg_order_value`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "line",
                                "encodings": {
                                    "x": {
                                        "fieldName": "metric_date",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Date"}
                                    },
                                    "y": {
                                        "fieldName": "avg(avg_order_value)",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Average Order Value"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "AOV Trend"}
                            }
                        }
                    },
                    # Orders by Channel Line Chart
                    {
                        "position": {"x": 0, "y": 6, "width": 6, "height": 4},
                        "widget": {
                            "name": "wLineOrdersByChannel",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "sales_metrics",
                                        "fields": [
                                            {"name": "metric_date", "expression": "`metric_date`"},
                                            {"name": "sum(orders_web)", "expression": "SUM(`orders_web`)"},
                                            {"name": "sum(orders_store)", "expression": "SUM(`orders_store`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "line",
                                "encodings": {
                                    "x": {
                                        "fieldName": "metric_date",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Date"}
                                    },
                                    "y": {
                                        "scale": {"type": "quantitative"},
                                        "fields": [
                                            {"fieldName": "sum(orders_web)", "displayName": "Web Orders"},
                                            {"fieldName": "sum(orders_store)", "displayName": "Store Orders"}
                                        ],
                                        "axis": {"title": "Orders"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Orders by Channel"}
                            }
                        }
                    }
                ]
            }
        ]
    }


def create_product_performance_dashboard():
    """Create Product Performance Dashboard JSON"""
    return {
        "datasets": [
            {
                "name": "product_sales",
                "displayName": "Product Sales",
                "queryLines": [
                    f"SELECT\n",
                    f"  ps.month,\n",
                    f"  ps.product_id,\n",
                    f"  p.name as product_name,\n",
                    f"  p.category,\n",
                    f"  ps.units_sold,\n",
                    f"  ps.revenue,\n",
                    f"  ps.returns_rate\n",
                    f"FROM {CATALOG}.gold.product_sales_by_month ps\n",
                    f"JOIN {CATALOG}.silver.dim_product p ON ps.product_id = p.product_id\n",
                    f"ORDER BY ps.revenue DESC"
                ]
            }
        ],
        "pages": [
            {
                "name": "product_page",
                "displayName": "Product Performance",
                "layout": [
                    # Category Filter (V6)
                    {
                        "position": {"x": 0, "y": 0, "width": 2, "height": 1},
                        "widget": {
                            "name": "wFilterCategory",
                            "queries": [
                                {
                                    "name": "filter_query",
                                    "query": {
                                        "datasetName": "product_sales",
                                        "fields": [
                                            {"name": "category", "expression": "`category`"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "filter-single-select",
                                "encodings": {
                                    "fields": [
                                        {"fieldName": "category", "queryName": "filter_query"}
                                    ]
                                },
                                "frame": {"showTitle": True, "title": "Category Filter"}
                            }
                        }
                    },
                    # Top Products Bar Chart (V7)
                    {
                        "position": {"x": 0, "y": 1, "width": 6, "height": 5},
                        "widget": {
                            "name": "wBarTopProducts",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "product_sales",
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
                                "widgetType": "bar",
                                "encodings": {
                                    "x": {
                                        "fieldName": "sum(revenue)",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Revenue"}
                                    },
                                    "y": {
                                        "fieldName": "product_name",
                                        "scale": {"type": "categorical"},
                                        "axis": {"title": "Product"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Top Products by Revenue"}
                            }
                        }
                    },
                    # Returns Rate Heatmap (V8)
                    {
                        "position": {"x": 0, "y": 6, "width": 6, "height": 5},
                        "widget": {
                            "name": "wHeatmapReturns",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "product_sales",
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
                                "widgetType": "heatmap",
                                "encodings": {
                                    "x": {
                                        "fieldName": "month",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Month"}
                                    },
                                    "y": {
                                        "fieldName": "product_name",
                                        "scale": {"type": "categorical"},
                                        "axis": {"title": "Product"}
                                    },
                                    "color": {
                                        "fieldName": "avg(returns_rate)",
                                        "scale": {"type": "quantitative"},
                                        "legend": {"title": "Returns Rate %"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Returns Rate by Product and Month"}
                            }
                        }
                    }
                ]
            }
        ]
    }


def create_customer_360_dashboard():
    """Create Customer 360 Dashboard JSON"""
    return {
        "datasets": [
            {
                "name": "customer_clv",
                "displayName": "Customer CLV",
                "queryLines": [
                    f"SELECT\n",
                    f"  clv.customer_id,\n",
                    f"  clv.total_spent,\n",
                    f"  clv.order_count,\n",
                    f"  clv.recency_days,\n",
                    f"  clv.churn_risk_score,\n",
                    f"  c.country_code\n",
                    f"FROM {CATALOG}.gold.customer_lifetime_value_features clv\n",
                    f"JOIN {CATALOG}.silver.dim_customer c\n",
                    f"  ON clv.customer_id = c.customer_id\n",
                    f"  AND c.is_current = true"
                ]
            }
        ],
        "pages": [
            {
                "name": "customer_page",
                "displayName": "Customer 360",
                "layout": [
                    # Country Filter (V9)
                    {
                        "position": {"x": 0, "y": 0, "width": 2, "height": 1},
                        "widget": {
                            "name": "wFilterCountry",
                            "queries": [
                                {
                                    "name": "filter_query",
                                    "query": {
                                        "datasetName": "customer_clv",
                                        "fields": [
                                            {"name": "country_code", "expression": "`country_code`"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "filter-multi-select",
                                "encodings": {
                                    "fields": [
                                        {"fieldName": "country_code", "queryName": "filter_query"}
                                    ]
                                },
                                "frame": {"showTitle": True, "title": "Country Filter"}
                            }
                        }
                    },
                    # Churn Risk Scatter Plot (V10)
                    {
                        "position": {"x": 0, "y": 1, "width": 6, "height": 5},
                        "widget": {
                            "name": "wScatterChurnRisk",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "customer_clv",
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
                                "widgetType": "scatter",
                                "encodings": {
                                    "x": {
                                        "fieldName": "recency_days",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Days Since Last Order"}
                                    },
                                    "y": {
                                        "fieldName": "total_spent",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Total Spent"}
                                    },
                                    "color": {
                                        "fieldName": "churn_risk_score",
                                        "scale": {"type": "quantitative"},
                                        "legend": {"title": "Churn Risk"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Churn Risk Analysis"}
                            }
                        }
                    },
                    # Recency Distribution (V11)
                    {
                        "position": {"x": 0, "y": 6, "width": 6, "height": 4},
                        "widget": {
                            "name": "wBarRecency",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "customer_clv",
                                        "fields": [
                                            {"name": "recency_days", "expression": "`recency_days`"},
                                            {"name": "count", "expression": "COUNT(*)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "bar",
                                "encodings": {
                                    "x": {
                                        "fieldName": "recency_days",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Days Since Last Order"},
                                        "bin": {"maxBins": 20}
                                    },
                                    "y": {
                                        "fieldName": "count",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Number of Customers"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Customer Recency Distribution"}
                            }
                        }
                    }
                ]
            }
        ]
    }


def create_cohort_retention_dashboard():
    """Create Cohort Retention Dashboard JSON"""
    return {
        "datasets": [
            {
                "name": "cohort_data",
                "displayName": "Cohort Retention",
                "queryLines": [
                    f"SELECT\n",
                    f"  DATE_FORMAT(cohort_month, 'yyyy-MM') as cohort,\n",
                    f"  CAST(month_offset AS STRING) as month_offset,\n",
                    f"  active_customers,\n",
                    f"  retention_rate * 100 as retention_rate\n",
                    f"FROM {CATALOG}.gold.cohort_retention\n",
                    f"ORDER BY cohort_month, month_offset"
                ]
            }
        ],
        "pages": [
            {
                "name": "cohort_page",
                "displayName": "Cohort Retention",
                "layout": [
                    # Cohort Heatmap
                    {
                        "position": {"x": 0, "y": 0, "width": 6, "height": 6},
                        "widget": {
                            "name": "wHeatmapCohort",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "cohort_data",
                                        "fields": [
                                            {"name": "cohort", "expression": "`cohort`"},
                                            {"name": "month_offset", "expression": "`month_offset`"},
                                            {"name": "retention_rate", "expression": "`retention_rate`"}
                                        ],
                                        "disaggregated": True
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "heatmap",
                                "encodings": {
                                    "x": {
                                        "fieldName": "month_offset",
                                        "scale": {"type": "categorical"},
                                        "axis": {"title": "Months Since First Order"}
                                    },
                                    "y": {
                                        "fieldName": "cohort",
                                        "scale": {"type": "categorical"},
                                        "axis": {"title": "Cohort"}
                                    },
                                    "color": {
                                        "fieldName": "retention_rate",
                                        "scale": {"type": "quantitative"},
                                        "legend": {"title": "Retention %"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Cohort Retention Matrix"}
                            }
                        }
                    }
                ]
            }
        ]
    }


def create_executive_summary_dashboard():
    """Create Executive Summary Dashboard JSON"""
    return {
        "datasets": [
            {
                "name": "monthly_sales",
                "displayName": "Monthly Sales Summary",
                "queryLines": [
                    f"WITH monthly_data AS (\n",
                    f"  SELECT\n",
                    f"    DATE_TRUNC('MONTH', metric_date) as month,\n",
                    f"    SUM(total_sales) as total_sales,\n",
                    f"    SUM(total_orders) as total_orders,\n",
                    f"    SUM(orders_web) as orders_web,\n",
                    f"    SUM(orders_store) as orders_store\n",
                    f"  FROM {CATALOG}.gold.sales_dashboard_metrics\n",
                    f"  WHERE metric_date >= DATE_SUB(CURRENT_DATE(), 365)\n",
                    f"  GROUP BY DATE_TRUNC('MONTH', metric_date)\n",
                    f")\n",
                    f"SELECT\n",
                    f"  month,\n",
                    f"  total_sales,\n",
                    f"  total_orders,\n",
                    f"  orders_web,\n",
                    f"  orders_store,\n",
                    f"  'WEB' as channel_web,\n",
                    f"  'STORE' as channel_store\n",
                    f"FROM monthly_data\n",
                    f"ORDER BY month DESC\n",
                    f"LIMIT 12"
                ]
            }
        ],
        "pages": [
            {
                "name": "exec_page",
                "displayName": "Executive Summary",
                "layout": [
                    # 12-Month Sales Trend (V13)
                    {
                        "position": {"x": 0, "y": 0, "width": 6, "height": 4},
                        "widget": {
                            "name": "wLine12MonthSales",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "monthly_sales",
                                        "fields": [
                                            {"name": "month", "expression": "`month`"},
                                            {"name": "sum(total_sales)", "expression": "SUM(`total_sales`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "line",
                                "encodings": {
                                    "x": {
                                        "fieldName": "month",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Month"}
                                    },
                                    "y": {
                                        "fieldName": "sum(total_sales)",
                                        "scale": {"type": "quantitative"},
                                        "axis": {"title": "Total Sales"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "12-Month Sales Trend"}
                            }
                        }
                    },
                    # Orders by Channel - 12 Months (V14)
                    {
                        "position": {"x": 0, "y": 4, "width": 6, "height": 4},
                        "widget": {
                            "name": "wLineOrdersByChannel12M",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "monthly_sales",
                                        "fields": [
                                            {"name": "month", "expression": "`month`"},
                                            {"name": "sum(orders_web)", "expression": "SUM(`orders_web`)"},
                                            {"name": "sum(orders_store)", "expression": "SUM(`orders_store`)"}
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "line",
                                "encodings": {
                                    "x": {
                                        "fieldName": "month",
                                        "scale": {"type": "temporal"},
                                        "axis": {"title": "Month"}
                                    },
                                    "y": {
                                        "scale": {"type": "quantitative"},
                                        "fields": [
                                            {"fieldName": "sum(orders_web)", "displayName": "Web Orders"},
                                            {"fieldName": "sum(orders_store)", "displayName": "Store Orders"}
                                        ],
                                        "axis": {"title": "Orders"}
                                    }
                                },
                                "frame": {"showTitle": True, "title": "Orders by Channel - Last 12 Months"}
                            }
                        }
                    }
                ]
            }
        ]
    }


def main():
    """Generate all dashboard JSONs"""
    output_dir = "resources/dashboards"
    os.makedirs(output_dir, exist_ok=True)
    
    dashboards = {
        "sales_overview": create_sales_overview_dashboard(),
        "product_performance": create_product_performance_dashboard(),
        "customer_360": create_customer_360_dashboard(),
        "cohort_retention": create_cohort_retention_dashboard(),
        "executive_summary": create_executive_summary_dashboard()
    }
    
    for name, dashboard in dashboards.items():
        output_file = os.path.join(output_dir, f"{name}_dashboard.json")
        with open(output_file, 'w') as f:
            json.dump(dashboard, f, indent=2)
        print(f"✓ Generated: {output_file}")
    
    print(f"\n✅ Generated {len(dashboards)} dashboard JSON files")


if __name__ == "__main__":
    main()

