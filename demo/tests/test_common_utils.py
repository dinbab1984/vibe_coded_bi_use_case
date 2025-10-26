"""Unit tests for common_utils module"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from demo.common_utils import (
    safe_cast_column,
    add_audit_columns,
    deduplicate_by_key,
    normalize_string_column,
    create_date_dimension,
    get_schema_from_spec
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_common_utils") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


def test_safe_cast_column_decimal(spark):
    """Test safe casting to DECIMAL type"""
    data = [("100.50",), ("200.75",), ("300.00",)]
    df = spark.createDataFrame(data, ["amount"])
    
    result_df = safe_cast_column(df, "amount", "DECIMAL(12,2)")
    
    result = result_df.collect()
    assert result[0]["amount"] == 100.50
    assert result[1]["amount"] == 200.75
    assert result_df.schema["amount"].dataType.typeName() == "decimal"


def test_safe_cast_column_int(spark):
    """Test safe casting to INT type"""
    data = [("10",), ("20",), ("30",)]
    df = spark.createDataFrame(data, ["quantity"])
    
    result_df = safe_cast_column(df, "quantity", "INT")
    
    result = result_df.collect()
    assert result[0]["quantity"] == 10
    assert result[1]["quantity"] == 20
    assert result_df.schema["quantity"].dataType.typeName() == "integer"


def test_safe_cast_column_boolean(spark):
    """Test safe casting to BOOLEAN type"""
    data = [("true",), ("false",), ("1",), ("0",), ("yes",), ("no",)]
    df = spark.createDataFrame(data, ["is_active"])
    
    result_df = safe_cast_column(df, "is_active", "BOOLEAN")
    
    result = result_df.collect()
    assert result[0]["is_active"] == True
    assert result[1]["is_active"] == False
    assert result[2]["is_active"] == True
    assert result[3]["is_active"] == False
    assert result[4]["is_active"] == True
    assert result[5]["is_active"] == False


def test_safe_cast_column_timestamp(spark):
    """Test safe casting to TIMESTAMP type"""
    data = [("2024-01-15 10:30:00",), ("2024-02-20 15:45:00",)]
    df = spark.createDataFrame(data, ["event_time"])
    
    result_df = safe_cast_column(df, "event_time", "TIMESTAMP")
    
    result = result_df.collect()
    assert result[0]["event_time"] is not None
    assert result_df.schema["event_time"].dataType.typeName() == "timestamp"


def test_safe_cast_column_nonexistent(spark):
    """Test safe casting when column doesn't exist"""
    data = [("100",)]
    df = spark.createDataFrame(data, ["amount"])
    
    result_df = safe_cast_column(df, "nonexistent_column", "INT")
    
    # Should return original df unchanged
    assert "nonexistent_column" not in result_df.columns


def test_add_audit_columns(spark):
    """Test adding audit columns"""
    data = [("order1", "customer1"), ("order2", "customer2")]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])
    
    result_df = add_audit_columns(df, file_name="test.csv", source="test_system")
    
    assert "_ingest_time" in result_df.columns
    assert "_file_name" in result_df.columns
    assert "_source" in result_df.columns
    
    result = result_df.collect()
    assert result[0]["_file_name"] == "test.csv"
    assert result[0]["_source"] == "test_system"


def test_add_audit_columns_no_params(spark):
    """Test adding audit columns without optional parameters"""
    data = [("order1",)]
    df = spark.createDataFrame(data, ["order_id"])
    
    result_df = add_audit_columns(df)
    
    assert "_ingest_time" in result_df.columns
    assert "_file_name" in result_df.columns
    assert "_source" in result_df.columns


def test_deduplicate_by_key(spark):
    """Test deduplication by key"""
    data = [
        ("order1", "2024-01-15 10:00:00", 100),
        ("order1", "2024-01-15 11:00:00", 200),  # Duplicate, newer
        ("order2", "2024-01-15 10:00:00", 150),
        ("order3", "2024-01-15 12:00:00", 300)
    ]
    df = spark.createDataFrame(data, ["order_id", "_ingest_time", "amount"])
    df = df.withColumn("_ingest_time", F.to_timestamp("_ingest_time"))
    
    result_df = deduplicate_by_key(df, ["order_id"], "_ingest_time", ascending=False)
    
    result = result_df.orderBy("order_id").collect()
    assert len(result) == 3  # Should have 3 unique orders
    
    # order1 should have the latest record (amount=200)
    order1_row = [r for r in result if r["order_id"] == "order1"][0]
    assert order1_row["amount"] == 200


def test_normalize_string_column_uppercase(spark):
    """Test normalizing string column to uppercase"""
    data = [("usd",), ("eur",), ("gbp",)]
    df = spark.createDataFrame(data, ["currency_code"])
    
    result_df = normalize_string_column(df, "currency_code", uppercase=True, trim=True)
    
    result = result_df.collect()
    assert result[0]["currency_code"] == "USD"
    assert result[1]["currency_code"] == "EUR"
    assert result[2]["currency_code"] == "GBP"


def test_normalize_string_column_lowercase(spark):
    """Test normalizing string column to lowercase"""
    data = [("WEB",), ("STORE",), ("APP",)]
    df = spark.createDataFrame(data, ["channel"])
    
    result_df = normalize_string_column(df, "channel", uppercase=False, trim=True)
    
    result = result_df.collect()
    assert result[0]["channel"] == "web"
    assert result[1]["channel"] == "store"
    assert result[2]["channel"] == "app"


def test_normalize_string_column_trim(spark):
    """Test trimming whitespace"""
    data = [("  USD  ",), ("EUR ",), (" GBP",)]
    df = spark.createDataFrame(data, ["currency_code"])
    
    result_df = normalize_string_column(df, "currency_code", uppercase=True, trim=True)
    
    result = result_df.collect()
    assert result[0]["currency_code"] == "USD"
    assert result[1]["currency_code"] == "EUR"
    assert result[2]["currency_code"] == "GBP"


def test_create_date_dimension(spark):
    """Test creating date dimension"""
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    
    result_df = create_date_dimension(spark, start_date, end_date)
    
    assert result_df.count() == 10  # 10 days inclusive
    
    # Check columns exist
    assert "date_key" in result_df.columns
    assert "date" in result_df.columns
    assert "year" in result_df.columns
    assert "month" in result_df.columns
    assert "week_of_year" in result_df.columns
    assert "day_of_week" in result_df.columns
    
    # Check first row
    first_row = result_df.orderBy("date").first()
    assert first_row["date_key"] == 20240101
    assert first_row["year"] == 2024
    assert first_row["month"] == 1


def test_get_schema_from_spec(spark):
    """Test generating schema from specification"""
    table_spec = [
        {"column_name": "order_id", "data_type": "STRING", "nullable": "false"},
        {"column_name": "amount", "data_type": "DECIMAL(12,2)", "nullable": "true"},
        {"column_name": "quantity", "data_type": "INT", "nullable": "true"},
        {"column_name": "order_date", "data_type": "DATE", "nullable": "true"}
    ]
    
    schema = get_schema_from_spec(table_spec)
    
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    
    # Check order_id field
    order_id_field = schema.fields[0]
    assert order_id_field.name == "order_id"
    assert isinstance(order_id_field.dataType, StringType)
    assert order_id_field.nullable == False
    
    # Check amount field
    amount_field = schema.fields[1]
    assert amount_field.name == "amount"
    assert isinstance(amount_field.dataType, DecimalType)
    assert amount_field.nullable == True


def test_safe_cast_column_with_nulls(spark):
    """Test safe casting handles null values"""
    data = [("100",), (None,), ("200",)]
    df = spark.createDataFrame(data, ["amount"])
    
    result_df = safe_cast_column(df, "amount", "INT")
    
    result = result_df.collect()
    assert result[0]["amount"] == 100
    assert result[1]["amount"] is None
    assert result[2]["amount"] == 200


def test_deduplicate_preserves_columns(spark):
    """Test that deduplication preserves all columns"""
    data = [
        ("order1", "customer1", "2024-01-15 10:00:00", 100),
        ("order1", "customer1", "2024-01-15 11:00:00", 200),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "_ingest_time", "amount"])
    df = df.withColumn("_ingest_time", F.to_timestamp("_ingest_time"))
    
    result_df = deduplicate_by_key(df, ["order_id"])
    
    # All original columns should be present
    assert "order_id" in result_df.columns
    assert "customer_id" in result_df.columns
    assert "_ingest_time" in result_df.columns
    assert "amount" in result_df.columns
    
    # No extra columns (like _row_num)
    assert "_row_num" not in result_df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])





