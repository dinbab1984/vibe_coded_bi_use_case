"""Unit tests for dq_utils module"""

import pytest
from pyspark.sql import SparkSession
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from demo.dq_utils import convert_csv_spec_to_dqx_format


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_dq_utils") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


def test_convert_is_not_null():
    """Test converting is_not_null check"""
    result = convert_csv_spec_to_dqx_format(
        col_name="order_id",
        function="is_not_null",
        criticality="error",
        name="valid_order_id"
    )
    
    assert result["criticality"] == "error"
    assert result["name"] == "valid_order_id"
    assert result["check"]["function"] == "is_not_null"
    assert result["check"]["arguments"]["column"] == "order_id"


def test_convert_is_not_less_than():
    """Test converting is_not_less_than check"""
    result = convert_csv_spec_to_dqx_format(
        col_name="amount",
        function="is_not_less_than",
        min_limit=0,
        criticality="error",
        name="non_negative_amount"
    )
    
    assert result["check"]["function"] == "is_not_less_than"
    assert result["check"]["arguments"]["column"] == "amount"
    assert result["check"]["arguments"]["limit"] == 0


def test_convert_is_in_range():
    """Test converting is_in_range check"""
    result = convert_csv_spec_to_dqx_format(
        col_name="quantity",
        function="is_in_range",
        min_limit=1,
        max_limit=9999,
        criticality="error",
        name="valid_quantity"
    )
    
    assert result["check"]["function"] == "is_in_range"
    assert result["check"]["arguments"]["column"] == "quantity"
    assert result["check"]["arguments"]["min_limit"] == 1
    assert result["check"]["arguments"]["max_limit"] == 9999


def test_convert_is_in_list():
    """Test converting is_in_list check"""
    result = convert_csv_spec_to_dqx_format(
        col_name="currency_code",
        function="is_in_list",
        allowed="['USD','EUR','GBP']",
        criticality="warn",
        name="valid_currency"
    )
    
    assert result["check"]["function"] == "is_in_list"
    assert result["check"]["arguments"]["column"] == "currency_code"
    assert result["check"]["arguments"]["allowed"] == ['USD', 'EUR', 'GBP']


def test_convert_regex_match():
    """Test converting regex_match check"""
    result = convert_csv_spec_to_dqx_format(
        col_name="email",
        function="regex_match",
        regex="^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
        criticality="warn",
        name="valid_email"
    )
    
    assert result["check"]["function"] == "regex_match"
    assert result["check"]["arguments"]["column"] == "email"
    assert result["check"]["arguments"]["regex"] == "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"


def test_convert_sql_expression():
    """Test converting sql_expression check"""
    result = convert_csv_spec_to_dqx_format(
        function="sql_expression",
        expression="COUNT(DISTINCT order_id) = COUNT(order_id)",
        criticality="error",
        name="no_duplicates"
    )
    
    assert result["check"]["function"] == "sql_expression"
    assert result["check"]["arguments"]["expression"] == "COUNT(DISTINCT order_id) = COUNT(order_id)"


def test_convert_with_message():
    """Test converting check with custom message"""
    result = convert_csv_spec_to_dqx_format(
        col_name="amount",
        function="is_not_less_than",
        min_limit=0,
        criticality="error",
        name="non_negative_amount",
        msg="Amount must be non-negative"
    )
    
    assert "message" in result
    assert result["message"] == "Amount must be non-negative"


def test_convert_default_criticality():
    """Test default criticality is 'error'"""
    result = convert_csv_spec_to_dqx_format(
        col_name="order_id",
        function="is_not_null"
    )
    
    assert result["criticality"] == "error"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])





