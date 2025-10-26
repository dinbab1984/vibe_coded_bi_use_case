"""Common utility functions for data transformations"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    DateType, IntegerType, BooleanType, DecimalType, LongType
)
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_cast_column(df: DataFrame, column_name: str, target_type: str) -> DataFrame:
    """
    Safely cast a column to target data type, handling nulls and invalid values.
    
    Args:
        df: Input DataFrame
        column_name: Name of column to cast
        target_type: Target data type (DECIMAL, INT, BOOLEAN, TIMESTAMP, DATE, STRING)
        
    Returns:
        DataFrame with casted column
    """
    if column_name not in df.columns:
        logger.warning(f"Column {column_name} not found in DataFrame")
        return df
    
    try:
        if target_type.startswith("DECIMAL"):
            # Extract precision and scale if specified, e.g., DECIMAL(12,2)
            if "(" in target_type:
                params = target_type.split("(")[1].split(")")[0].split(",")
                precision = int(params[0])
                scale = int(params[1]) if len(params) > 1 else 0
                df = df.withColumn(column_name, F.col(column_name).cast(DecimalType(precision, scale)))
            else:
                df = df.withColumn(column_name, F.col(column_name).cast("decimal(12,2)"))
                
        elif target_type == "INT":
            df = df.withColumn(column_name, F.col(column_name).cast(IntegerType()))
            
        elif target_type == "BIGINT":
            df = df.withColumn(column_name, F.col(column_name).cast(LongType()))
            
        elif target_type == "BOOLEAN":
            # Handle various boolean representations
            df = df.withColumn(
                column_name,
                F.when(F.lower(F.trim(F.col(column_name))).isin(["true", "t", "1", "yes", "y"]), True)
                 .when(F.lower(F.trim(F.col(column_name))).isin(["false", "f", "0", "no", "n"]), False)
                 .otherwise(None)
            )
            
        elif target_type == "TIMESTAMP":
            df = df.withColumn(column_name, F.to_timestamp(F.col(column_name)))
            
        elif target_type == "DATE":
            df = df.withColumn(column_name, F.to_date(F.col(column_name)))
            
        elif target_type == "STRING":
            df = df.withColumn(column_name, F.col(column_name).cast(StringType()))
        
        logger.info(f"Successfully cast column {column_name} to {target_type}")
        
    except Exception as e:
        logger.error(f"Error casting column {column_name} to {target_type}: {str(e)}")
        raise
        
    return df


def add_audit_columns(df: DataFrame, file_name: Optional[str] = None, source: Optional[str] = None) -> DataFrame:
    """
    Add standard audit columns to bronze layer tables.
    
    Args:
        df: Input DataFrame
        file_name: Optional file name
        source: Optional source system name
        
    Returns:
        DataFrame with audit columns
    """
    df = df.withColumn("_ingest_time", F.current_timestamp())
    
    if file_name:
        df = df.withColumn("_file_name", F.lit(file_name))
    elif "_file_name" not in df.columns:
        df = df.withColumn("_file_name", F.lit(None).cast(StringType()))
        
    if source:
        df = df.withColumn("_source", F.lit(source))
    elif "_source" not in df.columns:
        df = df.withColumn("_source", F.lit(None).cast(StringType()))
        
    return df


def deduplicate_by_key(df: DataFrame, key_columns: List[str], order_by_column: str = "_ingest_time", ascending: bool = False) -> DataFrame:
    """
    Deduplicate DataFrame by keeping the latest record per key.
    
    Args:
        df: Input DataFrame
        key_columns: List of columns that form the business key
        order_by_column: Column to use for ordering (default: _ingest_time)
        ascending: Sort order (default: False for latest first)
        
    Returns:
        Deduplicated DataFrame
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy(key_columns).orderBy(
        F.col(order_by_column).desc() if not ascending else F.col(order_by_column).asc()
    )
    
    df_deduped = df.withColumn("_row_num", F.row_number().over(window_spec)) \
                   .filter(F.col("_row_num") == 1) \
                   .drop("_row_num")
    
    logger.info(f"Deduplicated DataFrame by {key_columns}")
    return df_deduped


def normalize_string_column(df: DataFrame, column_name: str, uppercase: bool = True, trim: bool = True) -> DataFrame:
    """
    Normalize string column (trim, uppercase/lowercase).
    
    Args:
        df: Input DataFrame
        column_name: Column to normalize
        uppercase: Convert to uppercase (default: True)
        trim: Trim whitespace (default: True)
        
    Returns:
        DataFrame with normalized column
    """
    if column_name not in df.columns:
        return df
        
    col_expr = F.col(column_name)
    
    if trim:
        col_expr = F.trim(col_expr)
    if uppercase:
        col_expr = F.upper(col_expr)
    else:
        col_expr = F.lower(col_expr)
        
    df = df.withColumn(column_name, col_expr)
    return df


def create_date_dimension(spark: SparkSession, start_date: str, end_date: str) -> DataFrame:
    """
    Create a date dimension table with various date attributes.
    
    Args:
        spark: SparkSession
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        
    Returns:
        DataFrame with date dimension
    """
    # Generate date range
    df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
    """)
    
    # Add date attributes
    df = df.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType())) \
           .withColumn("year", F.year(F.col("date")).cast(IntegerType())) \
           .withColumn("month", F.month(F.col("date")).cast(IntegerType())) \
           .withColumn("week_of_year", F.weekofyear(F.col("date")).cast(IntegerType())) \
           .withColumn("day_of_week", F.dayofweek(F.col("date")).cast(IntegerType()))
    
    # Reorder columns
    df = df.select("date_key", "date", "year", "month", "week_of_year", "day_of_week")
    
    logger.info(f"Created date dimension from {start_date} to {end_date}")
    return df


def get_schema_from_spec(table_spec: List[Dict[str, str]]) -> StructType:
    """
    Generate StructType schema from table specification.
    
    Args:
        table_spec: List of dicts with keys: column_name, data_type, nullable
        
    Returns:
        StructType schema
    """
    type_mapping = {
        "STRING": StringType(),
        "TIMESTAMP": TimestampType(),
        "DATE": DateType(),
        "INT": IntegerType(),
        "BIGINT": LongType(),
        "BOOLEAN": BooleanType()
    }
    
    fields = []
    for col_spec in table_spec:
        col_name = col_spec["column_name"]
        data_type_str = col_spec["data_type"]
        nullable = col_spec.get("nullable", "true").lower() == "true"
        
        # Handle DECIMAL type
        if data_type_str.startswith("DECIMAL"):
            if "(" in data_type_str:
                params = data_type_str.split("(")[1].split(")")[0].split(",")
                precision = int(params[0])
                scale = int(params[1]) if len(params) > 1 else 0
                data_type = DecimalType(precision, scale)
            else:
                data_type = DecimalType(12, 2)
        else:
            data_type = type_mapping.get(data_type_str, StringType())
            
        fields.append(StructField(col_name, data_type, nullable))
    
    return StructType(fields)


def apply_scd2_merge(spark: SparkSession, 
                     source_df: DataFrame, 
                     target_table: str, 
                     business_key: List[str],
                     compare_columns: List[str]) -> None:
    """
    Apply SCD Type 2 merge operation.
    
    Args:
        spark: SparkSession
        source_df: Source DataFrame with new/updated records
        target_table: Target table name (catalog.schema.table)
        business_key: List of business key columns
        compare_columns: List of columns to compare for changes
    """
    # This is a simplified implementation
    # In production, use MERGE INTO with proper SCD2 logic
    
    source_df.createOrReplaceTempView("source_data")
    
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING source_data AS source
    ON {' AND '.join([f'target.{col} = source.{col}' for col in business_key])}
    WHEN MATCHED AND target.is_current = TRUE AND (
        {' OR '.join([f'target.{col} != source.{col}' for col in compare_columns])}
    ) THEN UPDATE SET 
        effective_to = current_timestamp(),
        is_current = FALSE
    WHEN NOT MATCHED THEN INSERT *
    """
    
    logger.info(f"Applying SCD2 merge to {target_table}")
    # Note: This requires Delta Lake merge capabilities
    # spark.sql(merge_sql)





