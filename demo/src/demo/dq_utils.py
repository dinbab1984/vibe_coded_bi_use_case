"""Data Quality utilities using databricks-labs-dqx"""

import logging
from typing import List, Dict
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def load_dq_checks_from_csv(checks_df: DataFrame, layer_from: str, layer_to: str, table_name: str) -> List[Dict]:
    """
    Load DQ checks from CSV specification and convert to dqx metadata format.
    
    Args:
        checks_df: DataFrame containing DQ check specifications
        layer_from: Source layer (e.g., 'bronze')
        layer_to: Target layer (e.g., 'silver')
        table_name: Target table name
        
    Returns:
        List of check dictionaries in dqx metadata format
    """
    # Filter checks for the specific transformation
    filtered_checks = checks_df.filter(
        (checks_df.layer_from == layer_from) & 
        (checks_df.layer_to == layer_to) & 
        (checks_df.table == table_name)
    ).collect()
    
    dq_checks = []
    
    for check in filtered_checks:
        check_dict = {
            "name": check.name,
            "criticality": check.criticality,
            "check": {}
        }
        
        function = check.function
        check_dict["check"]["function"] = function
        check_dict["check"]["arguments"] = {}
        
        # Build arguments based on function type
        if function == "is_not_null":
            check_dict["check"]["arguments"]["column"] = check.col_name
            
        elif function == "is_not_less_than":
            check_dict["check"]["arguments"]["column"] = check.col_name
            check_dict["check"]["arguments"]["limit"] = float(check.min_limit) if check.min_limit else 0
            
        elif function == "is_in_range":
            check_dict["check"]["arguments"]["column"] = check.col_name
            check_dict["check"]["arguments"]["min_limit"] = float(check.min_limit) if check.min_limit else None
            check_dict["check"]["arguments"]["max_limit"] = float(check.max_limit) if check.max_limit else None
            
        elif function == "is_in_list":
            check_dict["check"]["arguments"]["column"] = check.col_name
            # Parse allowed list from string representation
            if check.allowed:
                import ast
                allowed_values = ast.literal_eval(check.allowed)
                check_dict["check"]["arguments"]["allowed"] = allowed_values
                
        elif function == "regex_match":
            check_dict["check"]["arguments"]["column"] = check.col_name
            check_dict["check"]["arguments"]["regex"] = check.regex
            
        elif function == "sql_expression":
            check_dict["check"]["arguments"]["expression"] = check.expression
            if check.col_name:
                check_dict["check"]["arguments"]["columns"] = [check.col_name]
        
        # Add message if provided
        if check.msg:
            check_dict["message"] = check.msg
            
        dq_checks.append(check_dict)
    
    logger.info(f"Loaded {len(dq_checks)} DQ checks for {table_name}")
    return dq_checks


def convert_csv_spec_to_dqx_format(
    col_name: str = None,
    function: str = None,
    min_limit: float = None,
    max_limit: float = None,
    allowed: str = None,
    regex: str = None,
    expression: str = None,
    criticality: str = "error",
    name: str = None,
    msg: str = None
) -> Dict:
    """
    Convert a single DQ check specification to dqx metadata format.
    
    Args:
        col_name: Column name for the check
        function: DQ function name (is_not_null, is_in_range, etc.)
        min_limit: Minimum value for range checks
        max_limit: Maximum value for range checks
        allowed: Allowed values for list checks
        regex: Regex pattern for regex checks
        expression: SQL expression for sql_expression checks
        criticality: Check criticality (error, warn)
        name: Check name
        msg: Custom message
        
    Returns:
        Dictionary in dqx metadata format
    """
    check = {
        "criticality": criticality,
        "check": {
            "function": function,
            "arguments": {}
        }
    }
    
    if name:
        check["name"] = name
    if msg:
        check["message"] = msg
        
    # Add arguments based on function
    if function == "is_not_null" and col_name:
        check["check"]["arguments"]["column"] = col_name
        
    elif function == "is_not_less_than" and col_name:
        check["check"]["arguments"]["column"] = col_name
        check["check"]["arguments"]["limit"] = min_limit if min_limit is not None else 0
        
    elif function == "is_in_range" and col_name:
        check["check"]["arguments"]["column"] = col_name
        if min_limit is not None:
            check["check"]["arguments"]["min_limit"] = min_limit
        if max_limit is not None:
            check["check"]["arguments"]["max_limit"] = max_limit
            
    elif function == "is_in_list" and col_name and allowed:
        check["check"]["arguments"]["column"] = col_name
        import ast
        check["check"]["arguments"]["allowed"] = ast.literal_eval(allowed)
        
    elif function == "regex_match" and col_name and regex:
        check["check"]["arguments"]["column"] = col_name
        check["check"]["arguments"]["regex"] = regex
        
    elif function == "sql_expression" and expression:
        check["check"]["arguments"]["expression"] = expression
        if col_name:
            check["check"]["arguments"]["columns"] = [col_name]
    
    return check





