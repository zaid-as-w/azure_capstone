"""
Databricks SQL Client for Azure Data Engineering Capstone
Enables direct querying against Databricks SQL Warehouse using
Azure AD Managed Identity / DefaultAzureCredential (keyless) or Personal Access Token.
"""

import os
import pandas as pd
from typing import Optional
from databricks import sql
from azure.identity import DefaultAzureCredential, AzureCliCredential


# Default configuration discovered from workspace
DEFAULT_SERVER_HOSTNAME = "adb-7405607366206759.19.azuredatabricks.net"
DEFAULT_HTTP_PATH = "/sql/1.0/warehouses/44847d5087ca2301"
DEFAULT_CATALOG = "zaid_capstone_dbx_7405607366206759"
DEFAULT_SCHEMA = "default"
DATABRICKS_AZURE_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


def _get_config_val(key: str, default: str) -> str:
    """Retrieve config from env vars first, then st.secrets, then default."""
    val = os.getenv(key)
    if val:
        return val
    try:
        import streamlit as st
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return default


def get_databricks_token() -> str:
    """
    Acquire authentication token for Databricks.
    Checks environment variable DATABRICKS_TOKEN and st.secrets first.
    If not found, dynamically acquires an Azure AD Bearer token via DefaultAzureCredential.
    """
    token = _get_config_val("DATABRICKS_TOKEN", "")
    if token:
        return token

    try:
        credential = DefaultAzureCredential()
        token_obj = credential.get_token(DATABRICKS_AZURE_RESOURCE_ID)
        return token_obj.token
    except Exception as e:
        # Fallback to Azure CLI credential explicitly
        try:
            cli_cred = AzureCliCredential()
            token_obj = cli_cred.get_token(DATABRICKS_AZURE_RESOURCE_ID)
            return token_obj.token
        except Exception as cli_err:
            raise ConnectionError(
                f"Failed to acquire Databricks token: {e}. "
                f"CLI error: {cli_err}. Please ensure DATABRICKS_TOKEN is set in Streamlit secrets or `az login` is active."
            )


def get_connection(
    server_hostname: Optional[str] = None,
    http_path: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
):
    """Create a connection to the Databricks SQL Warehouse."""
    hostname = server_hostname or _get_config_val("DATABRICKS_SERVER_HOSTNAME", DEFAULT_SERVER_HOSTNAME)
    path = http_path or _get_config_val("DATABRICKS_HTTP_PATH", DEFAULT_HTTP_PATH)
    cat = catalog or _get_config_val("DATABRICKS_CATALOG", DEFAULT_CATALOG)
    sch = schema or _get_config_val("DATABRICKS_SCHEMA", DEFAULT_SCHEMA)
    token = get_databricks_token()

    return sql.connect(
        server_hostname=hostname,
        http_path=path,
        access_token=token,
        catalog=cat,
        schema=sch,
    )


def execute_query(query: str, params: Optional[dict] = None) -> pd.DataFrame:
    """
    Executes a SQL query against Databricks SQL Warehouse and returns results as a Pandas DataFrame.
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, parameters=params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)


def test_connection() -> dict:
    """Test SQL Warehouse connectivity and return server metadata."""
    try:
        df = execute_query("SELECT current_user() AS user_name, current_catalog() AS catalog, current_schema() AS schema")
        return {
            "success": True,
            "user": df.iloc[0]["user_name"],
            "catalog": df.iloc[0]["catalog"],
            "schema": df.iloc[0]["schema"],
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


def get_gold_category_sales() -> pd.DataFrame:
    """Fetch category-level sales aggregates from Gold layer."""
    query = f"""
        SELECT 
            category,
            total_orders,
            total_quantity_sold,
            total_revenue,
            avg_order_value
        FROM `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.gold_category_sales
        ORDER BY total_revenue DESC
    """
    return execute_query(query)


def get_gold_city_sales() -> pd.DataFrame:
    """Fetch city-level sales aggregates from Gold layer."""
    query = f"""
        SELECT 
            city,
            total_orders,
            total_quantity_sold,
            total_revenue,
            avg_order_value
        FROM `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.gold_city_sales
        ORDER BY total_revenue DESC
    """
    return execute_query(query)


def get_gold_monthly_sales() -> pd.DataFrame:
    """Fetch monthly sales trends from Gold layer."""
    query = f"""
        SELECT 
            sales_month,
            total_orders,
            total_quantity_sold,
            total_revenue,
            avg_order_value
        FROM `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.gold_monthly_sales
        ORDER BY sales_month ASC
    """
    return execute_query(query)


def get_overall_kpis() -> dict:
    """Calculate authoritative overall KPIs directly from Silver cleaned_sales."""
    query = f"""
        SELECT 
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT customer_id) AS total_customers,
            COALESCE(SUM(quantity), 0) AS total_units_sold,
            COALESCE(ROUND(SUM(total_sales), 2), 0.0) AS total_revenue,
            COALESCE(ROUND(SUM(total_sales) / NULLIF(COUNT(DISTINCT order_id), 0), 2), 0.0) AS avg_order_value
        FROM `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.cleaned_sales
    """
    df = execute_query(query)
    if not df.empty:
        row = df.iloc[0]
        return {
            "total_orders": int(row["total_orders"]),
            "total_customers": int(row["total_customers"]),
            "total_units_sold": int(row["total_units_sold"]),
            "total_revenue": float(row["total_revenue"]),
            "avg_order_value": float(row["avg_order_value"]),
        }
    return {
        "total_orders": 0,
        "total_customers": 0,
        "total_units_sold": 0,
        "total_revenue": 0.0,
        "avg_order_value": 0.0,
    }


def get_ingestion_control_history() -> pd.DataFrame:
    """Query file processing audit logs from the control table."""
    try:
        query = f"""
            SELECT 
                file_name,
                status,
                records_read,
                records_valid,
                records_rejected,
                started_at,
                processed_at,
                pipeline_run_id,
                error_message
            FROM `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.file_processing_history
            ORDER BY started_at DESC
            LIMIT 50
        """
        return execute_query(query)
    except Exception:
        return pd.DataFrame()
