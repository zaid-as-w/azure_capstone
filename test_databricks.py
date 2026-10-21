"""
Connection Test Script for Databricks SQL Warehouse
Tests Azure AD token retrieval, warehouse connectivity, catalog/schema verification,
and query execution against registered tables.
"""

import sys
from databricks_client import (
    test_connection,
    get_databricks_token,
    DEFAULT_SERVER_HOSTNAME,
    DEFAULT_HTTP_PATH,
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    execute_query
)

def main():
    print("=" * 60)
    print("Databricks SQL Warehouse Connection Test")
    print("=" * 60)
    print(f"Server Hostname: {DEFAULT_SERVER_HOSTNAME}")
    print(f"HTTP Path:       {DEFAULT_HTTP_PATH}")
    print(f"Default Catalog: {DEFAULT_CATALOG}")
    print(f"Default Schema:  {DEFAULT_SCHEMA}")
    print("-" * 60)

    print("Step 1: Acquiring Azure AD Bearer Token...")
    try:
        token = get_databricks_token()
        print(f"  [PASS] Token acquired successfully (Length: {len(token)} chars, Prefix: {token[:10]}...)")
    except Exception as e:
        print(f"  [FAIL] Could not acquire token: {e}")
        sys.exit(1)

    print("\nStep 2: Testing SQL Warehouse Connection...")
    result = test_connection()
    if result["success"]:
        print("  [PASS] Connection established successfully!")
        print(f"  User Identity:   {result['user']}")
        print(f"  Active Catalog:  {result['catalog']}")
        print(f"  Active Schema:   {result['schema']}")
    else:
        print(f"  [FAIL] Warehouse query failed: {result.get('error')}")
        print("  Note: If warehouse is STOPPED, Databricks auto-resumes it upon connection (may take 1-2 minutes).")
        sys.exit(1)

    print("\nStep 3: Checking Tables in Catalog...")
    try:
        df_tables = execute_query(f"SHOW TABLES IN `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`")
        print(f"  [PASS] Found {len(df_tables)} registered table(s):")
        for _, row in df_tables.iterrows():
            print(f"    - {row.get('tableName', row.get('table_name', str(row)))}")
    except Exception as e:
        print(f"  [WARN] Could not list tables: {e}")

    print("\n" + "=" * 60)
    print("Databricks SQL Connection Test Complete.")
    print("=" * 60)

if __name__ == "__main__":
    main()
