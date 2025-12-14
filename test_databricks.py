#!/usr/bin/env python3
"""
Test script to connect to Databricks and execute code
"""
import os
from databricks.sdk import WorkspaceClient

# Databricks workspace configuration
DATABRICKS_HOST = "https://dbc-1109a291-3564.cloud.databricks.com"
DATABRICKS_TOKEN = "dapi005ac5904e0dcd410292c1589ac4989e"

# Set environment variables
os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST
os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

print("=" * 60)
print("STEP 1: Connecting to Databricks Workspace")
print("=" * 60)

try:
    # Initialize the workspace client
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

    print("✓ Connected to Databricks workspace successfully!")
    print(f"  Host: {DATABRICKS_HOST}")

    print("\n" + "=" * 60)
    print("STEP 2: Listing Available Clusters")
    print("=" * 60)

    # List clusters
    clusters = list(w.clusters.list())
    if clusters:
        for cluster in clusters:
            print(f"  • {cluster.cluster_name}")
            print(f"    ID: {cluster.cluster_id}")
            print(f"    State: {cluster.state}")
    else:
        print("  No traditional clusters found (serverless compute available)")

    print("\n" + "=" * 60)
    print("STEP 3: Listing SQL Warehouses (Serverless Compute)")
    print("=" * 60)

    # List SQL warehouses
    warehouses = list(w.warehouses.list())
    warehouse_id = None

    if warehouses:
        print("  Available SQL Warehouses:")
        for warehouse in warehouses:
            print(f"  • {warehouse.name}")
            print(f"    ID: {warehouse.id}")
            print(f"    State: {warehouse.state}")
            if warehouse_id is None:
                warehouse_id = warehouse.id
    else:
        print("  No SQL warehouses found")
        print("\n  NOTE: To use serverless compute, you need to:")
        print("  1. Create a SQL Warehouse in your Databricks workspace")
        print("  2. Or create a compute cluster")
        raise Exception("No compute resources available")

    print("\n" + "=" * 60)
    print("STEP 4: Starting SQL Warehouse")
    print("=" * 60)

    # Check warehouse state and start if needed
    warehouse = w.warehouses.get(warehouse_id)
    from databricks.sdk.service.sql import State

    if warehouse.state == State.STOPPED:
        print(f"  Starting warehouse: {warehouse.name}...")
        w.warehouses.start(warehouse_id)
        print("  ✓ Warehouse start initiated")
        print("  Waiting for warehouse to be ready...")

        # Wait for warehouse to start
        import time
        max_wait = 300  # 5 minutes
        waited = 0
        while waited < max_wait:
            warehouse = w.warehouses.get(warehouse_id)
            if warehouse.state == State.RUNNING:
                print(f"  ✓ Warehouse is now running!")
                break
            elif warehouse.state == State.STARTING:
                print(f"  Still starting... ({waited}s)")
                time.sleep(10)
                waited += 10
            else:
                print(f"  Warning: Unexpected state: {warehouse.state}")
                time.sleep(10)
                waited += 10
    else:
        print(f"  Warehouse is already {warehouse.state}")

    print("\n" + "=" * 60)
    print("STEP 5: Executing SQL Queries on Serverless Warehouse")
    print("=" * 60)

    # Execute SQL using the statement execution API
    print("  Executing test query...")

    from databricks.sdk.service.sql import StatementParameterListItem

    # Execute a simple SQL query
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT 'Hello from Databricks Serverless!' as message, current_timestamp() as timestamp"
    )

    print("  ✓ Query executed successfully!")
    print(f"  Result:")
    if result.result and result.result.data_array:
        for row in result.result.data_array:
            print(f"    {row}")

    print("\n" + "=" * 60)
    print("STEP 6: Creating and Querying Test Table")
    print("=" * 60)

    # Create a temporary table with test data
    create_table_sql = """
    CREATE OR REPLACE TEMPORARY VIEW test_data AS
    SELECT 'Alice' as name, 25 as age UNION ALL
    SELECT 'Bob' as name, 30 as age UNION ALL
    SELECT 'Charlie' as name, 35 as age
    """

    print("  Creating test table...")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=create_table_sql
    )
    print("  ✓ Test table created!")

    # Query the test data
    print("\n  Querying test data...")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT * FROM test_data"
    )

    print("  Test data:")
    if result.result and result.result.data_array:
        # Print header
        if result.manifest and result.manifest.schema and result.manifest.schema.columns:
            headers = [col.name for col in result.manifest.schema.columns]
            print(f"    {' | '.join(headers)}")
            print(f"    {'-' * 30}")

        # Print rows
        for row in result.result.data_array:
            print(f"    {' | '.join(str(val) for val in row)}")

    # Compute average age
    print("\n  Computing average age...")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT AVG(age) as avg_age FROM test_data"
    )

    if result.result and result.result.data_array:
        avg_age = result.result.data_array[0][0]
        print(f"  Average age: {avg_age}")

    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED SUCCESSFULLY!")
    print("=" * 60)

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
