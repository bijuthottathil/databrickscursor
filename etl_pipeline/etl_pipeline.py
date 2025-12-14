#!/usr/bin/env python3
"""
Simple ETL Pipeline
- Extracts data from CSV
- Transforms data (removes null values)
- Loads data to Databricks and displays results
"""

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import State

# Configuration
DATABRICKS_HOST = "https://dbc-1109a291-3564.cloud.databricks.com"
DATABRICKS_TOKEN = "your_databricks_token"
DATA_FILE = "data/sample_data.csv"

print("=" * 70)
print("SIMPLE ETL PIPELINE - Employee Data Processing")
print("=" * 70)

# ============================================================================
# STEP 1: EXTRACT - Load data from CSV
# ============================================================================
print("\n[STEP 1] EXTRACT - Loading data from CSV...")
print("-" * 70)

# Load CSV data into pandas DataFrame
df = pd.read_csv(DATA_FILE)

print(f"✓ Data loaded successfully!")
print(f"  Total records: {len(df)}")
print(f"  Columns: {', '.join(df.columns.tolist())}")

print("\nOriginal Data Preview:")
print(df.to_string(index=False))

# Show null value counts
print("\n\nNull Values Summary:")
null_counts = df.isnull().sum()
for col, count in null_counts.items():
    if count > 0:
        print(f"  {col}: {count} null values")

# ============================================================================
# STEP 2: TRANSFORM - Remove null values
# ============================================================================
print("\n\n[STEP 2] TRANSFORM - Removing null values...")
print("-" * 70)

# Store original count
original_count = len(df)

# Remove rows with any null values
df_cleaned = df.dropna()

# Calculate removed count
removed_count = original_count - len(df_cleaned)

print(f"✓ Transformation complete!")
print(f"  Original records: {original_count}")
print(f"  Records removed: {removed_count}")
print(f"  Clean records: {len(df_cleaned)}")

print("\nCleaned Data (no null values):")
print(df_cleaned.to_string(index=False))

# Show data types and basic stats
print("\n\nData Statistics:")
print(df_cleaned.describe())

# ============================================================================
# STEP 3: LOAD - Upload to Databricks and Display
# ============================================================================
print("\n\n[STEP 3] LOAD - Uploading to Databricks...")
print("-" * 70)

# Connect to Databricks
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
print("✓ Connected to Databricks workspace")

# Get SQL Warehouse
warehouses = list(w.warehouses.list())
if not warehouses:
    print("✗ No SQL warehouses found. Skipping Databricks upload.")
else:
    warehouse_id = warehouses[0].id
    warehouse_name = warehouses[0].name
    print(f"  Using warehouse: {warehouse_name}")

    # Check and start warehouse if needed
    warehouse = w.warehouses.get(warehouse_id)
    if warehouse.state == State.STOPPED:
        print("  Starting warehouse...")
        w.warehouses.start(warehouse_id)

        import time
        while True:
            warehouse = w.warehouses.get(warehouse_id)
            if warehouse.state == State.RUNNING:
                print("  ✓ Warehouse started!")
                break
            time.sleep(5)

    # Create table in Databricks
    print("\n  Creating table in Databricks...")

    # Drop table if exists
    drop_sql = "DROP TABLE IF EXISTS employees_cleaned"
    w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=drop_sql
    )

    # Create table schema
    create_table_sql = """
    CREATE TABLE employees_cleaned (
        id INT,
        name STRING,
        age INT,
        department STRING,
        salary DECIMAL(10,2),
        join_date DATE
    )
    """

    w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=create_table_sql
    )
    print("  ✓ Table created: employees_cleaned")

    # Insert cleaned data
    print("  Inserting cleaned records...")

    for _, row in df_cleaned.iterrows():
        insert_sql = f"""
        INSERT INTO employees_cleaned VALUES (
            {row['id']},
            '{row['name']}',
            {row['age']},
            '{row['department']}',
            {row['salary']},
            '{row['join_date']}'
        )
        """
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=insert_sql
        )

    print(f"  ✓ Inserted {len(df_cleaned)} records")

    # Query and display data from Databricks
    print("\n\n[DISPLAY] Data from Databricks:")
    print("-" * 70)

    # Select all data
    select_sql = "SELECT * FROM employees_cleaned ORDER BY id"
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=select_sql
    )

    if result.result and result.result.data_array:
        # Print header
        if result.manifest and result.manifest.schema and result.manifest.schema.columns:
            headers = [col.name for col in result.manifest.schema.columns]
            print("  " + " | ".join(f"{h:>12}" for h in headers))
            print("  " + "-" * 90)

        # Print rows
        for row in result.result.data_array:
            print("  " + " | ".join(f"{str(val):>12}" for val in row))

    # Show aggregations
    print("\n\n[ANALYTICS] Summary Statistics from Databricks:")
    print("-" * 70)

    # Department-wise statistics
    stats_sql = """
    SELECT
        department,
        COUNT(*) as employee_count,
        ROUND(AVG(age), 1) as avg_age,
        ROUND(AVG(salary), 2) as avg_salary,
        MIN(join_date) as earliest_join,
        MAX(join_date) as latest_join
    FROM employees_cleaned
    GROUP BY department
    ORDER BY department
    """

    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=stats_sql
    )

    print("\nDepartment-wise Summary:")
    if result.result and result.result.data_array:
        if result.manifest and result.manifest.schema and result.manifest.schema.columns:
            headers = [col.name for col in result.manifest.schema.columns]
            print("  " + " | ".join(f"{h:>15}" for h in headers))
            print("  " + "-" * 120)

        for row in result.result.data_array:
            print("  " + " | ".join(f"{str(val):>15}" for val in row))

print("\n\n" + "=" * 70)
print("✓ ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 70)
print(f"\nSummary:")
print(f"  • Extracted: {original_count} records from CSV")
print(f"  • Transformed: Removed {removed_count} records with null values")
print(f"  • Loaded: {len(df_cleaned)} clean records to Databricks")
print(f"  • Table created: employees_cleaned")
print("=" * 70)
