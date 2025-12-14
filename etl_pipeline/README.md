# Simple ETL Pipeline

A complete ETL (Extract, Transform, Load) pipeline that processes employee data and loads it into Databricks serverless warehouse.

## Pipeline Overview

```
CSV File → Extract → Transform (Remove Nulls) → Load to Databricks → Display Results
```

---

## 📋 Complete Setup Guide

### Step 1: Databricks Workspace Setup

1. **Access your Databricks workspace**
   - Navigate to: `https://dbc-1109a291-3564.cloud.databricks.com`
   - Login with your credentials

2. **Generate Personal Access Token**
   - Click on your email/profile in the top right corner
   - Go to **Settings** → **Developer** → **Access tokens**
   - Click **Generate new token**
   - Give it a name (e.g., "etl-pipeline")
   - Click **Generate**
   - **Copy the token immediately** (you won't see it again!)

3. **Verify SQL Warehouse**
   - Go to **SQL Warehouses** in the sidebar
   - You should see "Serverless Starter Warehouse" (or similar)
   - Note: The pipeline will automatically start it if stopped

### Step 2: Local Environment Setup

1. **Navigate to project directory**
   ```bash
   cd /Volumes/D/Databricks/Databrickscursor
   ```

2. **Create Python virtual environment**
   ```bash
   python3 -m venv venv
   ```

3. **Activate virtual environment**
   ```bash
   source venv/bin/activate
   ```

4. **Install required packages**
   ```bash
   pip install databricks-sdk pandas ipykernel
   ```

### Step 3: Configure Databricks Connection

1. **Update credentials in `etl_pipeline.py`**

   Open `etl_pipeline/etl_pipeline.py` and update these lines:

   ```python
   DATABRICKS_HOST = "https://dbc-1109a291-3564.cloud.databricks.com"
   DATABRICKS_TOKEN = "your-token-here"  # Replace with your actual token
   ```

   **⚠️ Security Note**: Keep your token secure! Don't commit it to version control.

### Step 4: Run the ETL Pipeline

1. **Navigate to ETL directory**
   ```bash
   cd etl_pipeline
   ```

2. **Ensure virtual environment is active**
   ```bash
   source ../venv/bin/activate
   ```

3. **Run the pipeline**
   ```bash
   python etl_pipeline.py
   ```

---

## 📁 Project Structure

```
etl_pipeline/
├── README.md              # This file - Complete documentation
├── etl_pipeline.py        # Main ETL script with Databricks integration
└── data/
    └── sample_data.csv    # Sample employee data (15 records with nulls)
```

---

## 🔄 ETL Pipeline Components

### 1. EXTRACT - Load Data
- Reads employee data from `data/sample_data.csv`
- Loads into pandas DataFrame
- Displays original data with null value counts
- **Output**: 15 total records

### 2. TRANSFORM - Clean Data
- Removes all rows containing any null values
- Displays transformation statistics
- Shows cleaned dataset
- **Output**: 7 clean records (8 removed)

### 3. LOAD - Upload to Databricks
- Connects to Databricks serverless SQL warehouse
- Creates table: `employees_cleaned`
- Inserts cleaned records
- Executes analytics queries
- **Output**: Data stored in Databricks

---

## 📊 Sample Data Overview

The CSV file contains employee records with **intentional null values** to demonstrate the cleaning process:

| Field       | Null Count |
|-------------|------------|
| name        | 1          |
| age         | 4          |
| department  | 2          |
| salary      | 3          |
| join_date   | 2          |
| **Total Records** | **15** |

---

## 🎯 Pipeline Output

When you run the pipeline, you'll see:

### 1. **Extraction Results**
```
✓ Data loaded successfully!
  Total records: 15
  Columns: id, name, age, department, salary, join_date
```

### 2. **Transformation Results**
```
✓ Transformation complete!
  Original records: 15
  Records removed: 8
  Clean records: 7
```

### 3. **Load Results**
```
✓ Connected to Databricks workspace
✓ Table created: employees_cleaned
✓ Inserted 7 records
```

### 4. **Analytics Results**
Department-wise statistics:
- **Engineering**: 4 employees, avg salary $85,500
- **Marketing**: 2 employees, avg salary $74,000
- **Sales**: 1 employee, avg salary $68,000

---

## 🛠️ Troubleshooting

### Issue: "Command not found: pip"
**Solution**: Use `python3 -m pip install` instead of `pip install`

### Issue: "externally-managed-environment"
**Solution**: Make sure you're using the virtual environment:
```bash
source venv/bin/activate
```

### Issue: "Cluster does not exist"
**Solution**: The script automatically uses SQL Warehouses. Make sure you have at least one SQL Warehouse in your Databricks workspace.

### Issue: Running notebook requires ipykernel
**Solution**: Install ipykernel in the virtual environment:
```bash
source venv/bin/activate
pip install ipykernel
```

### Issue: Warehouse is stopped
**Solution**: The script automatically starts the warehouse. Wait 10-30 seconds for it to initialize.

---

## 📦 Requirements

- **Python**: 3.8 or higher
- **Packages**:
  - `pandas` - Data manipulation
  - `databricks-sdk` - Databricks API client
  - `ipykernel` - Jupyter notebook support (optional)
- **Databricks**:
  - Active Databricks workspace
  - Personal Access Token
  - SQL Warehouse (serverless or provisioned)

---

## 🔐 Security Best Practices

1. **Never commit tokens to git**
   ```bash
   # Add to .gitignore
   echo "*.token" >> .gitignore
   echo ".env" >> .gitignore
   ```

2. **Use environment variables** (recommended)
   ```python
   import os
   DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
   ```

3. **Rotate tokens regularly**
   - Generate new tokens every 90 days
   - Delete old tokens from Databricks settings

---

## 📚 Additional Resources

- [Databricks SDK Documentation](https://docs.databricks.com/dev-tools/sdk-python.html)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Databricks SQL Warehouses](https://docs.databricks.com/sql/admin/sql-endpoints.html)

---

## ✅ Quick Start Checklist

- [ ] Databricks workspace access confirmed
- [ ] Personal Access Token generated
- [ ] Virtual environment created and activated
- [ ] Required packages installed (`databricks-sdk`, `pandas`)
- [ ] Credentials updated in `etl_pipeline.py`
- [ ] SQL Warehouse available in Databricks
- [ ] Pipeline executed successfully
- [ ] Data visible in Databricks table: `employees_cleaned`

---

**Last Updated**: December 2025
**Databricks Workspace**: `dbc-1109a291-3564.cloud.databricks.com`
