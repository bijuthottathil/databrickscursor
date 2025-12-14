# Databricks ETL Pipeline Project

A complete end-to-end ETL pipeline project demonstrating integration with Databricks serverless warehouse using Python.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Databricks](https://img.shields.io/badge/Databricks-Serverless-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 🎯 Project Overview

This project demonstrates how to:
- Connect to Databricks from a local Python environment
- Build an ETL pipeline that processes CSV data
- Remove null values and clean data
- Load cleaned data into Databricks serverless SQL warehouse
- Execute analytics queries on Databricks

## 📁 Project Structure

```
databrickscursor/
├── README.md                           # This file - Project overview
├── .gitignore                          # Git ignore configuration
├── LICENSE.md                          # Project license
│
├── databricks_connection.ipynb         # Jupyter notebook for Databricks connection
├── test_databricks.py                  # Test script for Databricks connectivity
│
└── etl_pipeline/                       # ETL Pipeline module
    ├── README.md                       # Detailed setup guide
    ├── etl_pipeline.py                 # Main ETL script
    └── data/
        └── sample_data.csv             # Sample employee data (15 records)
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Databricks workspace with SQL Warehouse
- Databricks Personal Access Token

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/bijuthottathil/databrickscursor.git
   cd databrickscursor
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install databricks-sdk pandas ipykernel
   ```

4. **Configure Databricks credentials**

   Update the following files with your Databricks token:
   - `etl_pipeline/etl_pipeline.py`
   - `test_databricks.py`
   - `databricks_connection.ipynb`

   ```python
   DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
   DATABRICKS_TOKEN = "your-token-here"
   ```

### Running the ETL Pipeline

```bash
cd etl_pipeline
python etl_pipeline.py
```

## 📊 ETL Pipeline Features

### Extract
- Reads employee data from CSV file
- 15 records with intentional null values
- Displays original data statistics

### Transform
- Removes all rows containing null values
- Data quality checks and validation
- Statistical analysis of cleaned data

### Load
- Connects to Databricks serverless warehouse
- Creates table: `employees_cleaned`
- Inserts 7 clean records
- Executes department-wise analytics

### Results

**Input Data**: 15 employee records (with nulls)
**Cleaned Data**: 7 records (8 removed)
**Databricks Table**: `employees_cleaned`

**Department Analytics**:
- Engineering: 4 employees, avg salary $85,500
- Marketing: 2 employees, avg salary $74,000
- Sales: 1 employee, avg salary $68,000

## 📚 Components

### 1. Databricks Connection Notebook
**File**: `databricks_connection.ipynb`

Interactive Jupyter notebook demonstrating:
- Workspace client initialization
- SQL Warehouse management
- Query execution
- Data operations

### 2. Test Script
**File**: `test_databricks.py`

Standalone Python script to:
- Test Databricks connectivity
- Verify SQL Warehouse availability
- Execute sample queries
- Validate authentication

### 3. ETL Pipeline
**Directory**: `etl_pipeline/`

Complete ETL implementation with:
- CSV data extraction
- Null value removal
- Databricks integration
- Analytics and reporting

**See**: `etl_pipeline/README.md` for detailed documentation

## 🛠️ Technologies Used

- **Python**: Core programming language
- **Databricks SDK**: Workspace and SQL operations
- **Pandas**: Data manipulation and analysis
- **Jupyter**: Interactive development
- **SQL**: Query execution on Databricks

## 📋 Detailed Setup Guide

For complete step-by-step setup instructions, see:
- **ETL Pipeline Setup**: [etl_pipeline/README.md](etl_pipeline/README.md)

Includes:
- Databricks workspace configuration
- Personal Access Token generation
- Virtual environment setup
- Package installation
- Troubleshooting guide

## 🔐 Security Notes

- **Never commit tokens**: Use `.gitignore` to exclude sensitive files
- **Use environment variables**: Store credentials securely
- **Rotate tokens regularly**: Generate new tokens every 90 days
- **Repository includes**: Token placeholders only (not actual tokens)

## 📖 Documentation

- [Databricks SDK Documentation](https://docs.databricks.com/dev-tools/sdk-python.html)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Databricks SQL Warehouses](https://docs.databricks.com/sql/admin/sql-endpoints.html)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## 👤 Author

**Biju Thottathil**
- GitHub: [@bijuthottathil](https://github.com/bijuthottathil)

## ✨ Acknowledgments

- Built with [Databricks](https://databricks.com/)
- Created with assistance from [Claude Code](https://claude.com/claude-code)

---

**Last Updated**: December 2025
