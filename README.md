# 🏦 End-to-End Financial Transactions Data Pipeline using Snowflake Medallion Architecture

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)](#)
[![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)](https://jupyter.org/)

An end-to-end data engineering pipeline for financial transactions built on **Snowflake**, implementing the **Medallion Architecture** (Bronze → Silver → Gold). Raw transaction data is ingested via Snowflake Stages, incrementally refined through each layer, and surfaced as analytics-ready aggregations with **SCD Type 1** change tracking.

---

## 📐 Architecture

![](snowflake_project.png)

The pipeline follows a three-layer Medallion Architecture:

| Layer | Schema | Description |
|-------|--------|-------------|
| 🥉 **Bronze** | `BRONZE` | Raw, unmodified data loaded from internal/external stages via `COPY INTO` |
| 🥈 **Silver** | `SILVER` | Cleansed, deduplicated, and type-cast data with business rules applied |
| 🥇 **Gold** | `GOLD` | Aggregated, analytics-ready tables optimised for reporting and BI consumption |

---

## 🗂️ Repository Structure

```
📦 End-to-End-Financial-Transactions-Data-Pipeline
├── 2_configs.sql                        # Snowflake environment setup (warehouse, database, schemas, roles)
├── 4_copy_into.sql                      # COPY INTO commands to ingest raw data into Bronze layer
├── 5_tranforamtion_bronze_to_silver.sql # Bronze → Silver transformation (cleansing, deduplication, type casting)
├── 6_SCD1_demo.ipynb                    # SCD Type 1 demo using Python + Snowflake connector
├── 6_transformation_silver_to_gold.sql  # Silver → Gold transformation (aggregations, fact/dim models)
├── connection.py                        # Snowflake Python connector setup
├── snowflake_project.png                # Architecture diagram
└── README.md
```

---

## 🛠️ Tech Stack

- **Cloud Data Warehouse** — Snowflake
- **Data Ingestion** — Snowflake Internal Stages + `COPY INTO`
- **Transformation** — Snowflake SQL (DDL + DML)
- **Change Data Management** — SCD Type 1 (upsert pattern via `MERGE`)
- **Orchestration** — Snowflake Tasks & Streams *(native)*
- **Python** — `snowflake-connector-python` for programmatic access
- **Notebook** — Jupyter (SCD Type 1 demo)

---

## 🚀 Pipeline Walkthrough

### Step 1 — Environment Configuration (`2_configs.sql`)

Sets up the full Snowflake environment:

```sql
-- Create warehouse, database, schemas, and roles
CREATE WAREHOUSE IF NOT EXISTS FINANCE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS FINANCE_DB;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS FINANCE_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS FINANCE_DB.GOLD;
```

### Step 2 — Bronze Layer Ingestion (`4_copy_into.sql`)

Raw financial transaction files (CSV/JSON) are staged and loaded into Bronze tables with zero transformation — preserving the source data exactly as received.

```sql
-- Create internal stage
CREATE STAGE IF NOT EXISTS FINANCE_DB.BRONZE.TXN_STAGE;

-- Load raw data into Bronze
COPY INTO FINANCE_DB.BRONZE.RAW_TRANSACTIONS
FROM @FINANCE_DB.BRONZE.TXN_STAGE/transactions/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

### Step 3 — Bronze → Silver Transformation (`5_tranforamtion_bronze_to_silver.sql`)

Applies data quality rules — removes duplicates, casts data types, handles nulls, standardises formats, and applies business validation logic.

```sql
-- Merge cleansed records into Silver
MERGE INTO FINANCE_DB.SILVER.TRANSACTIONS AS tgt
USING (
    SELECT DISTINCT
        transaction_id,
        TRIM(customer_id)                         AS customer_id,
        TRY_TO_DATE(transaction_date, 'YYYY-MM-DD') AS transaction_date,
        TRY_TO_DECIMAL(amount, 18, 2)             AS amount,
        UPPER(TRIM(transaction_type))             AS transaction_type,
        UPPER(TRIM(status))                       AS status
    FROM FINANCE_DB.BRONZE.RAW_TRANSACTIONS
    WHERE transaction_id IS NOT NULL
) AS src
ON tgt.transaction_id = src.transaction_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

### Step 4 — SCD Type 1 Demo (`6_SCD1_demo.ipynb`)

Demonstrates Slowly Changing Dimension Type 1 (overwrite) using the Snowflake Python connector — showing how customer or account master data is kept current by updating changed records in place.

```python
import snowflake.connector
from connection import get_snowflake_connection

conn = get_snowflake_connection()
cur  = conn.cursor()

# SCD1: overwrite changed dimension attributes
cur.execute("""
    MERGE INTO SILVER.DIM_CUSTOMERS AS tgt
    USING STAGING.STG_CUSTOMERS    AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED AND (
        tgt.email    <> src.email    OR
        tgt.phone    <> src.phone    OR
        tgt.address  <> src.address
    ) THEN UPDATE SET
        tgt.email      = src.email,
        tgt.phone      = src.phone,
        tgt.address    = src.address,
        tgt.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        customer_id, email, phone, address, created_at, updated_at
    ) VALUES (
        src.customer_id, src.email, src.phone, src.address,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );
""")
```

### Step 5 — Silver → Gold Transformation (`6_transformation_silver_to_gold.sql`)

Produces business-level aggregations: daily transaction summaries, customer-level metrics, fraud flagging, and KPI tables ready for BI dashboards.

```sql
-- Daily transaction summary (Gold)
CREATE OR REPLACE TABLE FINANCE_DB.GOLD.DAILY_TXN_SUMMARY AS
SELECT
    transaction_date,
    transaction_type,
    COUNT(*)          AS total_transactions,
    SUM(amount)       AS total_amount,
    AVG(amount)       AS avg_amount,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM FINANCE_DB.SILVER.TRANSACTIONS
WHERE status = 'COMPLETED'
GROUP BY transaction_date, transaction_type;
```

---

## ⚙️ Setup & Execution

### Prerequisites

- Snowflake account (free trial works)
- Python 3.8+
- `snowflake-connector-python` package

### 1. Clone the Repository

```bash
git clone https://github.com/Shandeep-Raula/End-to-End-Financial-Transactions-Data-Pipeline-using-Snowflake-Medallion-Architecture.git
cd End-to-End-Financial-Transactions-Data-Pipeline-using-Snowflake-Medallion-Architecture
```

### 2. Install Python Dependencies

```bash
pip install snowflake-connector-python pandas jupyter
```

### 3. Configure Snowflake Connection

Edit `connection.py` with your Snowflake credentials:

```python
import snowflake.connector

def get_snowflake_connection():
    return snowflake.connector.connect(
        user       = "YOUR_USERNAME",
        password   = "YOUR_PASSWORD",
        account    = "YOUR_ACCOUNT_IDENTIFIER",
        warehouse  = "FINANCE_WH",
        database   = "FINANCE_DB",
        schema     = "BRONZE"
    )
```

> ⚠️ **Security:** Never commit credentials to version control. Use environment variables or Snowflake key-pair authentication in production.

### 4. Run the Pipeline

Execute the SQL files in order inside Snowflake Worksheet or SnowSQL:

```bash
# Step 1 — Environment setup
snowsql -f 2_configs.sql

# Step 2 — Load raw data into Bronze
snowsql -f 4_copy_into.sql

# Step 3 — Bronze → Silver
snowsql -f 5_tranforamtion_bronze_to_silver.sql

# Step 4 — Silver → Gold
snowsql -f 6_transformation_silver_to_gold.sql
```

For the SCD Type 1 demo, launch Jupyter:

```bash
jupyter notebook 6_SCD1_demo.ipynb
```

---

## 📊 Data Flow

```
Raw CSV/JSON Files
        │
        ▼
┌───────────────────┐
│  Snowflake Stage  │  ← Internal Stage (PUT command)
└─────────┬─────────┘
          │  COPY INTO
          ▼
┌───────────────────┐
│   🥉 BRONZE Layer  │  ← Raw, immutable, append-only
│  RAW_TRANSACTIONS │
└─────────┬─────────┘
          │  MERGE + Cleanse
          ▼
┌───────────────────┐
│   🥈 SILVER Layer  │  ← Cleaned, typed, deduplicated
│    TRANSACTIONS   │
│   DIM_CUSTOMERS   │  ← SCD Type 1
└─────────┬─────────┘
          │  Aggregate
          ▼
┌───────────────────┐
│   🥇 GOLD Layer    │  ← Analytics-ready
│ DAILY_TXN_SUMMARY │
│ CUSTOMER_METRICS  │
│   FRAUD_FLAGS     │
└───────────────────┘
          │
          ▼
     BI / Reporting
```

---

## 🔑 Key Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| Medallion Architecture | Three-schema design (Bronze / Silver / Gold) |
| Data Ingestion | Snowflake Internal Stage + `COPY INTO` |
| Incremental Loading | `MERGE` statements with match conditions |
| SCD Type 1 | Overwrite pattern for dimension updates |
| Data Cleansing | `TRY_TO_DATE`, `TRY_TO_DECIMAL`, `TRIM`, `NULLIF` |
| Deduplication | `SELECT DISTINCT` + window functions |
| Aggregation | `GROUP BY` summaries in Gold layer |
| Python Integration | `snowflake-connector-python` for programmatic access |

---

## 📁 Data Schema

### Bronze — `RAW_TRANSACTIONS`

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | VARCHAR | Source system transaction ID |
| `customer_id` | VARCHAR | Customer identifier |
| `transaction_date` | VARCHAR | Raw date string (not parsed) |
| `amount` | VARCHAR | Raw amount string |
| `transaction_type` | VARCHAR | DEBIT / CREDIT / TRANSFER |
| `status` | VARCHAR | COMPLETED / PENDING / FAILED |
| `_loaded_at` | TIMESTAMP | Ingestion timestamp |

### Silver — `TRANSACTIONS`

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | VARCHAR | Primary key |
| `customer_id` | VARCHAR | FK to DIM_CUSTOMERS |
| `transaction_date` | DATE | Parsed, validated date |
| `amount` | DECIMAL(18,2) | Validated numeric amount |
| `transaction_type` | VARCHAR | Standardised / uppercased |
| `status` | VARCHAR | Standardised / uppercased |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

### Gold — `DAILY_TXN_SUMMARY`

| Column | Type | Description |
|--------|------|-------------|
| `transaction_date` | DATE | Aggregation date |
| `transaction_type` | VARCHAR | Transaction category |
| `total_transactions` | INTEGER | Count of transactions |
| `total_amount` | DECIMAL | Sum of transaction amounts |
| `avg_amount` | DECIMAL | Average transaction value |
| `unique_customers` | INTEGER | Distinct customers transacting |



---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

> ⭐ If you found this project useful, please consider giving it a star!
