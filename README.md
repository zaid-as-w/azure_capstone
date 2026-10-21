# Azure Cloud Data Engineering Capstone

> **End-to-end Medallion Architecture** on Azure: CSV Upload → ADLS Gen2 Bronze → PySpark/Delta Lake Silver → Gold Analytics → Databricks SQL → Streamlit Dashboard

---

## 🏆 Problem Statement

Sales data arrives as ad-hoc CSV uploads. This project implements a **production-grade Medallion data pipeline** that ingests raw files, validates and cleanses them, produces business-grade Gold aggregations, and exposes live analytics through an interactive Streamlit dashboard — all orchestrated by Azure Data Factory.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        STREAMLIT DASHBOARD                              │
│  Upload CSV ──► Azure Identity (keyless) ──► azure_storage.py          │
│  Analytics  ──► databricks_client.py ──► Databricks SQL Warehouse       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                   ADLS Gen2 (zaid123)
                   Bronze Container
                   └── uploads/sales.csv
                               │
                   Azure Data Factory (zaid-capstone-adf)
                   PL_Sales_Bronze_To_Silver_To_Gold
                               │
                  ┌────────────▼────────────┐
                  │  nb_bronze_to_silver     │  PySpark (Databricks)
                  │  ● Type-cast columns     │  Cluster: capstone-cluster
                  │  ● 6 validation rules    │  Auto-stop: 15 min
                  │  ● Deduplication         │
                  │  ● rejection_reason col  │
                  │  ● Write Delta (overwrite)│
                  └────────────┬────────────┘
                               │
                   Silver Container (cleaned_sales, rejected_sales)
                               │
                  ┌────────────▼────────────┐
                  │  nb_silver_to_gold       │
                  │  ● gold_category_sales   │
                  │  ● gold_city_sales       │
                  │  ● gold_monthly_sales    │
                  │  ● Register Unity Catalog│
                  └────────────┬────────────┘
                               │
                   Silver Container → gold/ (Delta)
                               │
                   Unity Catalog (zaid_capstone_dbx_7405607366206759.default)
                   ● cleaned_sales      ● rejected_sales
                   ● gold_category_sales ● gold_city_sales ● gold_monthly_sales
```

---

## 📊 Data Flow

1. **User** uploads CSV via Streamlit → `azure_storage.py` → `bronze/uploads/`
2. **ADF** triggers `nb_bronze_to_silver` notebook on `capstone-cluster`
3. Notebook validates, deduplicates, calculates `total_sales`, writes `cleaned_sales` (Delta)
4. ADF (on Silver success) triggers `nb_silver_to_gold` notebook
5. Gold notebook aggregates by category / city / month, registers tables in Unity Catalog
6. **Streamlit** queries Gold tables live via `databricks_client.py` through the SQL Warehouse

---

## 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| Ingestion & Upload | Python, Azure Identity SDK, Azure Storage Blob SDK |
| Orchestration | Azure Data Factory V2 (`zaid-capstone-adf`) |
| Raw Storage | ADLS Gen2 (`zaid123` / `bronze` container) |
| Transformation | PySpark, Delta Lake, Azure Databricks |
| Serving Storage | ADLS Gen2 (`zaid123` / `silver` container) |
| Governance | Unity Catalog, Storage Credentials, External Locations |
| SQL Access | Databricks SQL Warehouse (Serverless, ID `44847d5087ca2301`) |
| Dashboard | Streamlit, Pandas, Plotly |
| Automation | Azure CLI 2.91.0, PowerShell, ARM REST API |

---

## 📁 Project Structure

```
azure-capstone/
├── app.py                          # Streamlit multi-tab dashboard
├── azure_storage.py                # ADLS Gen2 upload (DefaultAzureCredential)
├── databricks_client.py            # SQL Warehouse client (keyless Azure AD)
├── test_databricks.py              # Connection & table verification test
├── requirements.txt                # Python dependencies
├── .gitignore
│
├── notebooks/                      # Databricks notebook source (also in workspace)
│   ├── nb_bronze_to_silver.py      # Silver transformation (Iter 3 repaired)
│   ├── nb_silver_to_gold.py        # Gold aggregation (Iter 5)
│   ├── nb_file_ingestion_control.py # Incremental ingestion controller (Iter 9)
│   └── nb_discover_bronze_files.py # Bronze file discovery
│
├── adf/                            # Azure Data Factory pipeline definitions
│   ├── pipeline_bronze_to_silver.json
│   ├── pipeline_bronze_to_silver_to_gold.json
│   └── pipeline_incremental_ingestion.json
│
├── scripts/
│   └── deploy_adf_pipelines.ps1    # ADF deployment automation script
│
├── data/                           # Test datasets (do not commit production data)
│   ├── sales.csv                   # Primary 10-row test dataset
│   ├── sales_batch_002.csv         # Batch 2 test (incremental ingestion test)
│   └── sales_invalid_records.csv   # Data quality rejection test
│
└── docs/
    ├── IMPLEMENTATION_STATUS.md
    ├── DEPLOYMENT_LOG.md
    ├── RESOURCE_INVENTORY.md
    └── TROUBLESHOOTING.md
```

---

## ⚙️ Setup & Deployment

### Prerequisites

1. **Azure CLI** installed and logged in: `az login`
2. **Python 3.12+** with a virtual environment: `.venv`
3. **Azure Subscription** with:
   - Resource Group `my-group`
   - Storage Account `zaid123` (ADLS Gen2, HNS enabled)
   - Databricks Workspace `zaid-capstone-dbx` (Premium tier)
   - Data Factory `zaid-capstone-adf`
   - Unity Catalog with external locations for bronze and silver

### Authentication Prerequisites

The project uses **keyless authentication** throughout:

| Component | Auth Method |
|---|---|
| Streamlit → ADLS Gen2 | `DefaultAzureCredential` (picks up `az login`) |
| Databricks SQL Warehouse | `DefaultAzureCredential` → Azure AD Bearer token |
| ADF → Databricks | Databricks Personal Access Token (stored in ADF as SecureString, auto-refreshable) |
| ADF → ARM | ADF Managed Identity |

**Never hardcode storage account keys or Databricks PATs in source code.**

### Local Setup

```powershell
# Clone / navigate to project
cd C:\Users\Zaid\Desktop\azure-capstone

# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Set storage account name (required for upload feature)
$env:AZURE_STORAGE_ACCOUNT = "zaid123"

# Run the dashboard
streamlit run app.py
```

### Deploy ADF Pipelines

```powershell
.\scripts\deploy_adf_pipelines.ps1 -ResourceGroup "my-group" -FactoryName "zaid-capstone-adf"
```

### Upload Notebooks to Databricks

Notebooks are uploaded via REST API using Azure AD token:
```powershell
# See: scripts/deploy_adf_pipelines.ps1 comments for notebook upload steps
# Or upload from the Databricks workspace UI at:
# https://adb-7405607366206759.19.azuredatabricks.net
```

---

## 🚀 How to Execute the Pipeline

### Option 1: Azure CLI (Recommended)

```powershell
# Full Bronze → Silver → Gold pipeline
az datafactory pipeline create-run `
  --factory-name zaid-capstone-adf `
  --resource-group my-group `
  --name PL_Sales_Bronze_To_Silver_To_Gold `
  --output json

# Bronze → Silver only
az datafactory pipeline create-run `
  --factory-name zaid-capstone-adf `
  --resource-group my-group `
  --name PL_Sales_Bronze_To_Silver `
  --output json

# Incremental ingestion with control table
az datafactory pipeline create-run `
  --factory-name zaid-capstone-adf `
  --resource-group my-group `
  --name PL_Sales_Incremental_Ingestion `
  --output json
```

### Option 2: Streamlit Dashboard

Use the **Ingestion & ADF Orchestration** tab to upload files.

### Check Pipeline Status

```powershell
az datafactory pipeline-run show `
  --factory-name zaid-capstone-adf `
  --resource-group my-group `
  --run-id <run-id-from-trigger> `
  --output json
```

---

## 📈 Running the Streamlit Dashboard

```powershell
# Activate venv and set environment variables
.venv\Scripts\Activate.ps1
$env:AZURE_STORAGE_ACCOUNT = "zaid123"

# Run
streamlit run app.py
```

Dashboard opens at: `http://localhost:8501`

**Tabs:**
- **Executive Sales Analytics** — KPIs, Revenue by Category, City, Monthly Trend (live Databricks SQL)
- **Ingestion & ADF Orchestration** — CSV upload + pipeline trigger info
- **Data Quality & Audit Logs** — Validation rules + Delta control table view

**Data Source Mode:**
- `Databricks SQL Warehouse (Direct)` — queries live Gold tables via SQL Warehouse
- `Local Silver Fallback` — uses `data/sales.csv` for offline preview

---

## 🧪 Testing

### Databricks SQL Connection Test
```powershell
.venv\Scripts\python.exe test_databricks.py
```

Expected output:
```
[PASS] Token acquired successfully
[PASS] Connection established successfully!
[PASS] Found 5 registered table(s): cleaned_sales, gold_category_sales, gold_city_sales, gold_monthly_sales, rejected_sales
```

### Data Quality Test (Rejection)
Upload `data/sales_invalid_records.csv` — all 6 rows should be rejected:
- Missing order_id
- Invalid date
- Missing category
- Missing city
- Negative quantity
- Negative price

### Duplicate Prevention Test
Upload `data/sales.csv` twice — second run should not increase record count due to Delta `overwrite` in Bronze-to-Silver and idempotent MERGE in incremental pipeline.

---

## 💰 Cost Considerations

| Resource | Type | Est. Monthly Cost |
|---|---|---|
| `zaid123` Storage | Standard LRS, ~50MB | < $0.01 |
| `zaid-capstone-adf` | Pipeline runs (pay-per-use) | < $1/month at student volume |
| `capstone-cluster` | Standard_D4ds_v4, auto-stop 15min | ~$0.50/run hour |
| Serverless SQL Warehouse | Serverless Small, auto-stop 10min | ~$0.50/run hour |

**Cost controls in place:**
- Cluster auto-termination: 15 minutes
- SQL Warehouse auto-stop: 10 minutes
- No scheduled triggers configured (manual-only)

---

## ⚠️ Known Limitations

1. **ADF → Databricks Auth:** Uses PAT (Personal Access Token) stored in ADF linked service as SecureString. Token is auto-generated via REST API. For production, use Databricks + ADF Managed Identity authentication with Azure Credential passthrough.
2. **SparkContext restriction:** Serverless compute does not support `spark.sparkContext` / JVM access. All notebooks use high-level DataFrame API only.
3. **Gold tables:** Registered via `CREATE TABLE ... LOCATION` in Unity Catalog. After re-running Silver, refresh with `REFRESH TABLE` or re-run `nb_silver_to_gold`.
4. **No scheduling:** Pipelines are triggered manually. ADF trigger scheduling requires subscription-level resource provider registration.

---

## 🔮 Future Enhancements

1. Event-driven pipeline trigger via Azure Event Grid on new blob uploads
2. ADF schedule trigger for nightly batch runs
3. Delta Live Tables (DLT) pipeline replacing manual notebook orchestration
4. Databricks Asset Bundles (DAB) for version-controlled notebook deployment
5. Azure Monitor alerts for pipeline failures
6. Power BI integration on top of Databricks SQL Warehouse
7. Managed Identity authentication for ADF → Databricks (eliminates PAT management)
